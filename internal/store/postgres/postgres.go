package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-po/po/internal/observer"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/internal/store"
	"github.com/go-po/po/internal/store/postgres/generated/db"
	"github.com/go-po/po/streams"
	"github.com/lib/pq"
)

var emptyJson = []byte("{}")

func NewFromUrl(databaseUrl string, builder *observer.Builder) (*PGStore, error) {
	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		return nil, err
	}
	return New(db, builder)
}

func New(conn *sql.DB, builder *observer.Builder) (*PGStore, error) {
	err := migrateDatabase(conn)
	if err != nil {
		return nil, err
	}
	return &PGStore{
		conn: conn,
		db:   db.New(conn),
		observer: pgObserver{
			ReadSnapshot: builder.Unary().
				LogDebugf("store/postgres read snapshot: %s").
				Metrics().
				Build(),
			UpdateSnapshot: builder.Nullary().
				LogDebugf("store/postgres update snapshot").
				Build(),
		},
	}, nil
}

type PGStore struct {
	conn     *sql.DB
	db       *db.Queries
	observer pgObserver
}

func (store *PGStore) ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error) {
	done := store.observer.ReadSnapshot.Observe(ctx, id.String())
	defer done()

	position, err := store.db.GetSnapshotPosition(ctx, db.GetSnapshotPositionParams{
		Stream:   id.String(),
		Listener: snapshotId,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return record.Snapshot{
				Data:        emptyJson,
				Position:    0,
				ContentType: "application/json",
			}, nil
		}
		return record.Snapshot{}, err
	}
	return record.Snapshot{
		Data:        position.Data,
		Position:    position.No,
		ContentType: position.ContentType,
	}, nil
}

func (store *PGStore) UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error {
	done := store.observer.UpdateSnapshot.Observe(ctx)
	defer done()

	err := store.db.SetSubscriberPosition(ctx, db.SetSubscriberPositionParams{
		Stream:      id.String(),
		Listener:    snapshotId,
		No:          snapshot.Position,
		ContentType: snapshot.ContentType,
		Data:        snapshot.Data,
	})
	if err != nil {
		return err
	}
	return nil
}

func (store *PGStore) GetStreamPosition(ctx context.Context, id streams.Id) (int64, error) {
	position, err := store.db.GetStreamPosition(ctx, id.String())
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return position, nil
}

func (store *PGStore) ReadRecords(ctx context.Context, id streams.Id, from int64) ([]record.Record, error) {
	var records []record.Record
	var poMsgs []db.PoMsg
	var err error

	if id.HasEntity() {
		poMsgs, err = store.db.GetRecordsByStream(ctx, db.GetRecordsByStreamParams{
			Stream: id.String(),
			No:     from,
		})
	} else {
		poMsgs, err = store.db.GetRecordsByGroup(ctx, db.GetRecordsByGroupParams{
			Grp: id.Group,
			GrpNo: sql.NullInt64{
				Int64: from,
				Valid: true,
			},
		})
	}
	if err != nil {
		return nil, err
	}
	for _, msg := range poMsgs {
		records = append(records, toRecord(msg))
	}
	return records, nil
}

func (store *PGStore) GetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id) (int64, error) {
	t, ok := tx.(*pgTx)
	if !ok {
		return 0, ErrUnknownTx{tx}
	}
	position, err := t.db.GetSubscriberPosition(t.ctx, db.GetSubscriberPositionParams{
		Stream:   stream.String(),
		Listener: subscriberId,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return position.No, nil
}

func (store *PGStore) SetSubscriberPosition(tx store.Tx, subscriberId string, stream streams.Id, position int64) error {
	t, ok := tx.(*pgTx)
	if !ok {
		return ErrUnknownTx{tx}
	}

	err := t.db.SetSubscriberPosition(t.ctx, db.SetSubscriberPositionParams{
		Stream:      stream.String(),
		Listener:    subscriberId,
		No:          position,
		ContentType: "text/plain",
		Data:        []byte(""),
	})
	if err != nil {
		return err
	}
	return nil
}

func (store *PGStore) begin(ctx context.Context) (*pgTx, error) {
	tx, err := store.conn.BeginTx(ctx, &sql.TxOptions{
		//Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return nil, err
	}
	return &pgTx{
		db:  store.db.WithTx(tx),
		tx:  tx,
		ctx: ctx,
	}, nil
}

func (store *PGStore) Begin(ctx context.Context) (store.Tx, error) {
	return store.begin(ctx)
}

func (store *PGStore) StoreRecord(tx store.Tx, id streams.Id, number int64, contentType string, data []byte) (record.Record, error) {
	t, ok := tx.(*pgTx)
	if !ok {
		return record.Record{}, ErrUnknownTx{tx}
	}

	next, err := t.db.GetNextIndex(t.ctx, db.GetNextIndexParams{
		Stream: id.String(),
		Grp:    false,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			next = 1
		} else {
			return record.Record{}, fmt.Errorf("get next index: %s", err)
		}
	}

	if next != number {
		return record.Record{}, fmt.Errorf("number out of order: number=%d next=%d", number, next)
	}

	err = t.db.Insert(t.ctx, db.InsertParams{
		Stream:      id.String(),
		No:          next,
		ContentType: contentType,
		Data:        data,
		Grp:         id.Group,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("insert: %s", err)
	}

	err = t.db.SetNextIndex(t.ctx, db.SetNextIndexParams{
		Stream: id.String(),
		Grp:    false,
		Next:   next + 1,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("set next index: %s", err)
	}

	msg, err := t.db.GetRecordByStream(t.ctx, db.GetRecordByStreamParams{
		Stream: id.String(),
		No:     next,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("get record: %s", err)
	}

	return toRecord(msg), nil
}

func (store *PGStore) AssignGroup(ctx context.Context, id streams.Id, number int64) (record.Record, error) {
	tx, err := store.begin(ctx)
	if err != nil {
		return record.Record{}, err
	}
	defer func() { _ = tx.Rollback() }()

	next, nextIndexErr := tx.db.GetNextIndex(tx.ctx, db.GetNextIndexParams{
		Stream: id.Group,
		Grp:    true,
	})
	if nextIndexErr != nil {
		if nextIndexErr == sql.ErrNoRows {
			next = 1
		} else if pqErr, ok := nextIndexErr.(*pq.Error); ok {
			fmt.Printf("PQ: %#v\n", pqErr)
		} else {
			return record.Record{}, fmt.Errorf("get next index: %T %s", nextIndexErr, nextIndexErr)
		}
	}

	err = tx.db.SetNextIndex(ctx, db.SetNextIndexParams{
		Next:   next + 1,
		Stream: id.Group,
		Grp:    true,
	})
	if err != nil {
		return record.Record{}, err
	}

	msg, err := tx.db.SetGroupNumber(tx.ctx, db.SetGroupNumberParams{
		GrpNo: sql.NullInt64{
			Int64: next,
			Valid: true,
		},
		Stream: id.String(),
		No:     number,
	})
	if err != nil {
		return record.Record{}, fmt.Errorf("write group [%s:%d]: %w", id, number, err)
	}
	err = tx.Commit()
	if err != nil {
		return record.Record{}, err
	}

	return toRecord(msg), nil
}

type pgTx struct {
	ctx context.Context
	db  *db.Queries
	tx  *sql.Tx
}

func (t *pgTx) Commit() error {
	return t.tx.Commit()
}

func (t *pgTx) Rollback() error {
	return t.tx.Rollback()
}

func toRecord(msg db.PoMsg) record.Record {
	var grpNo int64 = 0
	if msg.GrpNo.Valid {
		grpNo = msg.GrpNo.Int64
	}
	return record.Record{
		Number:      msg.No,
		Stream:      streams.ParseId(msg.Stream),
		Data:        msg.Data,
		Group:       msg.Grp,
		GroupNumber: grpNo,
		ContentType: msg.ContentType,
		Time:        msg.Created,
	}
}
