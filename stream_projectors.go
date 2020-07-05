package po

import (
	"context"
	"encoding/json"

	"github.com/go-po/po/internal/pager"
	"github.com/go-po/po/internal/record"
	"github.com/go-po/po/streams"
)

type projector interface {
	Project(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error)
}

type projectorFunc func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error)

func (fn projectorFunc) Project(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
	return fn(ctx, id, lockPosition, projection)
}

type projectorStore interface {
	ReadRecords(ctx context.Context, id streams.Id, from, to, limit int64) ([]record.Record, error)
}

func newProjectorFunc(store projectorStore, registry Registry) projectorFunc {
	return func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
		err := pager.ToMax(lockPosition, 100, pager.Func(func(from, to, limit int64) (int, error) {
			records, err := store.ReadRecords(ctx, id, from, to, limit)
			if err != nil {
				return 0, err
			}
			if len(records) == 0 {
				// nothing new, bail out
				return 0, nil
			}

			var messages []streams.Message
			for _, r := range records {
				message, err := registry.ToMessage(r)
				if err != nil {
					return -1, err
				}
				messages = append(messages, message)
			}

			for _, message := range messages {
				err = projection.Handle(ctx, message)
				if err != nil {
					return 0, err
				}
			}

			if len(messages) == 0 {
				return 0, nil
			}

			message := messages[len(messages)-1]
			if id.HasEntity() {
				lockPosition = message.Number
			} else {
				lockPosition = message.GlobalNumber
			}

			return len(messages), nil
		}))

		if err != nil {
			return -1, err
		}

		return lockPosition, nil
	}
}

type snapshotStore interface {
	ReadSnapshot(ctx context.Context, id streams.Id, snapshotId string) (record.Snapshot, error)
	UpdateSnapshot(ctx context.Context, id streams.Id, snapshotId string, snapshot record.Snapshot) error
}

func newSnapshots(store snapshotStore, inner projector) projectorFunc {
	var reader projector = newSnapshotReader(store)
	var writer projector = newSnapshotWriter(store)

	// TODO Observe errors from the reader and writer.
	// They should never fail though, as projection
	// must not halter execution.
	return func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
		var err error
		position, _ := reader.Project(ctx, id, lockPosition, projection)

		position, err = inner.Project(ctx, id, position, projection)
		if err != nil {
			return position, err
		}

		_, _ = writer.Project(ctx, id, position, projection)

		return position, nil

	}
}

func newSnapshotWriter(store snapshotStore) projectorFunc {
	return func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
		snap, supportsSnapshot := projection.(streams.NamedSnapshot)
		if supportsSnapshot {
			b, err := json.Marshal(projection)
			if err != nil {
				// failed to marshal, discard
				return lockPosition, err
			}

			err = store.UpdateSnapshot(ctx, id, snap.SnapshotName(), record.Snapshot{
				Data:        b,
				Position:    lockPosition,
				ContentType: "application/json",
			})
			if err != nil {
				// failed to store the snapshot, discard
				return lockPosition, err
			}
		}
		return lockPosition, nil
	}
}

// reads snapshots, but never fails as snapshotting should just default
// to not working if something goes wrong
func newSnapshotReader(store snapshotStore) projectorFunc {
	return func(ctx context.Context, id streams.Id, lockPosition int64, projection Handler) (int64, error) {
		snap, supportsSnapshot := projection.(streams.NamedSnapshot)
		if supportsSnapshot {
			snapshot, err := store.ReadSnapshot(ctx, id, snap.SnapshotName())
			if err != nil {
				return lockPosition, err
			}

			err = json.Unmarshal(snapshot.Data, projection)
			if err != nil {
				// failed to unmarshal, discard
				return lockPosition, err
			}
			return snapshot.Position, nil
		}
		return lockPosition, nil
	}
}
