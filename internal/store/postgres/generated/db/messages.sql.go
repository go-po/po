// Code generated by sqlc. DO NOT EDIT.
// source: messages.sql

package db

import (
	"context"
	"database/sql"
)

const getStreamPosition = `-- name: GetStreamPosition :one
SELECT GREATEST(MAX(no), -1)::bigint
FROM po_messages
WHERE stream = $1
`

func (q *Queries) GetStreamPosition(ctx context.Context, stream string) (int64, error) {
	row := q.db.QueryRowContext(ctx, getStreamPosition, stream)
	var column_1 int64
	err := row.Scan(&column_1)
	return column_1, err
}

const readRecordsByGroup = `-- name: ReadRecordsByGroup :many
SELECT id, created, stream, no, grp, content_type, data, correlation_id
FROM po_messages
WHERE grp = $1
  AND id > $2
  AND id <= $3
ORDER BY id ASC
`

type ReadRecordsByGroupParams struct {
	Grp  string `json:"grp"`
	ID   int64  `json:"id"`
	ID_2 int64  `json:"id_2"`
}

func (q *Queries) ReadRecordsByGroup(ctx context.Context, arg ReadRecordsByGroupParams) ([]PoMessage, error) {
	rows, err := q.db.QueryContext(ctx, readRecordsByGroup, arg.Grp, arg.ID, arg.ID_2)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []PoMessage
	for rows.Next() {
		var i PoMessage
		if err := rows.Scan(
			&i.ID,
			&i.Created,
			&i.Stream,
			&i.No,
			&i.Grp,
			&i.ContentType,
			&i.Data,
			&i.CorrelationID,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const readRecordsByStream = `-- name: ReadRecordsByStream :many
SELECT id, created, stream, no, grp, content_type, data, correlation_id
FROM po_messages
WHERE stream = $1
  AND no > $2
  AND no <= $3
ORDER BY no ASC
`

type ReadRecordsByStreamParams struct {
	Stream string `json:"stream"`
	No     int64  `json:"no"`
	No_2   int64  `json:"no_2"`
}

func (q *Queries) ReadRecordsByStream(ctx context.Context, arg ReadRecordsByStreamParams) ([]PoMessage, error) {
	rows, err := q.db.QueryContext(ctx, readRecordsByStream, arg.Stream, arg.No, arg.No_2)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []PoMessage
	for rows.Next() {
		var i PoMessage
		if err := rows.Scan(
			&i.ID,
			&i.Created,
			&i.Stream,
			&i.No,
			&i.Grp,
			&i.ContentType,
			&i.Data,
			&i.CorrelationID,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const storeRecord = `-- name: StoreRecord :one
INSERT INTO po_messages (stream, no, grp, content_type, data, correlation_id)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, created, stream, no, grp, content_type, data, correlation_id
`

type StoreRecordParams struct {
	Stream        string         `json:"stream"`
	No            int64          `json:"no"`
	Grp           string         `json:"grp"`
	ContentType   string         `json:"content_type"`
	Data          []byte         `json:"data"`
	CorrelationID sql.NullString `json:"correlation_id"`
}

func (q *Queries) StoreRecord(ctx context.Context, arg StoreRecordParams) (PoMessage, error) {
	row := q.db.QueryRowContext(ctx, storeRecord,
		arg.Stream,
		arg.No,
		arg.Grp,
		arg.ContentType,
		arg.Data,
		arg.CorrelationID,
	)
	var i PoMessage
	err := row.Scan(
		&i.ID,
		&i.Created,
		&i.Stream,
		&i.No,
		&i.Grp,
		&i.ContentType,
		&i.Data,
		&i.CorrelationID,
	)
	return i, err
}
