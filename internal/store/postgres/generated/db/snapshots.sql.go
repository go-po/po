// Code generated by sqlc. DO NOT EDIT.
// source: snapshots.sql

package db

import (
	"context"
)

const deleteSnapshot = `-- name: DeleteSnapshot :exec
DELETE
FROM po_snapshots
WHERE stream = $1
  AND snapshot_id = $2
`

type DeleteSnapshotParams struct {
	Stream     string `json:"stream"`
	SnapshotID string `json:"snapshot_id"`
}

func (q *Queries) DeleteSnapshot(ctx context.Context, arg DeleteSnapshotParams) error {
	_, err := q.db.ExecContext(ctx, deleteSnapshot, arg.Stream, arg.SnapshotID)
	return err
}

const getSnapshotPosition = `-- name: GetSnapshotPosition :one
SELECT no, content_type, data
FROM po_snapshots
WHERE stream = $1
  AND snapshot_id = $2
`

type GetSnapshotPositionParams struct {
	Stream     string `json:"stream"`
	SnapshotID string `json:"snapshot_id"`
}

type GetSnapshotPositionRow struct {
	No          int64  `json:"no"`
	ContentType string `json:"content_type"`
	Data        []byte `json:"data"`
}

func (q *Queries) GetSnapshotPosition(ctx context.Context, arg GetSnapshotPositionParams) (GetSnapshotPositionRow, error) {
	row := q.db.QueryRowContext(ctx, getSnapshotPosition, arg.Stream, arg.SnapshotID)
	var i GetSnapshotPositionRow
	err := row.Scan(&i.No, &i.ContentType, &i.Data)
	return i, err
}

const updateSnapshot = `-- name: UpdateSnapshot :exec
INSERT INTO po_snapshots (stream, snapshot_id, no, content_type, data)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (stream, snapshot_id) DO UPDATE
    SET no           = excluded.no,
        content_type = excluded.content_type,
        data         = excluded.data,
        updated      = NOW()
WHERE po_snapshots.stream = $1
  AND po_snapshots.snapshot_id = $2
`

type UpdateSnapshotParams struct {
	Stream      string `json:"stream"`
	SnapshotID  string `json:"snapshot_id"`
	No          int64  `json:"no"`
	ContentType string `json:"content_type"`
	Data        []byte `json:"data"`
}

func (q *Queries) UpdateSnapshot(ctx context.Context, arg UpdateSnapshotParams) error {
	_, err := q.db.ExecContext(ctx, updateSnapshot,
		arg.Stream,
		arg.SnapshotID,
		arg.No,
		arg.ContentType,
		arg.Data,
	)
	return err
}