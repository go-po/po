// Code generated by sqlc. DO NOT EDIT.
// source: index.sql

package db

import (
	"context"
)

const getNextIndex = `-- name: GetNextIndex :one
SELECT next
FROM po_msg_index
WHERE stream = $1 AND grp = $2 FOR UPDATE
`

type GetNextIndexParams struct {
	Stream string `json:"stream"`
	Grp    bool   `json:"grp"`
}

func (q *Queries) GetNextIndex(ctx context.Context, arg GetNextIndexParams) (int64, error) {
	row := q.db.QueryRowContext(ctx, getNextIndex, arg.Stream, arg.Grp)
	var next int64
	err := row.Scan(&next)
	return next, err
}

const setNextIndex = `-- name: SetNextIndex :exec
INSERT INTO po_msg_index (stream, grp, next)
VALUES ($1, $2, $3)
ON CONFLICT (stream, grp) DO UPDATE
    SET next = $3
WHERE po_msg_index.next = $3 - 1
  AND po_msg_index.stream = $1
  AND po_msg_index.grp = $2
`

type SetNextIndexParams struct {
	Stream string `json:"stream"`
	Grp    bool   `json:"grp"`
	Next   int64  `json:"next"`
}

func (q *Queries) SetNextIndex(ctx context.Context, arg SetNextIndexParams) error {
	_, err := q.db.ExecContext(ctx, setNextIndex, arg.Stream, arg.Grp, arg.Next)
	return err
}
