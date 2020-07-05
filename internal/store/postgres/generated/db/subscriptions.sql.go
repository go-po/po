// Code generated by sqlc. DO NOT EDIT.
// source: subscriptions.sql

package db

import (
	"context"

	"github.com/lib/pq"
)

const lockSubscriberPosition = `-- name: LockSubscriberPosition :many
SELECT subscriber_id, no
FROM po_subscriptions
WHERE stream = $1
  AND subscriber_id = ANY ($2::varchar[])
    FOR UPDATE
`

type LockSubscriberPositionParams struct {
	Stream       string   `json:"stream"`
	SubscriberID []string `json:"subscriber_id"`
}

type LockSubscriberPositionRow struct {
	SubscriberID string `json:"subscriber_id"`
	No           int64  `json:"no"`
}

func (q *Queries) LockSubscriberPosition(ctx context.Context, arg LockSubscriberPositionParams) ([]LockSubscriberPositionRow, error) {
	rows, err := q.db.QueryContext(ctx, lockSubscriberPosition, arg.Stream, pq.Array(arg.SubscriberID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []LockSubscriberPositionRow
	for rows.Next() {
		var i LockSubscriberPositionRow
		if err := rows.Scan(&i.SubscriberID, &i.No); err != nil {
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

const setSubscriberPosition = `-- name: SetSubscriberPosition :exec
INSERT INTO po_subscriptions (updated, no, subscriber_id, stream)
VALUES (NOW(), -1, $1, $2)
ON CONFLICT (stream, subscriber_id) DO UPDATE
    SET no      = $3,
        updated = NOW()
WHERE po_subscriptions.stream = $2
  AND po_subscriptions.subscriber_id = $1
  AND po_subscriptions.no < $3
`

type SetSubscriberPositionParams struct {
	SubscriberID string `json:"subscriber_id"`
	Stream       string `json:"stream"`
	No           int64  `json:"no"`
}

func (q *Queries) SetSubscriberPosition(ctx context.Context, arg SetSubscriberPositionParams) error {
	_, err := q.db.ExecContext(ctx, setSubscriberPosition, arg.SubscriberID, arg.Stream, arg.No)
	return err
}
