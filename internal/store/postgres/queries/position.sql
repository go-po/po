-- name: GetSubscriberPosition :one
SELECT *
FROM po_pos
WHERE stream = $1
  AND listener = $2
    FOR UPDATE;

-- name: GetSnapshotPosition :one
SELECT no, content_type, data
FROM po_pos
WHERE stream = $1
  AND listener = $2;

-- name: SetSubscriberPosition :exec
INSERT INTO po_pos (stream, listener, no, content_type, data)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (stream, listener) DO UPDATE
    SET no           = excluded.no,
        content_type = excluded.content_type,
        data         = excluded.data,
        updated      = NOW()
WHERE po_pos.stream = $1
  AND po_pos.listener = $2;







