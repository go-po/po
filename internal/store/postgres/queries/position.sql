-- name: GetSubscriberPosition :one
SELECT *
FROM po_pos
WHERE stream = $1
  AND listener = $2
    FOR UPDATE;

-- name: SetSubscriberPosition :exec
INSERT INTO po_pos (stream, listener, no, content_type, data)
VALUES ($1, $2, $3, 'application/json', '{}'::bytea)
ON CONFLICT (stream, listener) DO UPDATE
    SET no      = $3,
        updated = NOW()
WHERE po_pos.stream = $1
  AND po_pos.listener = $2;

-- name: GetStreamPosition :one
SELECT GREATEST(MAX(no), 0)::bigint
FROM po_msgs
WHERE stream = $1;
