-- name: StoreRecord :one
INSERT INTO po_messages (stream, no, grp, content_type, data, correlation_id)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetStreamPosition :one
SELECT GREATEST(MAX(no), -1)::bigint
FROM po_messages
WHERE stream = $1;

-- name: ReadRecordsByStream :many
SELECT *
FROM po_messages
WHERE stream = $1
  AND no > $2
  AND no <= $3
ORDER BY no ASC
LIMIT $4;

-- name: ReadRecordsByGroup :many
SELECT *
FROM po_messages
WHERE grp = $1
  AND id > $2
  AND id <= $3
ORDER BY id ASC
LIMIT $4;
