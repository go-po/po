-- name: StoreRecord :one
INSERT INTO po_msgs (stream, no, grp, content_type, data, correlation_id)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetStreamPosition :one
SELECT GREATEST(MAX(no), -1)::bigint
FROM po_msgs
WHERE stream = $1;

-- name: ReadRecordsByStream :many
SELECT *
FROM po_msgs
WHERE stream = $1
  AND no > $2
ORDER BY no ASC;

-- name: ReadRecordsByGroup :many
SELECT *
FROM po_msgs
WHERE grp = $1
  AND id > $2
ORDER BY id ASC;
