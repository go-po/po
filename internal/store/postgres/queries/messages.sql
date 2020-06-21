-- name: StoreRecord :one
INSERT INTO po_msgs (stream, no, grp, content_type, data, correlation_id)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetStreamPosition :one
SELECT GREATEST(MAX(no), -1)::bigint
FROM po_msgs
WHERE stream = $1;

-- name: GetRecords :many
select *
from po_msgs;

-- name: Insert :exec
INSERT INTO po_msgs (stream, no, grp, content_type, data)
VALUES ($1, $2, $3, $4, $5);

-- name: GetRecordByStream :one
SELECT *
FROM po_msgs
WHERE stream = $1
  AND no = $2;

-- name: GetRecordsByStream :many
SELECT *
FROM po_msgs
WHERE stream = $1
  AND no > $2
ORDER BY no;
