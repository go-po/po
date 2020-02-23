-- name: GetRecords :many
select *
from po_msgs;

-- name: Insert :exec
INSERT INTO po_msgs (stream, no, grp, content_type, data)
VALUES ($1, $2, $3, $4, $5);

-- name: GetNextIndex :one
SELECT next
FROM po_msg_index
WHERE stream = $1 FOR UPDATE;

-- name: SetNextIndex :exec
INSERT INTO po_msg_index (stream, next)
VALUES ($1, $2)
ON CONFLICT (stream) DO UPDATE
    SET next = $2
WHERE po_msg_index.next = $2 - 1
  AND po_msg_index.stream = $1;

-- name: SetGroupNumber :one
UPDATE po_msgs
SET grp_no = $1
WHERE stream = $2
  AND no = $3
  AND grp_no = 0
RETURNING *;

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

-- name: GetRecordsByGroup :many
SELECT *
FROM po_msgs
WHERE grp = $1
  AND grp_no IS NOT NULL
  AND grp_no > $2
ORDER BY grp_no;

