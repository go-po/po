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
UPDATE po_msg_index
SET next = next + 1
WHERE stream = $1;
