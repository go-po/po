-- name: GetRecords :many
select *
from po_msgs;

-- name: Insert :exec
INSERT INTO po_msgs (stream, no, grp, content_type, data)
VALUES ($1, $2, $3, $4, $5);

-- name: SetGroupNumber :one
UPDATE po_msgs
SET grp_no  = $1,
    updated = NOW()
WHERE stream = $2
  AND no = $3
  AND grp_no IS NULL
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


-- name: GetStreamPosition :one
SELECT GREATEST(MAX(no), 0)::bigint
FROM po_msgs
WHERE stream = $1;
