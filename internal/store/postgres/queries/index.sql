-- name: GetNextIndex :one
SELECT next
FROM po_msg_index
WHERE stream = $1 AND grp = $2 FOR UPDATE;

-- name: SetNextIndex :exec
INSERT INTO po_msg_index (stream, grp, next)
VALUES ($1, $2, $3)
ON CONFLICT (stream, grp) DO UPDATE
    SET next = $3
WHERE po_msg_index.next = $3 - 1
  AND po_msg_index.stream = $1
  AND po_msg_index.grp = $2;
