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
