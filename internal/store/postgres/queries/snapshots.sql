-- name: GetSnapshotPosition :one
SELECT no, content_type, data
FROM po_snapshots
WHERE stream = $1
  AND snapshot_id = $2;

-- name: UpdateSnapshot :exec
INSERT INTO po_snapshots (stream, snapshot_id, no, content_type, data)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (stream, snapshot_id) DO UPDATE
    SET no           = excluded.no,
        content_type = excluded.content_type,
        data         = excluded.data,
        updated      = NOW()
WHERE po_snapshots.stream = $1
  AND po_snapshots.snapshot_id = $2;

-- name: DeleteSnapshot :exec
DELETE
FROM po_snapshots
WHERE stream = $1
  AND snapshot_id = $2;
