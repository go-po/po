-- name: LockSubscriberPosition :many
SELECT subscriber_id, no
FROM po_subscriptions
WHERE stream = @stream
  AND subscriber_id = ANY (@subscriber_id::varchar[])
    FOR UPDATE;

-- name: SetSubscriberPosition :exec
INSERT INTO po_subscriptions (updated, no, subscriber_id, stream)
VALUES (NOW(), -1, $1, $2)
ON CONFLICT (stream, subscriber_id) DO UPDATE
    SET no      = $3,
        updated = NOW()
WHERE po_subscriptions.stream = $2
  AND po_subscriptions.subscriber_id = $1
  AND po_subscriptions.no < $3;
