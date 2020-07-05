-- name: LockSubscriberPosition :many
SELECT subscriber_id, no
FROM po_subscriptions
WHERE stream = @stream
  AND subscriber_id = ANY (@subscriber_id::varchar[])
    FOR UPDATE;

-- name: SetSubscriberPosition :exec
UPDATE po_subscriptions
SET no = @no
WHERE stream = @stream
  AND subscriber_id = @subscriber_id;
