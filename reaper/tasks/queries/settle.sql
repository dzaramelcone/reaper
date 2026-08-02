WITH settled AS (
    UPDATE reaper.promises p
    SET
        state = $2, result = $3::jsonb, error = $4::jsonb,
        settled_at = clock_timestamp()
    FROM reaper.tasks t
    WHERE p.id = $1 AND t.promise_id = p.id AND p.state = 'pending'
    RETURNING
        p.id, p.idempotency_key, p.state, p.root_id,
        p.result, p.error, p.due_at, p.expires_at,
        p.delete_after, p.settled_at
),

removed AS (
    DELETE FROM reaper.tasks t
    USING settled s
    WHERE t.promise_id = s.id
),

waiter_changes AS (
    SELECT
        changed.waiter_id,
        count(*) AS settled_count
    FROM reaper.waits changed
    JOIN settled s ON s.id = changed.awaited_id
    GROUP BY changed.waiter_id
),

locked_waiters AS (
    SELECT waiter.promise_id
    FROM reaper.tasks waiter
    JOIN waiter_changes changes ON changes.waiter_id = waiter.promise_id
    ORDER BY waiter.promise_id
    FOR UPDATE OF waiter
),

ready AS (
    UPDATE reaper.tasks waiter
    SET
        pending_waits = greatest(waiter.pending_waits - changes.settled_count, 0),
        available_at = CASE
            WHEN waiter.pending_waits <= changes.settled_count THEN clock_timestamp()
            ELSE waiter.available_at
        END
    FROM waiter_changes changes
    JOIN locked_waiters locked ON locked.promise_id = changes.waiter_id
    WHERE waiter.promise_id = changes.waiter_id AND waiter.pending_waits > 0
    RETURNING waiter.topic, waiter.pending_waits
),

notified AS (
    SELECT pg_notify('reaper_task_' || md5(topic), '')
    FROM (
        SELECT DISTINCT topic FROM ready
        WHERE pending_waits = 0
    ) topics
)

SELECT
    p.id,
    p.idempotency_key,
    p.state,
    p.root_id,
    p.result::text AS result_json,
    p.error::text AS error_json,
    p.due_at,
    p.expires_at,
    p.delete_after,
    p.settled_at,
    (SELECT count(*) FROM notified) AS notifications
FROM settled p;
