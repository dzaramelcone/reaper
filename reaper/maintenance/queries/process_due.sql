WITH expired_tasks AS MATERIALIZED (
    SELECT t.promise_id
    FROM reaper.tasks t
    JOIN reaper.promises p ON p.id = t.promise_id
    WHERE
        p.state = 'pending'
        AND p.expires_at IS NOT NULL
        AND p.expires_at <= statement_timestamp()
    ORDER BY p.expires_at, p.id
    LIMIT $1
    FOR UPDATE OF t SKIP LOCKED
),

timer_due AS MATERIALIZED (
    SELECT p.id
    FROM reaper.promises p
    WHERE
        p.state = 'pending'
        AND p.due_at IS NOT NULL
        AND p.due_at <= statement_timestamp()
    ORDER BY p.due_at, p.id
    LIMIT $1
    FOR UPDATE OF p SKIP LOCKED
),

timeout_due AS MATERIALIZED (
    SELECT p.id
    FROM reaper.promises p
    JOIN expired_tasks due ON due.promise_id = p.id
    WHERE p.expires_at IS NOT NULL AND p.state = 'pending'
    ORDER BY p.id
    FOR UPDATE OF p SKIP LOCKED
),

timers AS (
    UPDATE reaper.promises p
    SET
        state = 'resolved', result = 'null'::jsonb,
        settled_at = statement_timestamp()
    FROM timer_due due
    WHERE p.id = due.id AND p.state = 'pending'
    RETURNING p.id
),

timeouts AS (
    UPDATE reaper.promises p
    SET
        state = 'timed_out',
        error = '{"type":"PromiseTimeout"}'::jsonb,
        settled_at = statement_timestamp()
    FROM timeout_due due
    WHERE p.id = due.id AND p.state = 'pending'
    RETURNING p.id
),

removed AS (
    DELETE FROM reaper.tasks t
    USING timeouts settled
    WHERE t.promise_id = settled.id
),

settled AS (
    SELECT id FROM timers
    UNION ALL
    SELECT id FROM timeouts
),

waiter_changes AS (
    SELECT
        changed.waiter_id,
        count(*) AS settled_count
    FROM reaper.waits changed
    JOIN settled ON settled.id = changed.awaited_id
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
            WHEN waiter.pending_waits <= changes.settled_count THEN statement_timestamp()
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
    (SELECT count(*) FROM timers) AS timers,
    (SELECT count(*) FROM timeouts) AS timeouts,
    (SELECT count(*) FROM notified) AS notifications;
