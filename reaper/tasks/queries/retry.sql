WITH candidate AS (
    SELECT
        t.promise_id,
        t.topic,
        t.max_failures,
        t.failures + 1 AS failures
    FROM reaper.tasks t
    JOIN reaper.promises p ON p.id = t.promise_id
    WHERE t.promise_id = $1 AND p.state = 'pending'
),

retried AS (
    UPDATE reaper.tasks t
    SET
        failures = c.failures,
        available_at = clock_timestamp() + ($3 * interval '1 millisecond')
    FROM candidate c
    WHERE
        t.promise_id = c.promise_id
        AND c.failures < c.max_failures
    RETURNING t.promise_id
),

rejected AS (
    UPDATE reaper.promises p
    SET
        state = 'rejected',
        error = $2::jsonb,
        settled_at = clock_timestamp()
    FROM candidate c
    WHERE p.id = c.promise_id AND c.failures >= c.max_failures
    RETURNING p.id
),

removed AS (
    DELETE FROM reaper.tasks t
    USING rejected r
    WHERE t.promise_id = r.id
),

waiter_changes AS (
    SELECT
        changed.waiter_id,
        count(*) AS settled_count
    FROM reaper.waits changed
    JOIN rejected r ON r.id = changed.awaited_id
    GROUP BY changed.waiter_id
),

locked_waiters AS (
    SELECT waiter.promise_id
    FROM reaper.tasks waiter
    JOIN waiter_changes changes ON changes.waiter_id = waiter.promise_id
    ORDER BY waiter.promise_id
    FOR UPDATE OF waiter
),

ready_waiters AS (
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
        SELECT c.topic
        FROM candidate c
        WHERE c.failures < c.max_failures
        UNION
        SELECT rw.topic
        FROM ready_waiters rw
        WHERE rw.pending_waits = 0
    ) events
)

SELECT
    c.failures,
    c.failures >= c.max_failures AS rejected,
    (SELECT count(*) FROM notified) AS notifications
FROM candidate c;
