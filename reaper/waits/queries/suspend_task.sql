WITH requested AS (
    SELECT id FROM unnest($2::text []) requested (id)
),

claimed AS (
    SELECT
        t.promise_id,
        t.topic,
        coalesce(p.root_id, p.id) AS root_id
    FROM reaper.tasks t
    JOIN reaper.promises p ON p.id = t.promise_id
    WHERE t.promise_id = $1
    FOR UPDATE OF t
),

awaited AS (
    SELECT
        requested.id,
        p.state,
        coalesce(p.root_id, p.id) AS root_id
    FROM requested
    JOIN reaper.promises p ON p.id = requested.id
    ORDER BY requested.id
    FOR UPDATE OF p
),

validity AS (
    SELECT
        exists(SELECT 1 FROM claimed) AS waiter_exists,
        array(
            SELECT requested.id
            FROM requested
            WHERE NOT EXISTS (
                SELECT 1 FROM awaited
                WHERE awaited.id = requested.id
            )
        ) AS missing,
        coalesce(
            bool_and(awaited.root_id = claim_row.root_id)
            FILTER (WHERE awaited.id IS NOT NULL),
            TRUE
        ) AS same_root
    FROM (SELECT TRUE AS present) anchor
    LEFT JOIN awaited ON TRUE
    LEFT JOIN claimed AS claim_row ON TRUE
    WHERE anchor.present
),

inserted AS (
    INSERT INTO reaper.waits (waiter_id, awaited_id, graph_id)
    SELECT
        $1,
        awaited.id,
        claimed.root_id
    FROM awaited, validity, claimed
    WHERE
        validity.waiter_exists
        AND cardinality(validity.missing) = 0
        AND validity.same_root
    ON CONFLICT DO NOTHING
),

pending AS (
    SELECT count(*) AS count
    FROM awaited
    WHERE awaited.state = 'pending'
),

updated AS (
    UPDATE reaper.tasks t
    SET
        pending_waits = pending.count,
        available_at = clock_timestamp()
    FROM validity, pending
    WHERE
        t.promise_id = $1
        AND validity.waiter_exists
        AND cardinality(validity.missing) = 0
        AND validity.same_root
    RETURNING t.topic, t.pending_waits
),

notified AS (
    SELECT pg_notify('reaper_task_' || md5(topic), '')
    FROM updated
    WHERE pending_waits = 0
)

SELECT
    validity.waiter_exists,
    validity.missing,
    validity.same_root,
    updated.pending_waits,
    (SELECT count(*) FROM notified) AS notifications
FROM validity
LEFT JOIN updated ON TRUE;
