WITH requested AS MATERIALIZED (
    SELECT request.*
    FROM JSONB_TO_RECORDSET($1::jsonb) AS request (
        id text,
        idempotency_key text,
        root_id text,
        expires_at timestamptz,
        retention_ms integer,
        function text,
        version integer,
        topic text,
        input jsonb,
        depth smallint,
        priority smallint,
        available_at timestamptz,
        max_failures integer,
        execution_timeout_ms integer
    )
),

promises AS (
    INSERT INTO reaper.promises (
        id, idempotency_key, root_id, expires_at, delete_after
    )
    SELECT
        request.id,
        request.idempotency_key,
        request.root_id,
        request.expires_at,
        CASE
            WHEN request.root_id IS NULL THEN
                request.expires_at + (request.retention_ms * interval '1 millisecond')
        END
    FROM requested request
    ON CONFLICT (id) DO NOTHING
    RETURNING
        id, idempotency_key, state, root_id, result, error,
        due_at, expires_at, delete_after, settled_at
),

existing AS (
    SELECT
        p.id,
        p.idempotency_key,
        p.state,
        p.root_id,
        p.result,
        p.error,
        p.due_at,
        p.expires_at,
        p.delete_after,
        p.settled_at
    FROM reaper.promises p
    WHERE p.id = ANY(ARRAY(SELECT request.id FROM requested request))
),

returned AS (
    SELECT
        id,
        idempotency_key,
        state,
        root_id,
        result,
        error,
        due_at,
        expires_at,
        delete_after,
        settled_at
    FROM promises
    UNION ALL
    SELECT
        id,
        idempotency_key,
        state,
        root_id,
        result,
        error,
        due_at,
        expires_at,
        delete_after,
        settled_at
    FROM existing
),

tasks AS (
    INSERT INTO reaper.tasks (
        promise_id, function, version, topic, input, depth, priority,
        available_at, max_failures, execution_timeout_ms
    )
    SELECT
        promise.id,
        request.function,
        request.version,
        request.topic,
        COALESCE(request.input, 'null'::jsonb),
        request.depth,
        request.priority,
        request.available_at,
        request.max_failures,
        request.execution_timeout_ms
    FROM promises promise
    JOIN requested request ON request.id = promise.id
    RETURNING
        topic
),

notified AS (
    SELECT PG_NOTIFY('reaper_task_' || MD5(topic), '')
    FROM (SELECT DISTINCT topic FROM tasks) topics
)

SELECT
    promise.id,
    promise.idempotency_key,
    promise.state,
    promise.root_id,
    promise.result::text AS result_json,
    promise.error::text AS error_json,
    promise.due_at,
    promise.expires_at,
    promise.delete_after,
    promise.settled_at,
    (SELECT COUNT(*) FROM notified) AS notifications
FROM returned promise
ORDER BY promise.id;
