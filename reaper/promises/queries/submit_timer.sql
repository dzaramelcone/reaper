WITH inserted AS (
    INSERT INTO reaper.promises (
        id, idempotency_key, root_id, due_at, delete_after
    ) VALUES (
        $1,
        $2,
        $3,
        $4,
        CASE
            WHEN $3::text IS NULL
                THEN $4::timestamptz + ($5 * interval '1 millisecond')
        END
    )
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
    WHERE p.id = $1
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
    FROM inserted
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
    p.settled_at
FROM returned p;
