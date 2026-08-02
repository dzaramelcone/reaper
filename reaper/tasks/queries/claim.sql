SELECT
    p.id,
    p.root_id,
    t.function,
    t.version,
    t.topic,
    t.input::text AS input_json,
    t.depth,
    t.max_failures,
    t.execution_timeout_ms,
    COALESCE((
        SELECT
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'id', child.id,
                'state', child.state,
                'result', child.result,
                'error', child.error,
                'settled_at_ms', FLOOR(EXTRACT(EPOCH FROM child.settled_at) * 1000)::bigint
            ) ORDER BY child.id)
        FROM reaper.waits w
        JOIN reaper.promises child ON child.id = w.awaited_id
        WHERE w.waiter_id = p.id
    ), '[]'::jsonb)::text AS waits_json,
    SET_CONFIG(
        'idle_in_transaction_session_timeout',
        t.execution_timeout_ms::text || 'ms',
        true
    ) AS transaction_timeout,
    SET_CONFIG(
        'lock_timeout',
        GREATEST(t.execution_timeout_ms / 2, 1)::text || 'ms',
        true
    ) AS lock_timeout,
    SET_CONFIG(
        'statement_timeout',
        t.execution_timeout_ms::text || 'ms',
        true
    ) AS statement_timeout
FROM reaper.tasks t
JOIN reaper.promises p ON p.id = t.promise_id
WHERE
    t.topic = $1
    AND NOT EXISTS (
        SELECT 1
        FROM UNNEST($2::text [], $3::integer []) excluded (function, version)
        WHERE excluded.function = t.function AND excluded.version = t.version
    )
    AND t.pending_waits = 0
    AND t.available_at <= STATEMENT_TIMESTAMP()
    AND p.state = 'pending'
    AND p.expires_at > STATEMENT_TIMESTAMP()
ORDER BY t.priority DESC, t.available_at ASC, t.promise_id ASC
LIMIT 1
FOR UPDATE OF t SKIP LOCKED;
