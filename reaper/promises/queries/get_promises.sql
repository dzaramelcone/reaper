SELECT
    id,
    idempotency_key,
    state,
    root_id,
    result::text AS result_json,
    error::text AS error_json,
    due_at,
    expires_at,
    delete_after,
    settled_at
FROM reaper.promises
WHERE id = ANY($1::text [])
ORDER BY id;
