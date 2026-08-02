WITH doomed AS (
    SELECT root.id
    FROM reaper.promises root
    WHERE
        root.root_id IS NULL
        AND root.delete_after <= $1
        AND root.state <> 'pending'
        AND NOT EXISTS (
            SELECT 1
            FROM reaper.promises child
            WHERE
                child.graph_id = root.id
                AND child.root_id IS NOT NULL
                AND child.state = 'pending'
        )
    ORDER BY root.delete_after, root.id
    LIMIT $2
    FOR UPDATE OF root SKIP LOCKED
)

DELETE FROM reaper.promises root
USING doomed
WHERE root.id = doomed.id
RETURNING root.id;
