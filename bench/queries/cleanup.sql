DELETE FROM reaper.promises
WHERE root_id IS NULL AND id LIKE $1 || '%'
RETURNING id;
