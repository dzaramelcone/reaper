SELECT p.id
FROM reaper.promises p
JOIN reaper.tasks t ON t.promise_id = p.id
WHERE p.id = $1 AND p.state = 'pending'
FOR UPDATE OF p;
