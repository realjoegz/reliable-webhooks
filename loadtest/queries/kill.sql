-- Every event of the scenario with the number of its deliveries that succeeded. loadtest/kill.js
-- sends each event to the three endpoints of its tenant.
SELECT count(*) AS events,
       count(*) FILTER (WHERE deliveries = 3 AND succeeded = 3) AS events_with_all_three_succeeded
FROM (
  SELECT e.id,
         count(d.id) AS deliveries,
         count(d.id) FILTER (WHERE d.state = 'succeeded') AS succeeded
  FROM event e
  LEFT JOIN delivery d ON d.event_id = e.id
  WHERE e.created_at >= :'started'
  GROUP BY e.id
) per_event;

-- The successful attempts by the replica that recorded them, before and after the kill.
SELECT a.replica,
       count(*) FILTER (WHERE d.completed_at <= :'killed') AS recorded_before_the_kill,
       count(*) FILTER (WHERE d.completed_at > :'killed') AS recorded_after_the_kill
FROM delivery_attempt a
JOIN delivery d ON d.id = a.delivery_id
WHERE d.created_at >= :'started' AND a.outcome = 'success'
GROUP BY a.replica
ORDER BY a.replica;

-- The deliveries that the killed replica held: a later claim logged their first attempt as
-- unknown. How long after the kill each of them was recorded as succeeded.
SELECT count(*) AS deliveries,
       round(min(extract(epoch FROM d.completed_at - :'killed')), 1) AS first_seconds_after_it,
       round(max(extract(epoch FROM d.completed_at - :'killed')), 1) AS last_seconds_after_it
FROM delivery d
WHERE d.created_at >= :'started'
  AND EXISTS (SELECT 1 FROM delivery_attempt a WHERE a.delivery_id = d.id AND a.outcome = 'unknown');
