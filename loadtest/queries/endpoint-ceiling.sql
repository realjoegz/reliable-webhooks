-- The drain of the backlog, from the first recorded 2xx to the last: the deliveries that each
-- replica made, and both together.
WITH made AS (
  SELECT d.completed_at, a.replica
  FROM delivery d
  JOIN delivery_attempt a ON a.delivery_id = d.id AND a.outcome = 'success'
  WHERE d.created_at >= :'started'
), drain AS (
  SELECT extract(epoch FROM max(completed_at) - min(completed_at)) AS seconds FROM made
)
SELECT coalesce(made.replica, 'both') AS replica,
       count(*) AS deliveries,
       round(drain.seconds, 1) AS seconds,
       round(count(*) / drain.seconds, 1) AS per_second
FROM made
CROSS JOIN drain
GROUP BY GROUPING SETS ((made.replica, drain.seconds), (drain.seconds))
ORDER BY made.replica;

-- The deliveries that were made in each five seconds of the drain, by each replica and by
-- both, and how many a second that is for both. The five seconds are counted from the first
-- recorded 2xx, so the last of them are not full. The time of day is that of the database, as
-- in its log.
SELECT to_char(date_bin('5 seconds', d.completed_at, first.completed_at), 'HH24:MI:SS')
         AS made_from,
       count(*) FILTER (WHERE a.replica = 'replica-1') AS replica_1,
       count(*) FILTER (WHERE a.replica = 'replica-2') AS replica_2,
       count(*) AS both,
       round(count(*) / 5.0, 1) AS both_per_second
FROM delivery d
JOIN delivery_attempt a ON a.delivery_id = d.id AND a.outcome = 'success'
CROSS JOIN (
  SELECT min(completed_at) AS completed_at FROM delivery WHERE created_at >= :'started'
) first
WHERE d.created_at >= :'started'
GROUP BY made_from
ORDER BY made_from;
