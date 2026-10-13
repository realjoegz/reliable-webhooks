-- Deliveries a second, from the creation of the first delivery to the last recorded 2xx.
SELECT count(*) AS succeeded,
       round(extract(epoch FROM max(completed_at) - min(created_at)), 1) AS seconds,
       round(count(*) / extract(epoch FROM max(completed_at) - min(created_at)), 1) AS per_second
FROM delivery
WHERE created_at >= :'started' AND state = 'succeeded';

-- How long a delivery took from its creation, which is the start of the transaction that
-- accepted its event, to the start of the transaction that recorded its 2xx. Both are times of
-- the clock of the database, so the shortest is below zero where that clock was set back
-- between the two.
SELECT round(min(seconds), 3) AS min_seconds,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY seconds)::numeric, 3) AS p50_seconds,
       round(percentile_cont(0.9) WITHIN GROUP (ORDER BY seconds)::numeric, 3) AS p90_seconds,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY seconds)::numeric, 3) AS p99_seconds,
       round(percentile_cont(0.999) WITHIN GROUP (ORDER BY seconds)::numeric, 3) AS p999_seconds,
       round(max(seconds), 3) AS max_seconds,
       round(avg(seconds), 3) AS mean_seconds
FROM (
  SELECT extract(epoch FROM completed_at - created_at) AS seconds
  FROM delivery
  WHERE created_at >= :'started' AND state = 'succeeded'
) took;

-- The same times in the buckets of webhooks_delivery_end_to_end_seconds, and how many of them
-- are below zero.
SELECT count(*) FILTER (WHERE completed_at < created_at) AS below_0,
       count(*) FILTER (WHERE completed_at - created_at <= interval '1 second') AS le_1,
       count(*) FILTER (WHERE completed_at - created_at <= interval '5 seconds') AS le_5,
       count(*) FILTER (WHERE completed_at - created_at <= interval '15 seconds') AS le_15,
       count(*) FILTER (WHERE completed_at - created_at <= interval '60 seconds') AS le_60,
       count(*) AS all_deliveries
FROM delivery
WHERE created_at >= :'started' AND state = 'succeeded';

-- Those times for the deliveries that were created in each half minute of the scenario. The
-- time of day is that of the database, as in its log.
SELECT to_char(date_bin('30 seconds', d.created_at, first.created_at), 'HH24:MI:SS')
         AS created_from,
       count(*) AS deliveries,
       round(percentile_cont(0.5) WITHIN GROUP (
         ORDER BY extract(epoch FROM d.completed_at - d.created_at))::numeric, 3) AS p50_seconds,
       round(percentile_cont(0.99) WITHIN GROUP (
         ORDER BY extract(epoch FROM d.completed_at - d.created_at))::numeric, 3) AS p99_seconds,
       round(max(extract(epoch FROM d.completed_at - d.created_at)), 3) AS max_seconds
FROM delivery d
CROSS JOIN (SELECT min(created_at) AS created_at FROM delivery WHERE created_at >= :'started') first
WHERE d.created_at >= :'started' AND d.state = 'succeeded'
GROUP BY created_from
ORDER BY created_from;
