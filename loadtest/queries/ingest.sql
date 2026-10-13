-- For each two minutes from the first event on, which is a step of loadtest/ingest.js: the
-- deliveries that were created in it, and how long they took from their creation to the
-- transaction that recorded their 2xx.
SELECT floor(extract(epoch FROM d.created_at - first.created_at) / 120) + 1 AS step,
       count(*) AS deliveries,
       round(percentile_cont(0.5) WITHIN GROUP (
         ORDER BY extract(epoch FROM d.completed_at - d.created_at))::numeric, 3) AS p50_seconds,
       round(percentile_cont(0.99) WITHIN GROUP (
         ORDER BY extract(epoch FROM d.completed_at - d.created_at))::numeric, 3) AS p99_seconds,
       round(max(extract(epoch FROM d.completed_at - d.created_at)), 3) AS max_seconds
FROM delivery d
CROSS JOIN (SELECT min(created_at) AS created_at FROM delivery WHERE created_at >= :'started') first
WHERE d.created_at >= :'started' AND d.state = 'succeeded'
GROUP BY step
ORDER BY step;
