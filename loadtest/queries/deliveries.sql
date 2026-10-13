-- What became of the events of the scenario and of their deliveries, how many of the deliveries
-- were made after the last event was accepted, and how long after it the last one was.
WITH last_event AS (
  SELECT max(created_at) AS created_at FROM event WHERE created_at >= :'started'
)
SELECT (SELECT count(*) FROM event WHERE created_at >= :'started') AS events,
       count(*) AS deliveries,
       count(*) FILTER (WHERE d.state = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE d.state <> 'succeeded') AS not_succeeded,
       count(*) FILTER (WHERE d.attempts > 1) AS claimed_more_than_once,
       count(*) FILTER (WHERE d.completed_at > last_event.created_at) AS made_after_the_last_event,
       round(extract(epoch FROM max(d.completed_at) - max(last_event.created_at)), 1)
         AS seconds_from_it_to_the_last_delivery
FROM delivery d
CROSS JOIN last_event
WHERE d.created_at >= :'started';

-- The attempts that were recorded for those deliveries.
SELECT a.outcome, a.http_status, a.error, count(*) AS attempts
FROM delivery_attempt a
JOIN delivery d ON d.id = a.delivery_id
WHERE d.created_at >= :'started'
GROUP BY a.outcome, a.http_status, a.error
ORDER BY a.outcome, a.http_status, a.error;
