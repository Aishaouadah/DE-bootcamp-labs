-- SELECT * FROM events;
-- SELECT MAX(event_time) , MIN(event_time) FROM events;
-- -- -------------------
-- DROP TABLE users_cumulated
-- CREATE TABLE users_cumulated(
-- user_id TEXT,
-- -- the list of dates in the past where the user was active 
-- dates_active DATE[],
-- -- the current date for the user
-- date DATE,
-- primary key(user_id, date)
-- )

---------

INSERT INTO users_cumulated
WITH yesterday AS(
SELECT * FROM users_cumulated
WHERE date = DATE('2023-01-30')
),
today AS(
SELECT 
CAST(user_id AS TEXT),
DATE(CAST(event_time AS TIMESTAMP)) AS date_active,
count(url),
count(referrer)
FROM events
WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31') and user_id is not null
GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT 
COALESCE(t.user_id, y.user_id) AS user_id,
CASE 
	WHEN y.dates_active is NULL
	THEN ARRAY[t.date_active]
	WHEN t.date_active is NULL then y.dates_active
	ELSE  ARRAY[t.date_active] || y.dates_active
	END
AS dates_active,
COALESCE(t.date_active, y.date + INTERVAL '1 DAY') AS date
FROM today t
FULL OUTER JOIN yesterday y 
on t.user_id = y.user_id


SELECT * FROM users_cumulated
WHERE user_id = '5804919602898318000';
--------------

WITH users AS(
	SELECT * FROM users_cumulated
	WHERE date = DATE('2023-01-31')
),
series AS(
	SELECT * 
	FROM generate_series(DATE('2023-01-01'),DATE('2023-01-31'), INTERVAL '1 DAY') as series_date
),
place_holder_ints AS 
(
SELECT 
	CASE WHEN 
	dates_active @> ARRAY[DATE(series_date)]
	THEN CAST(POW (2, 32 - (date-DATE(series_date))) AS BIGINT)
	ELSE 0
	END as placeholder_int_value,
	*
FROM users cross join series
)

SELECT 
user_id,
CAST(CAST(SUM(placeholder_int_value) AS BIGINT)AS BIT(32)),
BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT)AS BIT(32))) > 0 AS dim_is_monthly_active,
BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &
CAST(CAST(SUM(placeholder_int_value) AS BIGINT)AS BIT(32))) > 0  AS dim_is_weekly_active,
BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) &
CAST(CAST(SUM(placeholder_int_value) AS BIGINT)AS BIT(32))) > 0  AS dim_is_daily_active

	

FROM place_holder_ints
GROUP BY user_id


