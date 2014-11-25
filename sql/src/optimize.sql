-- [NOTE] SQL does not support comment line, we use it here only for clearity

SELECT month, count(user_id) as cnt
FROM t1 
GROUP BY month;

SELECT month, count(distinct user_id) as cnt
FROM t1 
GROUP BY month;

EXPLAIN SELECT month, count(distinct user_id) as cnt
FROM t1 
GROUP BY month;

-- optimized
SELECT month, count(*) as cnt
FROM (
    SELECT distinct month, user_id
    FROM t1 
) a 
GROUP BY month;

EXPLAIN SELECT month, count(*) as cnt
FROM (
    SELECT distinct month, user_id
    FROM t1 
) a 
GROUP BY month;
