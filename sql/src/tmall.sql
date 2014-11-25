-- [NOTE] SQL does not support comment line, we use it here only for clearity

DROP TABLE IF EXISTS tmall_user_brand;
CREATE TABLE tmall_user_brand (
    user_id string,
    brand_id string,
    type string COMMENT "click-0, buy-1, collect-2, shopping_cart-3",
visit_datetime string
);

-- 1) get to know data
SELECT distinct(substr(visit_datetime,1,2)) as month
FROM tmall_user_brand;

SELECT min(visit_datetime) as min_day, max(visit_datetime) as max_day
FROM tmall_user_brand;

SELECT 
    count(*) as total_cnt,
    count(distinct user_id) as total_user,
    count(distinct brand_id) as total_brand
FROM tmall_user_brand;

SELECT substr(visit_datetime,1,2) as month, type, count(*) as cnt      
FROM tmall_user_brand
GROUP BY substr(visit_datetime,1,2), type;

CREATE TABLE tmp_tmall_sample_by_user as
SELECT * FROM tmall_user_brand 
WHERE sample(100, 1, user_id) = true;

CREATE TABLE tmp_tmall_sample as
SELECT user_id, brand_id, type, visit_datetime
FROM(
    SELECT
        user_id,
        brand_id,
        type,
        visit_datetime,
        cluster_sample(100, 1) over (partition by user_id) as flag
    FROM tmall_user_brand
)t1
WHERE flag = true;

-- 2) two simple ways to work out
CREATE TABLE tmp_tmall_predict as
SELECT /*+ MAPJOIN(b) */
    distinct user_id, brand
FROM tmall_user_brand
LEFT OUTER JOIN 
(
SELECT wm_concat(',', brand_id) as brand
FROM(
        SELECT 
brand_id,
            row_number() over (partition by 1 order by buy_cnt desc) as rank
        FROM(
            SELECT brand_id, count(*) as buy_cnt 
FROM tmall_user_brand
            WHERE type='1'
            GROUP BY brand_id
) t1
    ) t2
WHERE t2.rank <= 5
) b;

SELECT user_id, wm_concat(',', brand_id) as brand
FROM(
SELECT 
user_id, 
brand_id, 
row_number() over (partition by user_id order by cnt desc) as rank
    FROM(
        SELECT user_id, brand_id, count(brand_id) as cnt
        FROM tmall_user_brand
        WHERE type='0' and substr(visit_datetime,1,2) > '0715'
        GROUP BY user_id, brand_id
    )t1
)t2
WHERE rank <= 5
GROUP BY user_id;

-- dive into the problem
-- step 1) generate features
CREATE VIEW tmall_train_b_cvr as
SELECT
    brand_id,
    case when click_cnt > 0 then buy_cnt/click_cnt else 0 end as cvr
FROM(
    SELECT
        brand_id,
        count(distinct case when type='1' then user_id else null end) as buy_cnt,
        count(distinct case when type='0' then user_id else null end) as click_cnt
    FROM tmall_user_brand
    WHERE visit_datetime <= '0715'
    GROUP BY brand_id
)t1;

CREATE VIEW tmall_train_ub_action as
SELECT
    user_id,
    brand_id,
    sum(case when type='1' then 1 else 0 end) as buy_cnt,
    sum(case when type='0' and visit_datetime > '0708' then 1 else 0 end) as click_d7,
    sum(case when type='2' and visit_datetime > '0708' then 1 else 0 end) as collect_d7,
    sum(case when type='3' and visit_datetime > '0708' then 1 else 0 end) as shopping_cart_d7,
    sum(case when type='0' and visit_datetime > '0712' then 1 else 0 end) as click_d3,
    sum(case when type='2' and visit_datetime > '0712' then 1 else 0 end) as collect_d3,
    sum(case when type='3' and visit_datetime > '0712' then 1 else 0 end) as shopping_cart_d3
FROM tmall_user_brand
WHERE visit_datetime <= '0715'
GROUP BY user_id, brand_id;

CREATE VIEW tmall_train_ub_action as
SELECT
    user_id,
    brand_id,
    sum(case when type='1' then 1 else 0 end) as buy_cnt,
    sum(case when type='0' and visit_datetime > '0708' then 1 else 0 end) as click_d7,
    sum(case when type='2' and visit_datetime > '0708' then 1 else 0 end) as collect_d7,
    sum(case when type='3' and visit_datetime > '0708' then 1 else 0 end) as shopping_cart_d7,
    sum(case when type='0' and visit_datetime > '0712' then 1 else 0 end) as click_d3,
    sum(case when type='2' and visit_datetime > '0712' then 1 else 0 end) as collect_d3,
    sum(case when type='3' and visit_datetime > '0712' then 1 else 0 end) as shopping_cart_d3
FROM tmall_user_brand
WHERE visit_datetime <= '0715'
GROUP BY user_id, brand_id;

-- step 2) positive/negative samples
CREATE TABLE tmall_ub_ifbuy AS
SELECT
    user_id
    ,brand_id
    ,sum(case when type=0 and visit_datetime>'0715' then 1.0 else 0.0 end) as buy_final
    ,sum(case when visit_datetime<='0715' then 1.0 else 0.0 end) as visit_past
FROM tmall_user_brand
GROUP BY brand_id,user_id;

SELECT sum(pos) as pos_cnt, sum(neg) as neg_cnt
FROM(
SELECT
        user_id
        ,brand_id
        ,case when visit_past > 0 and buy_final > 0 then 1 else 0 end as pos
        ,case when visit_past > 0 and buy_final = 0 then 1 else 0 end as neg
FROM tmall_ub_ifbuy
) a;

CREATE TABLE tmall_pos AS
SELECT
t1.user_id, t1.brand_id, t1.buy_day
FROM (
    SELECT
        user_id,
        brand_id,
        visit_datetime as buy_day
    FROM tmall_user_brand
    WHERE visit_datetime > '0715'
    AND type=0
    GROUP BY user_id, brand_id, visit_datetime
)t1
JOIN
(SELECT * FROM tmall_ub_ifbuy WHERE visit_past > 0) t2
ON t1.user_id = t2.user_id
AND t1.brand_id = t2.brand_id
AND t2.user_id is not null;

CREATE TABLE tmall_neg AS
SELECT brand_id, user_id
FROM(
    SELECT
    brand_id,user_id,
    cluster_sample(19,7) over(partition by 1) as flag
    FROM tmall_ub_ifbuy
    WHERE visit_past>0 and buy_final=0
)t1
WHERE t1.flag = true;

CREATE VIEW tmall_pos_view as
SELECT
    brand_id, user_id,
    cluster_sample(10,3) over (partition by buy_day) as f
FROM tmall_pos;

CREATE VIEW tmall_neg_view as
SELECT
    brand_id, user_id,
    cluster_sample(10,3) over (partition by 1) as f
FROM tmall_pos;

CREATE TABLE tmall_train_sample as
SELECT t2.*, t1.flag
FROM(
    SELECT user_id, brand_id, flag
    FROM(
        SELECT user_id, brand_id, 1 as flag FROM tmall_pos_view WHERE f = false
        UNION ALL
        SELECT user_id, brand_id, 0 as flag FROM tmall_neg_view WHERE f = false
    )t
)t1
JOIN tmall_train_features t2
ON t1.user_id = t2.user_id
AND t1.brand_id = t2.brand_id;

CREATE TABLE tmall_test_sample as
SELECT t2.*, t1.flag
FROM(
    SELECT user_id, brand_id, flag
    FROM(
        SELECT user_id, brand_id, 1 as flag FROM tmall_pos_view WHERE f = true
        union all
        SELECT user_id, brand_id, 0 as flag FROM tmall_neg_view WHERE f = true
    )t
)t1
JOIN tmall_train_features t2
ON t1.user_id = t2.user_id
AND t1.brand_id = t2.brand_id;

--step 3) generate model

--step 4) validate model
CREATE TABLE tmall_test_sample_predict as
SELECT user_id, wm_concat(',', brand_id) as brand
FROM (
    SELECT user_id, brand_id, row_number() over (partition by user_id order by score) as rank
    FROM (
        SELECT 
user_id, brand_id,
            sum( (2.0/3 * log(10,click_d3+1) + 1.0/7 * log(10,click_d7)) * 0.1 +
                 (2.0/3 * log(10,collect_d3+1) + 1.0/7 * log(10,collect_d7)) * 0.2 +
                 (2.0/3 * log(10,shopping_cart_d3+1) + 1.0/7*log(10,shopping_cart_d7)) * 0.7 )
            * pow(cvr,1.5) as score
        FROM tmall_test_sample
        WHERE cvr > 0
        GROUP BY user_id, brand_id, cvr
    )t1
)t2
WHERE t2.rank <= 5
GROUP BY user_id;

CREATE TABLE tmall_test_sample_real as
SELECT user_id, wm_concat(',', brand_id) as brand
FROM (
    SELECT distinct user_id, brand_id
    FROM tmall_test_sample
    WHERE flag = 1
) t1
GROUP BY user_id;

CREATE TABLE tmall_test_sample_real as
SELECT user_id, wm_concat(distinct ',', brand_id) as brand
FROM tmall_test_sample
WHERE flag = 1
GROUP BY user_id;

CREATE TABLE tmp_predict as
SELECT trans_array(1, ',', user_id, brand) as (user_id, brand_pre)
FROM tmall_test_sample_predict;

CREATE TABLE tmp_real as
SELECT trans_array(1, ',', user_id, brand) as (user_id, brand_real)
FROM tmall_test_sample_real;

SELECT sum(cnt) as hits
FROM(
    SELECT count(*) as cnt
    FROM tmp_predict t1
    JOIN tmp_real t2
    on t1.user_id = t2.user_id and t1.brand_pre = t2.brand_real
    GROUP BY t1.user_id
)t3;

SELECT count(*) as predict FROM tmp_predict;

SELECT count(*) as real FROM tmp_real;

--step 5) predict













