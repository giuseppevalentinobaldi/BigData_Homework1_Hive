CREATE TABLE IF NOT EXISTS pair_user (id STRING, product_id STRING, user_id STRING, profile_name STRING, helpfulness_numerator STRING, helpfulness_denominator STRING, score INT, time STRING, summary STRING, text STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/marco/Scrivania/BigData_progetto1/dati/FineFoodReviews/amazon/1999_2006.csv' OVERWRITE INTO TABLE pair_user;

add jar /home/marco/Scrivania/BigData_progetto1/Hive/jar/homework1_hive.jar;

CREATE TEMPORARY FUNCTION number AS 'pairUser.NumberUser';

CREATE TABLE IF NOT EXISTS pair_user_score4 AS
SELECT *, number(user_id) AS number_user
FROM(
	SELECT user_id, product_id
	FROM pair_user
	WHERE score >= 4
	GROUP BY user_id, product_id
	ORDER BY user_id ASC
	) AS a1;


SELECT t.user1, t.user2, t.list_prod
FROM(
	SELECT t1.user_id AS user1, t2.user_id AS user2, t1.number_user AS number_user1, t2.number_user AS number_user2, concat_ws('\t', collect_set(t1.product_id)) AS list_prod, COUNT(1) AS cont
	FROM
		pair_user_score4 AS t1
		JOIN
		pair_user_score4 AS t2
		ON (t1.product_id = t2.product_id)
	GROUP BY t1.user_id, t2.user_id, t1.number_user, t2.number_user
	ORDER BY user1 ASC, user2 ASC
	) AS t
WHERE t.number_user1 < t.number_user2 AND t.cont >= 3;
