CREATE TABLE IF NOT EXISTS user_top10 (id STRING, product_id STRING, user_id STRING, profile_name STRING, helpfulness_numerator STRING, helpfulness_denominator STRING, score INT, time STRING, summary STRING, text STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/marco/Scrivania/BigData_progetto1/dati/FineFoodReviews/amazon/1999_2006.csv' OVERWRITE INTO TABLE user_top10;


SELECT t2.user_id, t2.product_id, t2.max_score
FROM(	
	SELECT *, row_number() OVER (PARTITION BY user_id) AS row_number
	FROM(	
		SELECT user_id, product_id, MAX(score) AS max_score
		FROM user_top10
		GROUP BY user_id, product_id
		ORDER BY user_id ASC, max_score DESC
		) AS t1 
	) AS t2
WHERE t2.row_number <= 10;