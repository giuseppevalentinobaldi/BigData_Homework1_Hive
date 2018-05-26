CREATE TABLE IF NOT EXISTS top5 (id STRING, product_id STRING, user_id STRING, profile_name STRING, helpfulness_numerator STRING, helpfulness_denominator STRING, score INT, time STRING, summary STRING, text STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/marco/Scrivania/BigData_progetto1/dati/FineFoodReviews/amazon/1999_2006.csv' OVERWRITE INTO TABLE top5;

add jar /home/marco/Scrivania/BigData_progetto1/Hive/jar/homework1_hive.jar;

CREATE TEMPORARY FUNCTION time_conversion AS 'top5.UnixTime2Date';
CREATE TEMPORARY FUNCTION rank AS 'top5.Rank';


SELECT t2.month, t2.product_id, t2.average
FROM(	
	SELECT *, rank(month) AS row_number
	FROM(	
		SELECT time_conversion(time) AS month, product_id, AVG(score) AS average
		FROM top5
		GROUP BY time_conversion(time), product_id
		ORDER BY month ASC, average DESC
		) AS t1 
	) AS t2
WHERE t2.row_number < 5;