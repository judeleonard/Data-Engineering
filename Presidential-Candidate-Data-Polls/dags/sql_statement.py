create_twitter_fact = ("""
    DROP TABLE IF EXISTS twitter_fact;
    CREATE TABLE twitter_fact(
	tweet_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY, 
	datetime TIMESTAMP WITH TIMEZONE NOT NULL, 
	location VARCHAR(255) NULL, 
	text text NOT NULL,
	hashtag VARCHAR NULL, 
	candidate VARCHAR(255) NOT NULL, 
    PRIMARY KEY(tweet_id)
    );

""")


create_twitter_dim = ("""
    DROP TABLE IF EXISTS twitter_dim;
    CREATE TABLE twitter_dim(
	tweet_id BIGINT NOT NULL, 
	text text NOT NULL, 
	sentiment_score VARCHAR(255) NOT NULL 	
    );

""")


create_candidate_analytics_table = ("""
	CREATE TABLE IF NOT EXISTS candidate_table(
	datetime TIMESTAMP WITH TIMEZONE NOT NULL,
	location NULL,
	atiku_review VARCHAR(255) NULL,
	tinubu_review VARCHAR(255) NULL,
	peterobi_review VARCHAR(255) NULL,
	general_presidential_review VARCHAR(255) NULL,
	text text NOT NULL,
	hashtag VARCHAR NULL,
	candidate VARCHAR(255) NOT NULL,
	sentiment_score VARCHAR(255) NOT NULL
	);

""")


drop_staging_tables = ("""
    DROP TABLE IF EXISTS twitter_fact;
    DROP TABLE IF EXISTS article_fact;

""")


load_candidate_analytics_table = """
    INSERT INTO candidate_table(
		datetime, 
		atiku_review,
		tinubu_review,
		peterobi_review,
		general_presidential_review,
		location, 
		text,
		hashtag,
		candidate,
		sentiment_score)

	WITH transform_cte AS 
        (
		SELECT tf.tweet_id, tf.datetime, tf.text, tf.location, tf.hashtag,
				tf.candidate, td.tweet_id, td.sentiment_score 
		FROM  twitter_fact as tf
		JOIN twitter_dim td ON tf.twitter_id = td.twitter_id	
        )	

	(SELECT
		datetime,
		location,
		text,
		hashtag,
		candidate,
		sentiment_score
		
	FROM transform_cte
	);

""" 


back_fills = ("""
	INSERT INTO candidate_table(atiku_review)
	SELECT sentiment_score FROM candidate_table WHERE candidate = 'Atiku' AS atiku_review; 

	INSERT INTO candidate_table(tinubu_review)
	SELECT sentiment_score FROM candidate_table WHERE candidate = 'Tinubu' AS tinubu_review; 

	INSERT INTO candidate_table(peterobi_review)
	SELECT sentiment_score FROM candidate_table WHERE candidate = 'PeterObi' AS peterobi_review; 

	INSERT INTO candidate_table(general_presidential_review)
	SELECT sentiment_score FROM candidate_table WHERE candidate = '2023Presidentialcandidate' AS general_presidential_review; 

""")

