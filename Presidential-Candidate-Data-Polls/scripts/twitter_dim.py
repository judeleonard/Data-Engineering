import tweepy
import pandas as pd
from airflow.models import Variable
import re
import nltk
from nltk.corpus import stopwords
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import time

from article_dim import preprocess_text, sentiment_scores

######## twitter config ####################################
ACCESS_KEY = str(Variable.get("access_key"))
ACCESS_SECRET = str(Variable.get("access_secret"))
CONSUMER_KEY = str(Variable.get("consumer_key"))
CONSUMER_SECRET = str(Variable.get("consumer_secret"))


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)


tweets = []

def query_search_to_csv(text_query,count):
    try:
        # Creation of query method using parameters
        tweets = tweepy.Cursor(api.search_tweets,q=text_query+' -filter:retweets', tweet_mode='extended',
        ).items(count)

        # Pulling information from tweets iterable object
        tweets_list = [[tweet.id, 
                        tweet.full_text.replace('\n',' ').encode('utf-8')
                        ] for tweet in tweets]

        # Creation of dataframe from tweets list
        # Add or remove columns as you remove tweet information
        tweet_dim_df = pd.DataFrame(tweets_list,columns=['tweet_id', 'text'])
        tweet_dim_df['processed_text'] = tweet_dim_df['text'].apply(preprocess_text)
        tweet_dim_df['sentiment_score'] = tweet_dim_df['processed_text'].apply(sentiment_scores)
        tweet_dim_df.drop(['processed_text', 'text'], axis = 1)
    

        # Converting dataframe to CSV and saving each subject's tweet to a csv file 
        tweet_dim_df.to_csv('/opt/airflow/staging/dimensions-data/{}-tweet_dim.csv'.format(text_query), sep=',', index = False)
    except ValueError as e:
        print(e)



def main():
    """base function"""

    keywords = ['PeterObi', 'Tinubu', 'Atiku', '2023Presidentialcandidate']
    for keyword in keywords:
        text_query = keyword
        count = 500
        query_search_to_csv(text_query, count)



if __name__ == '__main__':
    main()
