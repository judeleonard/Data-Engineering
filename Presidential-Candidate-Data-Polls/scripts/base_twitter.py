import pandas as pd 
import tweepy
from airflow.models import Variable
import time


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
        tweets_list = [[tweet.created_at, 
                        tweet.id, 
                        tweet.user.location, 
                        [e['text'] for e in tweet._json['entities']['hashtags']], f'{text_query}',
                        tweet.full_text.replace('\n',' ').encode('utf-8')
                        ] for tweet in tweets]

        # Creation of dataframe from tweets list
        tweets_df = pd.DataFrame(tweets_list,columns=['datetime', 'tweet_id', 'location', 'hashtags', 'subject', 'text'])

        # Converting dataframe to CSV and saving each subject's tweet to a csv file 
        tweets_df.to_csv('/opt/airflow/staging/facts/{}-tweets.csv'.format(text_query), sep=',', index = False)
        

    except BaseException as e:
        print('failed on_status,',str(e))
        time.sleep(3)


if __name__ == '__main__':
    keywords = ['PeterObi', 'Tinubu', 'Atiku', '2023Presidentialcandidate']
    for keyword in keywords:
        text_query = keyword
        count = 500  # fetch 500 rows of data per keyword for every runtime to adhere to twitter rules
        # Calling function to query X amount of relevant tweets and create a CSV file
        query_search_to_csv(text_query, count)
        print('==== fetching data ========')
