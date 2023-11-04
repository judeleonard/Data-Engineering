import pandas as pd 
from pygooglenews import GoogleNews
from newspaper import Article
from newspaper import Config

from airflow.models import Variable
import re
import nltk
from nltk.corpus import stopwords
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer


gn = GoogleNews(lang = 'en', country = 'NG')

def get_candidate_info(query):
    data = []
    search = gn.search(query)
    newsitem = search['entries']
    for item in newsitem:
        story = {
            'id' : item.id,
            'link': item.link,
            'summary': item.summary
        }
        data.append(story)
    return data


def extract_articles(link):
  """This is a function that uses a third party library to extract article bodies from url"""


  user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
  config = Config()
  config.browser_user_agent = user_agent
  try:
    page = Article(link, config=config)
    page.download()
    page.parse()
    # returning only the value in page.text truncates the article
    # Using the print statement here to return the full body of the article
    #body = print(page.text)  
    body = page.text
    return body
  except:
    pass



def remove_newline(text):
    return re.sub('\n', ' ', str(text.lower()))
def remove_symbols(text):
    re.sub(r'^\x00-\x7F+', ' ', text)
    return re.sub(r'[@!.,(\/&)?:#*...-;'']', '', str(text)) 
def remove_urls(text):
    return re.sub(r'http\S+', '', str(text))
 

def preprocess_text(text):
    text = remove_urls(text)
    text = remove_symbols(text)
    text = remove_newline(text)

    return text


def sentiment_scores(sentence):
    """This function calculates the sentiment scores for each tweet"""
 
    sid_obj = SentimentIntensityAnalyzer()
 
    sentiment_dict = sid_obj.polarity_scores(sentence)
 
    # decide sentiment as positive, negative and neutral
    if sentiment_dict['neu'] > 0.80 and sentiment_dict['neu'] > sentiment_dict['pos'] and sentiment_dict['neu'] > sentiment_dict['neg']:
        result="neutral"
    elif sentiment_dict['pos'] > sentiment_dict['neg']:
        result="positive"
    else:
        result="negative"
    return result


def main():
    query_list = ['PeterObi', 'Tinubu', 'Atiku', '2023Presidentialcandidate']
    for query in query_list:
        dataset = get_candidate_info(query)
        article_dim_df = pd.DataFrame(dataset)
        article_dim_df['article'] = article_dim_df['link'].apply(extract_articles)  
        # preprocess article body for sentiment
        article_dim_df['processed_article'] = article_dim_df['article'].apply(preprocess_text)
        article_dim_df['sentiment_score'] = article_dim_df['processed_article'].apply(sentiment_scores)
        article_dim_df.drop(['article', 'processed_article', 'link',], axis = 1)
        article_dim_df.to_csv("/opt/airflow/staging/dimensions-data/{}-article_dim.csv".format(query), sep=',', index=False)







if __name__ == '__main__':
    main()