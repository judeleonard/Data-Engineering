import pandas as pd 
from pygooglenews import GoogleNews
from newspaper import Article
from newspaper import Config


gn = GoogleNews(lang = 'en', country = 'NG')

def get_candidate_info(query):
    data = []
    search = gn.search(query)
    newsitem = search['entries']
    for item in newsitem:
        story = {
            'id' : item.id,
            'title' : item.title,
            'link': item.link,
            'published': item.published,
            'source': item.source,
            'candidate': f'{query}',
            'summary': item.summary
        }
        data.append(story)
    return data


############ Feature engineering pipeline here ##########################
#df['new_source'] = df['title'].apply(lambda x: str(x).split('-')[1])
#df['article'] = df['link'].apply(extract_articles)    

if __name__ == '__main__':
    query_list = ['PeterObi', 'Tinubu', 'Atiku', '2023Presidentialcandidate']
    for query in query_list:
        dataset = get_candidate_info(query)
        print('====== fetching result =======')
        df = pd.DataFrame(dataset)
        # extract the news source
        df['news_source'] = df['title'].apply(lambda x: str(x).split('-')[1])
        df.to_csv("/opt/airflow/staging/facts/{}-result.csv".format(query), sep=',', index=False)
