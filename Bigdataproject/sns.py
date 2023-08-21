import pandas as pd
import snscrape.modules.twitter as sntwitter

def run_twitter_etl():

    limit = 500
    query = "Lambton"
    tweet_list = []


    for tweets in sntwitter.TwitterSearchScraper(query).get_items():

        if len(tweet_list)==limit:
            break
        else:
            tweet_list.append([tweets.date,tweets.user.username,tweets.content])

    #Airflow is ready
    #standalone | Login with username: admin  password: dkqWhkVQKtnXygGg

    df = pd.DataFrame(tweet_list,columns=['date','User','Tweet'])


    print(df)
    
    df.to_csv("s3://bigdataprojectlambton/Bigdata.csv") 



