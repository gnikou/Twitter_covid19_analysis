import pandas as pd
import os
from datetime import timedelta
import pymongo
from dateutil import parser
from mongoConfig import mongoConfig
import matplotlib.pyplot as plt
import json


# get only suspended users from file
def writef_suspended_users():
    file = f"compliance_result.txt"
    fp = open(file)
    users = []

    while True:
        line = fp.readline()
        if not line:
            break
        try:
            user = json.loads(line)
        except ValueError:
            print('Decoding JSON has failed - value error')
            continue
        users.append(user)

    df = pd.DataFrame(users)

    df = df[df['reason'] == 'suspended']

    df['id'].to_csv("id_suspended_users.csv", index=False, header=None)


# write in a file all tweets ids from suspended users
def writef_tweetids_from_suspended_users(db):
    file = "id_suspended_users.csv"
    df = pd.read_csv(file, header=None)
    user_ids = [int(i) for i in df[0].unique()]
    tweet_ids = []
    for user in user_ids:
        for tweet in db.tweets.find({"user_id": user}, {"id": 1, "user_id": 1, "_id": 0}):
            tweet_ids.append(tweet["id"])

    df = pd.DataFrame(tweet_ids)
    df.to_csv("/Storage/gnikou/sentiment_per_day/tweets_from_suspended_users", index=False, header=None)


# write in separate files daily sentiment score of suspended users' tweets
def write_daily_sentiment_susp_users():
    file = "/Storage/gnikou/sentiment_per_day/tweets_from_suspended_users"
    df = pd.read_csv(file, header=None)
    susp_tweets_ids = [int(i) for i in df[0].unique()]

    for month in range(2, 8):
        for day in range(1, 32):
            list_ids = "tweet_id\tpositive for covid\tnegative for covid\tpositive for lockdown\t" \
                       "negative for lockdown\tpositive for vaccine\tnegative for vaccine\tpositive for conspiracy\t" \
                       "negative for conspiracy\tpositive for masks\tnegative for masks\tpositive for cases\t" \
                       "negative for cases\tpositive for deaths\tnegative for deaths\tpositive for propaganda\t" \
                       "negative for propaganda\tpositive for 5G\tnegative for 5G"
            file = f"sentiment_by_id_day_2020-{month}-{day}.csv"
            if os.path.exists(file):
                df = pd.read_csv(file, delimiter='\t', index_col=False)
                tweets_ids = df['tweet_id'].tolist()
                common = list(set(susp_tweets_ids).intersection(set(tweets_ids)))

                for i in common:
                    d = df[df['tweet_id'] == i].to_string(header=False, index=False, index_names=False).split()
                    list_ids += "\n"
                    for item in d:
                        list_ids += f"{item}\t"

                sus_file = open(f"/Storage/gnikou/sentiment_per_day/suspended_tweets_day_2020-{month}-{day}.csv", "w+")
                sus_file.write(list_ids)
                sus_file.close()


# write daily average sentiment score of suspended users' tweets
def writef_sentiment_susp():
    list_ids = "day\tpositive_for_covid\tnegative_for_covid\tpositive_for_lockdown\t" \
               "negative_for_lockdown\tpositive_for_vaccine\tnegative_for_vaccine\tpositive_for_conspiracy\t" \
               "negative_for_conspiracy\tpositive_for_masks\tnegative_for_masks\tpositive_for_cases\t" \
               "negative_for_cases\tpositive_for_deaths\tnegative_for_deaths\tpositive_for_propaganda\t" \
               "negative_for_propaganda\tpositive_for_5G\tnegative_for_5G"

    for month in range(2, 8):
        for day in range(1, 32):
            file = f"/Storage/gnikou/sentiment_per_day/suspended_tweets_day_2020-{month}-{day}.csv"  #
            if os.path.exists(file):
                df = pd.read_csv(file, delimiter='\t', index_col=False)
                mean_score = df.mean(axis=0)
                scores = [i for i in mean_score]
                scores[0] = parser.parse(f"2020-{month}-{day}")
                scores[0] = f"{scores[0].year}-{scores[0].month}-{scores[0].day}"
                list_ids += "\n"
                for item in scores:
                    list_ids += f"{item}\t"

    file = open(f"/Storage/gnikou/sentiment_per_day/suspended_twitter_covid_sentiment.csv", "w+")  #
    file.write(list_ids)
    file.close()


# write to file daily volume of suspended tweets/users
def daily_tweets_volume(db):
    list_date = "day\tcount"

    for month in range(2, 8):
        for day in range(1, 32):
            file = f"/Storage/gnikou/sentiment_per_day/suspended_tweets_day_2020-{month}-{day}.csv"
            user_ids = set()
            if os.path.exists(file):
                date = parser.parse(f"2020-{month}-{day}")

                df = pd.read_csv(file, delimiter='\t', index_col=False)
                tweets_ids = df['tweet_id'].tolist()
                for tweet_id in tweets_ids:
                    for tweet in db.tweets.find(
                            {"created_at": {"$gte": date, "$lt": date + timedelta(days=1)}, "id": tweet_id},
                            {"user_id": 1, "_id": 0}):
                        if tweet['user_id'] not in user_ids:
                            user_ids.add(tweet['user_id'])

                date = parser.parse(f"2020-{month}-{day}")
                date = f"{date.year}-{date.month}-{date.day}"
                list_date += f"\n{date}\t{len(df.index)}\t{len(user_ids)}"

    file = open(f"/Storage/gnikou/sentiment_per_day/daily_suspended_volume.csv", "w+")
    file.write(list_date)
    file.close()


'''
def sentiment_of_retweets_from_nonsuspended_users(db):
    for month in range(2, 8):
        for day in range(1, 32):
            file = f"suspended_tweets_day_2020-{month}-{day}.csv"
            file2 = f"sentiment_by_id_day_2020-{month}-{day}.csv"
            if os.path.exists(file):
                date = parser.parse(f"2020-{month}-{day}")
                df = pd.read_csv(file, delimiter='\t', index_col=False)
                susp_tweets_ids = set(df['tweet_id'].tolist())
                df2 = pd.read_csv(file2, delimiter='\t', index_col=False)
                susp_tweets_ids2 = set(df2['tweet_id'].tolist())
                cm = list(set(susp_tweets_ids2).intersection(set(susp_tweets_ids)))
'''


def main():
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]

    writef_suspended_users()
    writef_tweetids_from_suspended_users(db)
    write_daily_sentiment_susp_users()
    writef_sentiment_susp()
    daily_tweets_volume(db)


if __name__ == '__main__':
    main()
