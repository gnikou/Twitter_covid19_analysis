from collections import defaultdict
import csv
import pymongo
from mongoConfig import mongoConfig
import pandas as pd
from datetime import timedelta
from dateutil import parser
import os


def three_tweets_rt(db):
    file = "id_suspended_users.csv"  # /Storage/gnikou/sentiment_per_day/
    df = pd.read_csv(file, header=None)
    susp_user_ids = set(int(i) for i in df[0].unique())
    tweets = [1264223201955102727, 1266396421923667968, 1267163158772408320]
    non_suspended = dict()
    suspended = dict()

    for i in tweets:
        for tweet in db.tweets.find({"retweeted_status.id": i}):
            if tweet["user_id"] not in susp_user_ids:
                non_suspended[tweet["user_id"]] = tweet["retweeted_status"]["id"]

        for tweet in db.tweets.find({"retweeted_status.id": i}):
            if tweet["user_id"] in susp_user_ids:
                suspended[tweet["user_id"]] = tweet["retweeted_status"]["id"]

    text_list = "digraph{"
    for user_id, rt_id in non_suspended.items():
        text_list += f"\n{user_id} [group = 'non_suspended'][color = blue]"
        text_list += f"\n{user_id} -> {rt_id}"
    for user_id, rt_id in suspended.items():
        text_list += f"\n{user_id} [group = 'suspended'][color = red]"
        text_list += f"\n{user_id} -> {rt_id}"

    text_list += "\n}"
    file = open(f"graph1.dot", "w+")
    file.write(text_list)
    file.close()


def all_suspended_peak_days(db):
    file = "id_suspended_users.csv"
    df = pd.read_csv(file, header=None)
    susp_user_ids = set(int(i) for i in df[0].unique())
    start_date = parser.parse("29-05-2020")
    end_date = start_date + timedelta(days=1)

    text_list = "digraph{"
    for user in susp_user_ids:
        for tweet in db.tweets.find(
                {"created_at": {"$gte": start_date, "$lt": end_date}, "retweeted_status": {"$exists": True},
                 "user_id": user}):
            text_list += f"\n{tweet['user_id']} -> {tweet['retweeted_status']['id']}"

    text_list += "\n}"
    file = open(f"graph-2020-{start_date.month}-{start_date.day}.dot", "w+")
    file.write(text_list)
    file.close()


def maxlabel_only(db):
    file = f"/home/gnikou/susp_texts/suspended_texts-positive_for_covid-2020-5-29.csv"
    df = pd.read_csv(file, sep='\t')
    tweet_ids = [int(i) for i in df.id]

    start_date = parser.parse("29-05-2020")
    end_date = start_date + timedelta(days=1)

    text_list = "digraph{"
    for tweet_id in tweet_ids:
        for tweet in db.tweets.find({"created_at": {"$gte": start_date, "$lt": end_date}, "id": tweet_id,
                                     "retweeted_status": {"$exists": True}}):
            text_list += f"\n{tweet['user_id']} -> {tweet['retweeted_status']['id']}"

    text_list += "\n}"
    file = open(f"graph2-2020-{start_date.month}-{start_date.day}.dot", "w+")
    file.write(text_list)
    file.close()


def maxlabel_user_to_user(db, date, label):
    start_date = parser.parse(date)
    end_date = start_date + timedelta(days=1)

    all_tweet_ids, suspended_tweet_ids, non_suspended_tweet_ids = separate_susp_nonsusp(label, start_date)
    retweet_dict = defaultdict(lambda: 0)

    file = "id_suspended_users.csv"
    df = pd.read_csv(file, header=None)
    susp_user_ids = set(int(i) for i in df[0].unique())

    susp_users = set()
    nonsusp_users = set()
    text_list = "digraph{"
    for tweet_id in suspended_tweet_ids:
        for rt_tweet in db.tweets.find({"created_at": {"$gte": start_date, "$lt": end_date}, "id": tweet_id,
                                        "retweeted_status": {"$exists": True}}):
            susp_users.add(rt_tweet['user_id'])
            for original_tweet in db.tweets.find({"id": rt_tweet['retweeted_status']['id']}, {"user_id": 1, "_id": 0}):
                if original_tweet['user_id'] in susp_user_ids:
                    susp_users.add(original_tweet['user_id'])
                else:
                    nonsusp_users.add(original_tweet['user_id'])
                retweet_dict[(rt_tweet['user_id'], original_tweet['user_id'])] += 1

    for tweet_id in non_suspended_tweet_ids:
        for rt_tweet in db.tweets.find({"created_at": {"$gte": start_date, "$lt": end_date}, "id": tweet_id,
                                        "retweeted_status": {"$exists": True}}):
            nonsusp_users.add(rt_tweet['user_id'])
            for original_tweet in db.tweets.find({"id": rt_tweet['retweeted_status']['id']}, {"user_id": 1, "_id": 0}):
                if original_tweet['user_id'] in susp_user_ids:
                    susp_users.add(original_tweet['user_id'])
                else:
                    nonsusp_users.add(original_tweet['user_id'])
                retweet_dict[(rt_tweet['user_id'], original_tweet['user_id'])] += 1

    for user in susp_users:
        text_list += f"\n{user} [group = 'suspended']"
    for user in nonsusp_users:
        text_list += f"\n{user} [group = 'non-suspended']"

    print(len(retweet_dict))
    for key, value in retweet_dict.items():
        text_list += f"\n{key[0]} -> {key[1]} [weight={value}]"

    text_list += "\n}"
    file_susp = open(f"graph-{label}-2020-{start_date.month}-{start_date.day}.dot", "w+")
    file_susp.write(text_list)
    file_susp.close()


def separate_susp_nonsusp(label, start_date):
    file_sentiment = f"/home/gnikou/sentiment_by_id/sentiment_by_id_day_2020-{start_date.month}-{start_date.day}.csv"
    file_suspended = f"/home/gnikou/susp_texts/suspended_texts-{label}-2020-{start_date.month}-{start_date.day}.csv"
    print(file_sentiment)
    print(file_suspended)

    if os.path.exists(file_sentiment) is False:
        print(f"File {file_sentiment} isn't here ")
        return

    df = pd.read_csv(file_sentiment, sep='\t', index_col=False)
    label = label.replace("_", " ")
    if label.split()[0] == "positive":
        df = df[['tweet_id', 'positive for covid', 'positive for lockdown',
                 'positive for vaccine', 'positive for conspiracy',
                 'positive for masks', 'positive for cases',
                 'positive for deaths', 'positive for propaganda',
                 'positive for 5G']].copy()
    else:
        df = df[['tweet_id', 'negative for covid', 'negative for lockdown',
                 'negative for vaccine', 'negative for conspiracy',
                 'negative for masks', 'negative for cases',
                 'negative for deaths', 'negative for propaganda',
                 'negative for 5G']].copy()
    df.set_index("tweet_id", inplace=True, drop=True)
    df = df[df.idxmax(axis="columns") == label]

    all_tweet_ids = [int(i) for i in df.index]
    df2 = pd.read_csv(file_suspended, sep='\t', quoting=csv.QUOTE_NONE, error_bad_lines=False)
    suspended_tweet_ids = [int(i) for i in df2.id]

    non_suspended_tweet_ids = set(all_tweet_ids) - set(suspended_tweet_ids)
    print(f"Non susp: {len(non_suspended_tweet_ids)}")
    print(f"Susp: {len(suspended_tweet_ids)}")
    return all_tweet_ids, suspended_tweet_ids, non_suspended_tweet_ids


def get_outliers(label):
    file = "suspended_twitter_covid_sentiment.csv"
    df = pd.read_csv(file, sep='\t', index_col=False)
    q = df[label].quantile(0.97)
    d = df[df[label] > q]
    print(d)
    return d['day'].values.flatten().tolist()


def main():
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]
    # three_tweets_rt(db)
    # all_suspended_peak_days(db)
    # maxlabel_only(db)

    labels_list = ['positive_for_covid', 'positive_for_lockdown', 'positive_for_vaccine', 'positive_for_conspiracy',
                   'positive_for_masks', 'positive_for_cases', 'positive_for_deaths', 'positive_for_propaganda',
                   'positive_for_5G', 'negative_for_covid', 'negative_for_lockdown', 'negative_for_vaccine',
                   'negative_for_conspiracy', 'negative_for_masks', 'negative_for_cases', 'negative_for_deaths',
                   'negative_for_propaganda', 'negative_for_5G']

    for label in labels_list:
        print(label)
        days = get_outliers(label)
        for date in days:
            maxlabel_user_to_user(db, date, label)


if __name__ == '__main__':
    main()
