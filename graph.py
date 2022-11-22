import pymongo
from mongoConfig import mongoConfig
import pandas as pd
from datetime import timedelta
from dateutil import parser


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


def main():
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]
    three_tweets_rt(db)
    all_suspended_peak_days(db)
    maxlabel_only(db)


if __name__ == '__main__':
    main()
