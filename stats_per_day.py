import pymongo
from dateutil import parser
from datetime import timedelta
import pandas as pd
from pathlib import Path
import collections
from mongoConfig import mongoConfig
import os
import csv


def tweets_users_count(db, start_date, end_date):
    user_ids = set()
    tweets_count = 0
    for tweet in db.tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}}):
        tweets_count += 1
        if tweet['user_id'] not in user_ids:
            user_ids.add(tweet['user_id'])
    return tweets_count, len(user_ids)


def get_hashtags(db, start_date, end_date):
    hashtags = collections.defaultdict(lambda: 0)

    for tweet in db.tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}}):
        for ht in tweet["hashtags"]:
            hashtags[ht] += 1
    return hashtags


def writef_tweets_users(cur_date, tweets_count, users_count):
    file = Path("/home/gnikou/tweets_users_count_pd.csv")
    date = "{}-{}-{}".format(cur_date.year, cur_date.month, cur_date.day)

    if file.is_file():
        file_out = open("tweets_users_count_pd.csv", "a+")
        file_out.write("\n{}\t{}\t{}".format(date, tweets_count, users_count))

    else:
        file_out = open("tweets_users_count_pd.csv", "w+")
        header = "day\ttweets\tusers"
        file_out.write("{}\n{}\t{}\t{}".format(header, date, tweets_count, users_count))
    file_out.close()


def writef_hashtags_count(cur_date, hashtags):
    date = "{}-{}-{}".format(cur_date.year, cur_date.month, cur_date.day)
    file = open(f"hashtags_count_day_{date}.csv", "w", encoding="utf-8")
    list_hts = ""
    list_hts += "Hashtag\tcount"

    for ht in hashtags:
        list_hts += f"\n{ht}\t{hashtags[ht]}"
    file.write(list_hts)
    file.close()


def write_hashtags_by_month():
    for month in range(2, 8):
        hashtags = collections.defaultdict(int)
        for day in range(1, 32):
            file = f"hashtags_count_day_2020-{month}-{day}.csv"
            if os.path.exists(file):
                with open(file, encoding='utf-8') as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter='\t')
                    next(csv_reader)
                    for row in csv_reader:
                        hashtags[row[0]] += int(row[1])
        file = open(f"hashtags_month_{month}.csv", "w", encoding="utf-8")
        list_hts = ""
        list_hts += "Hashtag\tcount"

        for ht in hashtags:
            list_hts += f"\n{ht}\t{hashtags[ht]}"
        file.write(list_hts)
        file.close()


def get_languages(db):
    languages = collections.defaultdict(lambda: 0)
    for i in db.tweets.find({}, {"lang": 1, "_id": 0}):
        for lang in i["lang"]:
            languages[lang] += 1

    file = open("languages_count.csv", "w", encoding="utf-8")
    list_lang = ""
    list_lang += "Language\tcount"

    for lang in languages:
        list_lang += f"\n{lang}\t{languages[lang]}"
    file.write(list_lang)
    file.close()


def set_start_date(db):
    fname = "tweets_users_count_pd.csv"
    try:
        df = pd.read_csv(fname, sep='\t')
    except OSError:
        return [parser.parse(i['created_at'].strftime('%Y-%m-%d')) for i in
                db.tweets.find({}, {"created_at": 1, "_id": 0}).sort('created_at', 1).limit(1)][0]

    # returns last written day
    return parser.parse(df["day"].tail(1).item()) + timedelta(days=1)


def main():
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]

    start_date = set_start_date(db)
    cur_date = start_date
    end_date = [parser.parse(i['created_at'].strftime('%Y-%m-%d')) for i in
                db.tweets.find({}, {"created_at": 1, "_id": 0}).sort('created_at', -1).limit(1)][0]

    while cur_date <= end_date:
        tweets_count, users_count = tweets_users_count(db, cur_date, cur_date + timedelta(days=1))
        writef_tweets_users(cur_date, tweets_count, users_count)
        hashtags = get_hashtags(db, cur_date, cur_date + timedelta(days=1))
        if hashtags:
            writef_hashtags_count(cur_date, hashtags)
        cur_date += timedelta(days=1)

    write_hashtags_by_month()
    get_languages(db)
    client.close()


if __name__ == '__main__':
    main()
