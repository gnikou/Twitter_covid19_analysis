import pymongo
from dateutil import parser
from datetime import timedelta
import pandas as pd
import sys
from pathlib import Path
import collections


def tweets_users_count(db, start_date, end_date):
    user_ids = set()
    tweets_count = 0
    for tweet in db.tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}}):
        tweets_count += 1
        if tweet['user']['id'] not in user_ids:
            user_ids.add(tweet['user']['id'])
    return tweets_count, len(user_ids)


def get_hashtags(db, start_date, end_date):
    hashtags = collections.defaultdict(lambda: 0)

    for tweet in db.tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}}):
        if "extended_tweet" in tweet:
            for ht in tweet['entities']['hashtags']:
                hashtags[ht['text']] += 1

        elif "entities" in tweet:
            for ht in tweet['entities']['hashtags']:
                hashtags[ht['text']] += 1

        if "retweeted_status" in tweet:
            if "extended_tweet" in tweet["retweeted_status"]:
                if tweet["retweeted_status"]["extended_tweet"]['entities']['hashtags'] in tweet:
                    for ht in tweet["retweeted_status"]["extended_tweet"]['entities']['hashtags']:
                        hashtags[ht['text']] += 1

            elif 'entities' in tweet["retweeted_status"]:
                for ht in tweet['retweeted_status']['entities']['hashtags']:
                    hashtags[ht['text']] += 1

    return hashtags


def writef_tweets_users(cur_date, tweets_count, users_count):
    file = Path("/home/gnikou/tweets_users_count_pd.csv")
    date = "{}-{}-{}".format(cur_date.year, cur_date.month, cur_date.day)

    if file.is_file():
        file_out = open("tweets_users_count_pd.csv", "a+")
        file_out.write("\n{}\t{}\t{}".format(date, tweets_count, users_count))

    else:
        file_out = open("tweets_users_count_pd.csv", "w+")
        header = "day\ttweets\tunique users"
        file_out.write("{}\n{}\t{}\t{}".format(header, date, tweets_count, users_count))
    file_out.close()


def writef_hashtags_count(cur_date, hashtags):
    date = "{}-{}-{}".format(cur_date.year, cur_date.month, cur_date.day)
    file = open(f"hashtags_count_day_{date}.csv", "w+")

    list_hts = ""
    list_hts += "Hashtag\tcount"

    for ht in hashtags:
        list_hts += f"\n{ht}\t{hashtags[ht]}"
    file.write(list_hts)
    file.close()


def set_start_date():
    fname = "tweets_users_count_pd.csv"
    try:
        df = pd.read_csv(fname, sep='\t')
    except OSError:
        return parser.parse("2020-02-18")

    # returns last written day
    return parser.parse(df["day"].tail(1).item())


def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['covidTweetsDB']

    start_date = set_start_date() + timedelta(days=1)
    cur_date = start_date
    num_of_days = int(sys.argv[1]) - 1
    end_date = start_date + timedelta(days=num_of_days)

    while cur_date <= end_date:
        tweets_count, users_count = tweets_users_count(db, cur_date, cur_date + timedelta(days=1))
        writef_tweets_users(cur_date, tweets_count, users_count)
        hashtags = get_hashtags(db, cur_date, cur_date + timedelta(days=1))
        if hashtags:
            writef_hashtags_count(cur_date, hashtags)
        cur_date += timedelta(days=1)

    client.close()


if __name__ == '__main__':
    main()
