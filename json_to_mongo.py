import pymongo
import json
from dateutil import parser
import gzip
import shutil
import os
from extra import *
from datetime import datetime, timedelta
from mongoConfig import mongoConfig
from collections import defaultdict

DATA_PATH = "data/"

def int_from_date(datetime_obj):
    return  (datetime_obj.year * 10000) + (datetime_obj.month * 100) + datetime_obj.day


class JsonParser:
    def __init__(self):
        self.client = pymongo.MongoClient(mongoConfig["address"])
        self.db = self.client[mongoConfig["db"]]
        self.collection = self.db[mongoConfig["collection"]]
        self.ids_by_day = defaultdict(lambda: set([]))
        self.tweet_ids = set()
    
    """Get tweet IDS that are already is in MongoDB"""
    def get_known_tweetIDS(self):
        #get last inserted tweet by date
        for item in self.collection.find().sort("created_at",pymongo.DESCENDING):
            last_date = item["created_at"]
            break
        first_date = last_date - timedelta(days=10)
        for item in self.collection.find({"created_at": {"$gte": first_date, "$lte": last_date }}, {"_id": 0, "id": 1, "created_at": 1}):
            self.tweet_ids.add(item["id"])
            self.ids_by_day[int_from_date(item["created_at"])].add(item["id"])

        #self.tweet_ids = set([item["id"] for item in self.collection.find({}, {"_id": 0, "id": 1})])


    def get_files_range(self):
        nmbr_range = []
        for file in os.listdir(DATA_PATH):
            if file.startswith("ht_"):
                nmbr_range.append(int(file.split("_")[2].split(".")[0]))
        if len(nmbr_range) == 0:
            return 0, 0
        return max(nmbr_range), min(nmbr_range)

    def get_hashtags(self, tweet):
        hashtags = set()
        if "extended_tweet" in tweet:
            for ht in tweet['entities']['hashtags']:
                hashtags.add(ht['text'])

        elif "entities" in tweet:
            for ht in tweet['entities']['hashtags']:
                hashtags.add(ht['text'])

        if "retweeted_status" in tweet:
            if "extended_tweet" in tweet["retweeted_status"]:
                if tweet["retweeted_status"]["extended_tweet"]['entities']['hashtags'] in tweet:
                    for ht in tweet["retweeted_status"]["extended_tweet"]['entities']['hashtags']:
                        hashtags.add(ht['text'])

            elif 'entities' in tweet["retweeted_status"]:
                for ht in tweet['retweeted_status']['entities']['hashtags']:
                    hashtags.add(ht['text'])
        return list(hashtags)


    def file_parse(self, filename):
        """Open compresed file and read line-by-line"""
        print(f'Parsing file: {filename}')
        if not os.path.exists(DATA_PATH + filename):
            print(f'\tSkip, file not exists')
            return 0
        fstream = gzip.open(DATA_PATH + filename, 'r')
        tweets_to_insert = []
        counter = 0
        while True:
            line = fstream.readline()
            if not line:
                break

            try:
                tweet = json.loads(line)
            except ValueError:
                print('Decoding JSON has failed - value error')
                continue
            except TypeError:
                print('Decoding JSON has failed - type error')
                continue

            """Check if tweet is not already in database"""
            if int(tweet["id"]) in self.tweet_ids:
                continue
            text = get_text(tweet)
            text = text_cleanup(remove_url(tweet, text)).strip()
            #text = text_cleanup(get_text(tweet)).strip()

            if (text is None or len(text) < 3 or
                ("account is temporarily unavailable because it violates the Twitter Media Policy. Learn more." in text)
                    or "account has been withheld in " in text):
                continue
            
            tweet["created_at"] = parser.parse(tweet["created_at"]) if type(tweet["created_at"]) == str else tweet["created_at"]
            new_tweet = {"id": int(tweet["id"]),
                         "text": text,
                         "user_id": int(tweet["user"]["id"]),
                         "created_at": tweet["created_at"],
                         "hashtags": self.get_hashtags(tweet), 
                         "lang": tweet["lang"]}
            if"retweeted_status" in tweet:
                new_tweet["retweeted_status"] = {"id": tweet["retweeted_status"]["id"] }
            
            tweets_to_insert.append(new_tweet)

            counter += 1

            self.tweet_ids.add(int(tweet["id"]))
            self.ids_by_day[int_from_date(tweet["created_at"])].add(int(tweet["id"]))

            """If they are to many tweets ready to insert, push them into database in order to reduce memory usage"""
            if len(tweets_to_insert) % 50000 == 0:
                self.collection.insert_many(tweets_to_insert)
                tweets_to_insert = []
        if len(tweets_to_insert) > 0:
            self.collection.insert_many(tweets_to_insert)

        print(f'\t Parsing done with {counter} tweets inserted into mongoDB')
        return counter

    def remove_old_ids(self):
        dates = list(self.ids_by_day.keys())
        dates.sort()
        if len(dates) > 10:
            for i in range(0, len(dates) -10):
                self.tweet_ids -= self.ids_by_day[dates[i]]
                self.ids_by_day.pop(dates[i])


    def main(self):
        self.get_known_tweetIDS()
        print(f"Number of initial tweets (DB): {len(self.tweet_ids)}\n")
        end_point, start_point = self.get_files_range()

        if end_point == 0 or end_point < start_point:
            return

        for fileIndex in range(start_point, end_point+1):
            for prefix in ['ht_coronavirus_', 'ht_COVID19_']:
                _ = self.file_parse(prefix + f'{fileIndex}.gz')
                self.remove_old_ids()

        #print('=======================================')
        #print(f"Total Number of tweets (DB): {len(self.tweet_ids)}")
        self.client.close()


if __name__ == '__main__':
    jp = JsonParser()
    jp.main()
