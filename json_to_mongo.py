import pymongo
import json
from dateutil import parser
import gzip
import shutil
import os
from extra import *
from mongoConfig import mongoConfig

DATA_PATH = "data/"



class JsonParser:
    def __init__(self):
        self.client = pymongo.MongoClient(mongoConfig["address"])
        self.db = self.client[mongoConfig["db"]]
        self.collection = self.db[mongoConfig["collection"]]

        self.tweet_ids = None

    """Get tweet IDS that are already is in MongoDB"""
    def get_known_tweetIDS(self):
        self.tweet_ids = set([item["id"] for item in self.collection.find({}, {"_id": 0, "id": 1})])


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
            tweet = json.loads(line)

            """Check if tweet is not already in database"""
            if int(tweet["id"]) in self.tweet_ids:
                continue

            text = text_cleanup(get_text(tweet)).strip()

            if (text is None or len(text) < 3 or
                ("account is temporarily unavailable because it violates the Twitter Media Policy. Learn more." in text)
                    or "account has been withheld in " in text):
                continue
            original_text = ""
            if "extended_tweet" in tweet and "full_text" in tweet["extended_tweet"]:
                original_text =  tweet["extended_tweet"]["full_text"]
            elif "full_text" in tweet:
                original_text =  tweet["full_text"]
            else:
                original_text =  tweet["text"]
            new_tweet = {"id": int(tweet["id"]),
                         "text": text,
                         "original_text": original_text,
                         "user_id": int(tweet["user"]["id"]),
                         "created_at": parser.parse(tweet["created_at"]) if type(tweet["created_at"]) == str else tweet["created_at"],
                         "hashtags": self.get_hashtags(tweet)}
            if"retweeted_status" in tweet:
                new_tweet["retweeted_status"] = {"id": tweet["retweeted_status"]["id"], "text": tweet["retweeted_status"]["full_text"] if "full_text" in
                                                                                                   tweet["retweeted_status"] else tweet["retweeted_status"]["text"]}

            tweets_to_insert.append(new_tweet)
            counter += 1

            self.tweet_ids.add(int(tweet["id"]))

            """If they are to many tweets ready to insert, push them into database in order to reduce memory usage"""
            if len(tweets_to_insert) % 50000 == 0:
                self.collection.insert_many(tweets_to_insert)
                tweets_to_insert = []
        if len(tweets_to_insert) > 0:
            self.collection.insert_many(tweets_to_insert)

        print(f'\t Parsing done with {counter} tweets inserted into mongoDB')
        return counter


    def main(self):
        self.get_known_tweetIDS()
        print(f"Number of initial tweets (DB): {len(self.tweet_ids)}\n")
        end_point, start_point = self.get_files_range()

        if end_point == 0 or end_point < start_point:
            return

        for fileIndex in range(start_point, end_point+1):
            for prefix in ['ht_coronavirus_', 'ht_COVID19_']:
                _ = self.file_parse(prefix + f'{fileIndex}.gz')

        print('=======================================')
        print(f"Total Number of tweets (DB): {len(self.tweet_ids)}")
        self.client.close()


if __name__ == '__main__':
    jp = JsonParser()
    jp.main()
