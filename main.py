from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import json
import numpy as np
import pandas as pd
import utils
import torch, time, sys, re
from collections import defaultdict
from os import listdir
from os.path import isfile, join
import ast



# tokenizer = AutoTokenizer.from_pretrained("vicgalle/xlm-roberta-large-xnli-anli")
# model = AutoModelForSequenceClassification.from_pretrained("vicgalle/xlm-roberta-large-xnli-anli")


# extract text field of twitter object
def get_text(tweet):
    if "extended_tweet" in tweet.keys() and "full_text" in tweet["extended_tweet"].keys():
        # case of extended tweet object
        return tweet['extended_tweet']['full_text']
    elif "retweeted_status" in tweet.keys() and "extended_tweet" in tweet["retweeted_status"].keys() and "full_text" in \
            tweet["retweeted_status"]["extended_tweet"].keys():
        # case of retweet object with extednded status
        return tweet['retweeted_status']['extended_tweet']['full_text']
    elif "retweeted_status" in tweet.keys() and "full_text" in tweet["retweeted_status"].keys():
        # case of retweetedd object with full_text
        return tweet['retweeted_status']['full_text']
    elif "retweeted_status" in tweet.keys():
        tweet_text = tweet["full_text"] if "full_text" in tweet else tweet["text"]
        tweet_text = utils.merge_tw_rt(tweet_text,
                                       tweet["retweeted_status"]["full_text"] if "full_text" in
                                                                                 tweet["retweeted_status"] else
                                       tweet["retweeted_status"]["text"])
        return tweet_text
    elif "full_text" in tweet.keys():
        # tweet object with full_text
        return tweet['full_text']
    elif "text" in tweet.keys():
        # case of simple text field in tweet object
        return tweet['text']
    return None


def remove_url(text):
    return re.sub(r"http\S+", "", text)


def text_cleanup(tweet_text):
    clean_tweet_text = remove_url(tweet_text)
    clean_tweet_text = re.sub('[@#]', '', clean_tweet_text)
    return clean_tweet_text


def legit_length(text):
    text.replace("  ", " ")
    words = [word for word in text.split(" ") if len(word) >= 1 and word[0] != "@"]
    score = len(words) / len("".join(words)) if len("".join(words)) != 0 else 0.0
    if score > 0.0 and score < 1.0:
        return True
    return False


def remove_rt(text):
    while len(text) > 3 and (text[:3] == "RT " or text[0] == "@"):
        if not legit_length(text):
            return None
        if "RT " == text[:3]:
            if ":" in text:
                text = text[text.index(":") + 2:]
            else:
                text = text[3:]
        if not legit_length(text):
            return None
        # in case when first word is mention, remove it
        if len(text) > 2 and text[0] == "@":
            if " " in text:
                text = text[text.index(" ") + 1:]
            elif "\t" in text:
                text = text[text.index("\t") + 1:]
            elif "\n" in text:
                text = text[text.index("\n") + 1:]
            else:
                return None
    if legit_length(text):
        text = text_cleanup(text)
        if legit_length(text):
            return text

    return None


def helper(tweets):
    ids = set()
    # tweet_texts = defaultdict(lambda: "")
    # tweet_multi = defaultdict(lambda: 1)
    tweetText_list = []
    for tweet in tweets:
        tweet_text = remove_rt(remove_url(get_text(tweet)))
        print(tweet_text)
        if tweet_text is None or len(
                tweet_text) < 3 or " " not in tweet_text or "account is temporarily unavailable because it violates the Twitter Media Policy. Learn more." in tweet_text or "account has been withheld in " in tweet_text:
            continue
        ids.add(tweet["id"])
        tweet_texts[tweet["id"]] = tweet_text
        print(tweet_text)
    #
    # for tweet in tweets:
    #     for text in tweet.find({"retweeted_status": {"$exists": True}}):
    #
    #         rt_text = remove_rt(remove_url(get_text(text)))
    #         if rt_text is None or len(rt_text) < 3 or " " not in rt_text:
    #             continue
    #         seq_size = len(rt_text[:-5])
    #
    #         """check if text sequences is same with the original text"""
    #         if (tweet["retweeted_status"]["id"] in ids) and rt_text[:-5] == tweet_texts[
    #                                                                             tweet["retweeted_status"]["id"]][
    #                                                                         :seq_size]:
    #             """if text is same , we increase multiplier index"""
    #             tweet_multi[tweet["retweeted_status"]["id"]] += 1
    #         elif (tweet["retweeted_status"]["id"] not in ids):
    #             """case when we dont have the original tweet text, so we keep id and text for analysis"""
    #             ids.add(tweet["retweeted_status"]["id"])
    #             tweet_texts[tweet["retweeted_status"]["id"]] = rt_text
    #         elif "account is temporarily unavailable because it violates the Twitter Media Policy. Learn more." not in rt_text and "account has been withheld in " not in rt_text:
    #             """case when we have the original tweet but text is different, need to store both for further analysis"""
    #             print("{}\n{}\n{}\n----------------------".format(
    #                 tweet_texts[tweet["retweeted_status"]["id"]],
    #                 tweet["retweeted_status"]["id"], rt_text))

    ids_list = []
    all_tweets = 0
    for tw_id in ids:
        tweetText_list.append(tweet_texts[tw_id])
        ids_list.append(tw_id)
        all_tweets += tweet_multi[tw_id]


def main():
    file = 'ht_COVID19_95.json'
    with open(file, 'r') as inp:
        tweets = []
        tweets_dict = []
        for line in inp:
            js = json.loads(line)
            tweets_dict.append(js)
            tweets.append(line)
    helper(tweets_dict)


if __name__ == '__main__':
    main()
