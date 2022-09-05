from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from extra import *

import collections
import pymongo
from dateutil import parser
from datetime import timedelta
from pathlib import Path
import pandas as pd
import sys
import time

tokenizer = AutoTokenizer.from_pretrained("vicgalle/xlm-roberta-large-xnli-anli")
model = AutoModelForSequenceClassification.from_pretrained("vicgalle/xlm-roberta-large-xnli-anli")
classifier = pipeline("zero-shot-classification", model=model, tokenizer=tokenizer, device=0)


def helper(db, start_date, end_date):
    ids = set()
    tweet_texts = collections.defaultdict(lambda: "")
    tweet_multi = collections.defaultdict(lambda: 1)
    tweet_text_list = []

    for tweet in db.tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}, "retweeted_status": {"$exists": False}}):
        tweet_text = remove_rt(remove_url(get_text(tweet)))
        if tweet_text is None or len(
                tweet_text) < 3 or " " not in tweet_text or "account is temporarily unavailable because it violates the Twitter Media Policy. Learn more." in tweet_text or "account has been withheld in " in tweet_text:
            continue
        ids.add(tweet["id"])
        tweet_texts[tweet["id"]] = tweet_text

    for tweet in db.tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}, "retweeted_status": {"$exists": True}}):
        rt_text = remove_rt(remove_url(get_text(tweet)))
        if rt_text is None or len(rt_text) < 3 or " " not in rt_text:
            continue
        seq_size = len(rt_text[:-5])

        """check if text sequences is same with the original text"""
        if (tweet["retweeted_status"]["id"] in ids) and rt_text[:-5] == tweet_texts[
                                                                            tweet["retweeted_status"]["id"]][
                                                                        :seq_size]:
            """if text is same , we increase multiplier index"""
            tweet_multi[tweet["retweeted_status"]["id"]] += 1
        elif tweet["retweeted_status"]["id"] not in ids:
            """case when we dont have the original tweet text, so we keep id and text for analysis"""
            ids.add(tweet["retweeted_status"]["id"])
            tweet_texts[tweet["retweeted_status"]["id"]] = rt_text

    ids_list = []
    all_tweets = 0
    for tw_id in ids:
        tweet_text_list.append(replace_labels(tweet_texts[tw_id]))
        ids_list.append(tw_id)
        all_tweets += tweet_multi[tw_id]

    print(f"\nDay: {start_date} Number of tweets: {all_tweets}")
    sent_classifier(tweet_text_list, tweet_multi, all_tweets, ids_list, start_date)


def sent_classifier(tweet_text_list, tweet_multi, all_tweets, ids_list, start_date):
    sentiment_labels = ['positive for covid', 'negative for covid',
                        'positive for lockdown', 'negative for lockdown',
                        'positive for vaccine', 'negative for vaccine',
                        'positive for conspiracy', 'negative for conspiracy',
                        'positive for masks', 'negative for masks',
                        'positive for cases', 'negative for cases',
                        'positive for deaths', 'negative for deaths',
                        'positive for propaganda', 'negative for propaganda',
                        'positive for 5G', 'negative for 5G',
                        ]
    sentiment = classifier(tweet_text_list, sentiment_labels, batch_size=16, gradient_accumulation_steps=4,
                           gradient_checkpointing=True, fp16=True, optim="adafactor")

    sent_scores = collections.defaultdict(lambda: 0.0)

    for item in range(len(sentiment)):
        for ind in range(len(sentiment[item]['labels'])):
            label = sentiment[item]['labels'][ind].replace(" ", "_")
            sent_scores[label] += (sentiment[item]['scores'][ind] * tweet_multi[ids_list[item]])

    data = "{}-{}-{}".format(start_date.year, start_date.month, start_date.day)

    writef_id_sentiment(data, ids_list, sentiment, sentiment_labels)
    writef_sentiment(all_tweets, data, sentiment_labels, sent_scores)


def replace_labels(original_text):
    text = original_text.lower()
    text = text.replace("covid19", "covid")
    text = text.replace("coronavirus", "covid")
    text = text.replace("pandemic", "covid")
    text = text.replace("covid-19", "covid")
    text = text.replace("isolation", "lockdown")
    text = text.replace("vaccination", "vaccine")
    text = text.replace("pfizer", "vaccine")
    text = text.replace("pfizer-biontech", "vaccine")
    text = text.replace("moderna", "vaccine")
    text = text.replace("johnson & johnson", "vaccine")
    text = text.replace("j&j", "vaccine")
    return text


def writef_id_sentiment(data, ids_list, sentiment, sentiment_labels):
    ids_file = open("sentiment_by_id_day_{}.csv".format(data), "w+")
    list_ids = ""
    list_ids += "tweet_id\t" + '\t'.join(sentiment_labels)

    for item in range(len(sentiment)):
        list_ids += f"\n{ids_list[item]}\t"
        for label in sentiment_labels:
            index = sentiment[item]['labels'].index(label)
            list_ids += f"{sentiment[item]['scores'][index]:.3f}\t"
    ids_file.write(list_ids)
    ids_file.close()


def writef_sentiment(all_tweets, data, sentiment_labels, sent_scores):
    my_file = Path("/home/gnikou/twitter_covid_sentiment.csv")

    if my_file.is_file():
        file_out = open("twitter_covid_sentiment.csv", "a+")
        for label in sentiment_labels:
            label = label.replace(" ", "_")
            data += "\t{}".format((sent_scores[label] / all_tweets) if all_tweets != 0 else 0.0)
        file_out.write("{}\n".format(data))

    else:
        file_out = open("twitter_covid_sentiment.csv", "w+")
        header = "day"
        for label in sentiment_labels:
            label = label.replace(" ", "_")
            header += "\t" + label
            data += "\t{}".format((sent_scores[label] / all_tweets) if all_tweets != 0 else 0.0)
        file_out.write("{}\n{}\n".format(header, data))
    file_out.close()


def set_start_date():
    fname = "twitter_covid_sentiment.csv"
    try:
        df = pd.read_csv(fname, sep='\t')
    except OSError:
        return parser.parse("2020-02-18")

    # returns last written day
    return parser.parse(df["day"].tail(1).item())


def main():
    client = pymongo.MongoClient("mongodb://localhost:27027/")
    db = client['covidTweetsDB']

    start_date = set_start_date() + timedelta(days=1)
    cur_date = start_date
    num_of_days = int(sys.argv[1]) - 1
    end_date = start_date + timedelta(days=num_of_days)

    while cur_date <= end_date:
        start_time = time.time()
        helper(db, cur_date, cur_date + timedelta(days=1))
        elapsed_time = time.time() - start_time
        print(time.strftime("Time elapsed: %H:%M:%S", time.gmtime(elapsed_time)))
        cur_date += timedelta(days=1)

    client.close()


if __name__ == '__main__':
    main()
