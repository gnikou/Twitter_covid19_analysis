from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from extra import *

from collections import defaultdict
import pymongo
from dateutil import parser
from datetime import timedelta

tokenizer = AutoTokenizer.from_pretrained("vicgalle/xlm-roberta-large-xnli-anli")
model = AutoModelForSequenceClassification.from_pretrained("vicgalle/xlm-roberta-large-xnli-anli")
classifier = pipeline("zero-shot-classification",
                      model="vicgalle/xlm-roberta-large-xnli-anli", device=0)


def helper(db, start_date, end_date):
    ids = set()
    tweet_texts = defaultdict(lambda: "")
    tweet_multi = defaultdict(lambda: 1)
    tweet_text_list = []

    for tweet in db.Tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}, "retweeted_status": {"$exists": False}}):

        tweet_text = remove_rt(remove_url(get_text(tweet)))
        if tweet_text is None or len(
                tweet_text) < 3 or " " not in tweet_text or "account is temporarily unavailable because it violates the Twitter Media Policy. Learn more." in tweet_text or "account has been withheld in " in tweet_text:
            continue
        ids.add(tweet["id"])
        tweet_texts[tweet["id"]] = tweet_text

    for tweet in db.Tweets.find(
            {"created_at": {"$gte": start_date, "$lt": end_date}, "retweeted_status": {"$exists": True}}):
        rt_text = tweet_text
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
        elif "account is temporarily unavailable because it violates the Twitter Media Policy. Learn more." not in rt_text and "account has been withheld in " not in rt_text:
            """case when we have the original tweet but text is different, need to store both for further analysis"""
            print("{}\n{}\n{}\n----------------------".format(
                tweet_texts[tweet["retweeted_status"]["id"]],
                tweet["retweeted_status"]["id"], rt_text))

    ids_list = []
    all_tweets = 0
    for tw_id in ids:
        tweet_text_list.append(tweet_texts[tw_id])
        ids_list.append(tw_id)
        all_tweets += tweet_multi[tw_id]
    print(f"Number of all tweets: {all_tweets}")
    sent_classifier(tweet_text_list, tweet_multi, all_tweets, ids_list, start_date)


def sent_classifier(tweet_text_list, tweet_multi, all_tweets, ids_list, start_date):
    sentiment_labels = ['positive for COVID-19', 'negative for COVID-19',
                        'positive for covid19', 'negative for covid19']
    sentiment = classifier(tweet_text_list, sentiment_labels, batch_size=16, gradient_accumulation_steps=4,
                           gradient_checkpointing=True, fp16=True, optim="adafactor")

    sent_scores = defaultdict(lambda: 0.0)

    for item in range(len(sentiment)):
        for ind in range(len(sentiment[item]['labels'])):
            label = sentiment[item]['labels'][ind].replace(" ", "_")
            sent_scores[label] += (sentiment[item]['scores'][ind] * tweet_multi[ids_list[item]])

    file_out = open("covid_sentiment_day_{}.csv".format(start_date).replace(" ", "_"), "w+")
    header = "day"
    data = "{}-{}-{}".format(start_date.year, start_date.month, start_date.day)
    for label in sentiment_labels:
        label = label.replace(" ", "_")
        header += "\t" + label
        data += "\t{}".format((sent_scores[label] / all_tweets) * 100 if all_tweets != 0 else 0.0)

    file_out.write("{}\n{}\n".format(header, data))
    file_out.close()


def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['covidTweetsDB']
    collection = db['tweets']
    start_date = parser.parse("2020-02-19")
    cur_date = start_date
    end_date = parser.parse("2020-02-22")

    while cur_date <= end_date:
        helper(db, collection, cur_date, cur_date + timedelta(days=1))
        cur_date += timedelta(days=1)
    # helper(db, collection, start_date, end_date)


if __name__ == '__main__':
    main()
