from collections import defaultdict
import pymongo
from mongoConfig import mongoConfig
import pandas as pd
from datetime import timedelta
from dateutil import parser
import os


def maxlabel_user_to_user(db, date, label):
    start_date = parser.parse(date)
    end_date = start_date + timedelta(days=1)

    all_tweet_ids = check_argmax_label(label, start_date)
    retweet_dict = defaultdict(lambda: 0)

    file_susp = "id_suspended_users.csv"
    df_suspended = pd.read_csv(file_susp, header=None)
    susp_user_ids = set(int(i) for i in df_suspended[0].unique())

    # file_deactivated = "id_deactivated.csv"
    # df_deactivated = pd.read_csv(file_deactivated, header=None)
    # deactivated_user_ids = set(int(i) for i in df_deactivated[0].unique())

    file_deleted = "id_deleted.csv"
    df_deleted = pd.read_csv(file_deleted, header=None)
    deleted_user_ids = set(int(i) for i in df_deleted[0].unique())

    file_protected = "id_protected.csv"
    df_protected = pd.read_csv(file_protected, header=None)
    protected_user_ids = set(int(i) for i in df_protected[0].unique())

    susp_users = set()
    # deactivated_users = set()
    deleted_users = set()
    protected_users = set()
    nonsusp_users = set()

    text_list = "digraph{"
    for tweet_id in all_tweet_ids:
        for rt_tweet in db.tweets.find({"created_at": {"$gte": start_date, "$lt": end_date}, "id": tweet_id,
                                        "retweeted_status": {"$exists": True}}):
            if rt_tweet['user_id'] in susp_user_ids:
                susp_users.add(rt_tweet['user_id'])
            # elif rt_tweet['user_id'] in deactivated_user_ids:
            #     deactivated_users.add(rt_tweet['user_id'])
            elif rt_tweet['user_id'] in deleted_user_ids:
                deleted_users.add(rt_tweet['user_id'])
            elif rt_tweet['user_id'] in protected_user_ids:
                protected_users.add(rt_tweet['user_id'])
            else:
                nonsusp_users.add(rt_tweet['user_id'])

            for original_tweet in db.tweets.find({"id": rt_tweet['retweeted_status']['id']}, {"user_id": 1, "_id": 0}):
                if original_tweet['user_id'] in susp_user_ids:
                    susp_users.add(original_tweet['user_id'])
                # elif original_tweet['user_id'] in deactivated_user_ids:
                #     deactivated_users.add(original_tweet['user_id'])
                elif original_tweet['user_id'] in deleted_user_ids:
                    deleted_users.add(original_tweet['user_id'])
                elif original_tweet['user_id'] in protected_user_ids:
                    protected_users.add(original_tweet['user_id'])
                else:
                    nonsusp_users.add(original_tweet['user_id'])

                retweet_dict[(rt_tweet['user_id'], original_tweet['user_id'])] += 1

    for user in susp_users:
        text_list += f"\n{user} [group = 'suspended']"
    # for user in deactivated_users:
    #     text_list += f"\n{user} [group = 'deactivated']"
    for user in deleted_users:
        text_list += f"\n{user} [group = 'deleted']"
    for user in protected_users:
        text_list += f"\n{user} [group = 'protected']"
    for user in nonsusp_users:
        text_list += f"\n{user} [group = 'non_suspended']"

    print(len(retweet_dict))
    for key, value in retweet_dict.items():
        text_list += f"\n{key[0]} -> {key[1]} [weight={value}]"

    text_list += "\n}"
    file_susp = open(f"/home/gnikou/dotgraphs2/graph-{label}-2020-{start_date.month}-{start_date.day}.dot", "w+")
    file_susp.write(text_list)
    file_susp.close()


def check_argmax_label(label, start_date):
    file_sentiment = f"/home/gnikou/sentiment_per_day/sentiment_by_id_day_2020-{start_date.month}-{start_date.day}.csv"
    file_suspended = f"/home/gnikou/suspended_texts/suspended_texts-{label}-2020-{start_date.month}-{start_date.day}.csv"
    # file_deactivated = f"id_deactivated.csv"
    # file_deleted = f"id_deleted.csv"
    # file_protected = f"id_deleted.csv"
    # print(file_sentiment)
    # print(file_suspended)

    if os.path.exists(file_sentiment) is False:
        print(f"File {file_sentiment} isn't here ")
        return

    df = pd.read_csv(file_sentiment, sep='\t', index_col=False)
    label = label.replace("_", " ")
    if label.split()[0] == "positive":
        df = df[['tweet_id', 'positive for covid', 'positive for lockdown',
                 'positive for vaccine', 'positive for conspiracy',
                 'positive for masks', 'positive for cases',
                 'positive for deaths', 'positive for propaganda']].copy()
    else:
        df = df[['tweet_id', 'negative for covid', 'negative for lockdown',
                 'negative for vaccine', 'negative for conspiracy',
                 'negative for masks', 'negative for cases',
                 'negative for deaths', 'negative for propaganda']].copy()
    df.set_index("tweet_id", inplace=True, drop=True)
    df = df[df.idxmax(axis="columns") == label]

    all_tweet_ids = [int(i) for i in df.index]

    return all_tweet_ids


def get_outliers(label):
    file = "suspended_twitter_covid_sentiment.csv"
    df = pd.read_csv(file, sep='\t', index_col=False)
    q = df[label].quantile(0.95)
    d = df[df[label] > q]
    print(d)
    return d['day'].values.flatten().tolist()


def main():
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]

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
