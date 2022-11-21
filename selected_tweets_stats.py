import pandas as pd
import pymongo
from mongoConfig import mongoConfig
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta


def retweets_stats(db):
    file = "tweets_from_suspended_users.csv"  # /Storage/gnikou/sentiment_per_day/
    df = pd.read_csv(file, header=None)
    susp_tweets_ids = set(int(i) for i in df[0].unique())
    tweets = [1264223201955102727, 1266396421923667968, 1267163158772408320, 1272545366773096453, 1266427319054413831,
              1272982565088092166, 1272919717007765505]

    for i in tweets:
        total_rts = 0
        suspended_rts = 0
        non_susp_rts = 0
        for tweet in db.tweets.find({"id": i}):
            print(tweet["id"])
        for tweet in db.tweets.find({"retweeted_status.id": i}):
            if tweet["id"] in susp_tweets_ids:
                suspended_rts += 1
            else:
                non_susp_rts += 1

        print(f"Total: {suspended_rts + non_susp_rts}")
        print(f"Suspended: {suspended_rts}")
        print(f"Non Suspended: {non_susp_rts}")
        print(f"Ratio: {suspended_rts / (suspended_rts + non_susp_rts)}\n")


def timedistribution(db):
    file = "tweets_from_suspended_users.csv"  # /Storage/gnikou/sentiment_per_day/
    df = pd.read_csv(file, header=None)
    susp_tweets_ids = set(int(i) for i in df[0].unique())
    tweets = [1264223201955102727, 1266396421923667968, 1267163158772408320]
    for i in tweets:
        timestamps_non = []
        timestamps_susp = []
        for tweet in db.tweets.find({"id": i}, {"_id": 0, "created_at": 1}):
            start_date = tweet["created_at"]
            end_date = start_date + timedelta(days=1)
        for tweet in db.tweets.find({"retweeted_status.id": i}):
            if tweet["id"] not in susp_tweets_ids:
                timestamps_non.append(tweet["created_at"])

        for tweet in db.tweets.find({"retweeted_status.id": i}):
            if tweet["id"] in susp_tweets_ids:
                timestamps_susp.append(tweet["created_at"])

        df = pd.DataFrame(timestamps_non, columns=['date'])
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'] < end_date]
        df2 = pd.DataFrame(timestamps_susp, columns=['date'])
        df2['date'] = pd.to_datetime(df2['date'])
        df2 = df2[df2['date'] < end_date]

        fig, ax = plt.subplots()
        tweet_df_5min = df.groupby(pd.Grouper(key='date', freq='5Min', origin='start')).size()
        tweet_df_5min_2 = df2.groupby(pd.Grouper(key='date', freq='5Min', origin='start')).size()
        hours = mdates.MinuteLocator(interval=30)
        h_fmt = mdates.DateFormatter('%d-%mmm %H:%M')
        ax.xaxis.set_major_locator(hours)
        ax.xaxis.set_major_formatter(h_fmt)

        tweet_df_5min.plot()
        tweet_df_5min_2.plot()
        plt.title(f'Timeseries of retweets of tweet {i} in first 24 hours')
        plt.ylabel('5 Minute Tweet Count')
        plt.grid(True)
        plt.legend(["non-suspended users' retweets", "suspended users' retweets"])
        fig.tight_layout()
        plt.savefig(f'timeseries-{i}.jpg', format='jpg', dpi=500)


def main():
    plt.rcParams['figure.figsize'] = [19.20, 10.80]
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]
    # retweets_stats(db)
    timedistribution(db)


if __name__ == '__main__':
    main()
