import pymongo
from mongoConfig import mongoConfig
from datetime import timedelta
from dateutil import parser
import pandas as pd


def check_argmax_label(label, start_date):
    file_sentiment = f"/home/gnikou/sentiment_per_day/sentiment_by_id_day_2020-{start_date.month}-{start_date.day}.csv"
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

    maxlabel_tweet_ids = set(int(i) for i in df.index)
    return maxlabel_tweet_ids


def rts_positive(db, susp_user_ids, deleted_user_ids):
    file_general = open(f"graph_retweets_positive_for_conspiracy.csv", "w")
    file_general_text = "User_id,Tweet ID,Number of Total Retweets,Suspended users retweets"
    file_pos = open("positive_conspiracy.csv", "w")
    file_pos_text = "Date\tUser_id\tTweet_ID\tText"
    dates = ["2020-4-8", "2020-4-27", "2020-5-1", "2020-5-31", "2020-6-16"]
    dates = [parser.parse(date) for date in dates]
    dictionary = {
        dates[0]: [482314103, 259260816, 3421316315, 162301746, 406483952, 878247600096509952, 18266688, 18166778,
                   91882544],
        dates[1]: [878247600096509952, 1049162319941517312, 799111347111751680, 1003140853571862528, 304123273,
                   1090687812062662656, 1234318846334394368],
        dates[2]: [1049814472817344512, 729676086632656900, 905150926280953856, 1003140853571862528, 877699334,
                   901063185305882624, 16989178, 14828860],
        dates[3]: [878247600096509952, 133184048, 1004694257725296641, 94132483, 23216714],
        dates[4]: [1249837543383859201, 50434327, 259260816, 489507027, 1136709465082945538, 966612436978581504]}

    for start_date in dates:
        maxlabel_tweet_ids = check_argmax_label("positive_for_conspiracy", start_date)
        end_date = start_date + timedelta(days=1)
        file = open(f"graph_retweets_positive_for_conspiracy_2020-{start_date.month}-{start_date.day}.csv", "w")

        file_text = "User_id,Tweet ID,Number of Total Retweets,Suspended users retweets"
        for user in dictionary[start_date]:
            for tweet in db.tweets.find({"created_at": {"$gte": start_date, "$lt": end_date}, "user_id": user,
                                         "retweeted_status": {"$exists": False}}):
                if tweet["id"] in maxlabel_tweet_ids:
                    suspended_rts = 0
                    non_susp_rts = 0
                    deleted_rts = 0
                    file_pos_text += f"\n{start_date.date()}\t{user}\t{tweet['id']}\t{tweet['text']}"
                    original_id = tweet["id"]
                    for rt in db.tweets.find({"retweeted_status.id": original_id}):
                        if rt["user_id"] in susp_user_ids:
                            suspended_rts += 1
                        elif rt["user_id"] in deleted_user_ids:
                            deleted_rts += 1
                        else:
                            non_susp_rts += 1
                    file_text += f"\n{user},{original_id},{suspended_rts + deleted_rts + non_susp_rts},{suspended_rts + deleted_rts}"
                    file_general_text += f"\n{user},{original_id},{suspended_rts + deleted_rts + non_susp_rts},{suspended_rts + deleted_rts}"
        file.write(file_text)
        file.close()
    file_pos.write(file_pos_text)
    file_pos.close()
    file_general.write(file_general_text)
    file_general.close()


def rts_negative(db, susp_user_ids, deleted_user_ids):
    file_general = open(f"graph_retweets_negative_for_conspiracy.csv", "w")
    file_general_text = "User_id,Tweet ID,Number of Total Retweets,Suspended users retweets"
    file_neg = open("negative_conspiracy.csv", "w")
    file_neg_text = "Date\tUser_id\tTweet_ID\tText"
    dates = ["2020-2-28", "2020-2-29", "2020-4-3", "2020-6-15"]
    dates = [parser.parse(date) for date in dates]
    dictionary = {
        dates[0]: [1092086942198452224, 15576967, 17980523, 729676086632656900, 142339174, 49023129, 2352629311,
                   988573326376427520, 101824385],
        dates[1]: [138182116, 877699334, 69190453, 14828860, 1092086942198452224, 817158775610179584, 172099674,
                   23443030, 1155688202730008578, 1222170036661628928],
        dates[2]: [1024775399736311808, 957234158111215616, 8775672, 259260816, 1136709465082945538],
        dates[3]: [2410068528, 2972164989]}
    for start_date in dates:
        maxlabel_tweet_ids = check_argmax_label("negative_for_conspiracy", start_date)
        end_date = start_date + timedelta(days=1)
        file = open(f"graph_retweets_negative_for_conspiracy_2020-{start_date.month}-{start_date.day}.csv", "w")

        file_text = "User_id,Tweet ID,Number of Total Retweets,Suspended users retweets"
        for user in dictionary[start_date]:
            for tweet in db.tweets.find({"created_at": {"$gte": start_date, "$lt": end_date}, "user_id": user,
                                         "retweeted_status": {"$exists": False}}):
                if tweet["id"] in maxlabel_tweet_ids:
                    suspended_rts = 0
                    non_susp_rts = 0
                    deleted_rts = 0
                    file_neg_text += f"\n{start_date.date()}\t{user}\t{tweet['id']}\t{tweet['text']}"
                    original_id = tweet["id"]
                    for rt in db.tweets.find({"retweeted_status.id": original_id}):
                        if rt["user_id"] in susp_user_ids:
                            suspended_rts += 1
                        elif rt["user_id"] in deleted_user_ids:
                            deleted_rts += 1
                        else:
                            non_susp_rts += 1
                    file_text += f"\n{user},{original_id},{suspended_rts + deleted_rts + non_susp_rts},{suspended_rts + deleted_rts}"
                    file_general_text += f"\n{user},{original_id},{suspended_rts + deleted_rts + non_susp_rts},{suspended_rts + deleted_rts}"
        file.write(file_text)
        file.close()
    file_neg.write(file_neg_text)
    file_neg.close()
    file_general.write(file_general_text)
    file_general.close()


def main():
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]
    file_susp = "id_suspended_users.csv"
    df_suspended = pd.read_csv(file_susp, header=None)
    susp_user_ids = set(int(i) for i in df_suspended[0].unique())

    file_deleted = "id_deleted.csv"
    df_deleted = pd.read_csv(file_deleted, header=None)
    deleted_user_ids = set(int(i) for i in df_deleted[0].unique())

    rts_positive(db, susp_user_ids, deleted_user_ids)
    rts_negative(db, susp_user_ids, deleted_user_ids)


if __name__ == '__main__':
    main()
