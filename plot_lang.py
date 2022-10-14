import pymongo
import collections
from mongoConfig import mongoConfig
import pandas as pd
import matplotlib.pyplot as plt
from dateutil import parser
from datetime import timedelta


def plot_lang_pie():
    filename = "languages_count.csv"
    df = pd.read_csv(filename, delimiter='\t')
    df = df.sort_values(by="count", ascending=False)
    df = df.iloc[0:9]
    df.set_index("Language", inplace=True, drop=True)
    data = list(df['count'])
    cols = list(df.index)
    plt.pie(data, labels=cols, autopct='%.1f%%')
    plt.title("Most used languages in tweets")
    plt.savefig('languages-pie.jpg', format='jpg', dpi=500)


def write_count_pd(db):
    filename = "languages_count.csv"
    df = pd.read_csv(filename, delimiter='\t')
    df = df.sort_values(by="count", ascending=False)
    langs = list(df.iloc[0:9]["Language"])

    start_date = [parser.parse(i['created_at'].strftime('%Y-%m-%d')) for i in
                  db.tweets.find({}, {"created_at": 1, "_id": 0}).sort('created_at', 1).limit(1)][0]
    cur_date = start_date
    end_date = [parser.parse(i['created_at'].strftime('%Y-%m-%d')) for i in
                db.tweets.find({}, {"created_at": 1, "_id": 0}).sort('created_at', -1).limit(1)][0]

    file = open("language_count_pd.csv", "w")
    list_hts = ""
    list_hts += "Date"
    for lang in langs:
        list_hts += f"\t{lang}"
    file.write(list_hts)
    file.close()

    while cur_date <= end_date:
        lang_count = list()
        for lang in langs:
            lang_count.append(db.tweets.count_documents(
                {"$and": [{"created_at": {"$gte": cur_date, "$lt": cur_date + timedelta(days=1)}}, {"lang": lang}]}))
        file = open("language_count_pd.csv", "a")
        list_hts = ""
        list_hts += f"\n{cur_date.year}-{cur_date.month}-{cur_date.day}"
        for i in lang_count:
            list_hts += f"\t{i}"
        file.write(list_hts)
        file.close()
        cur_date += timedelta(days=1)


def plot_count_pd():
    fig, ax = plt.subplots()
    df = pd.read_csv("language_count_pd.csv", delimiter='\t')
    df.set_index("Date", inplace=True, drop=True)
    df.plot(ax=ax)
    plt.title("Daily language count in tweets")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    plt.ticklabel_format(style='plain', axis='y')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('language_count_pd.jpg', format='jpg', dpi=500)
    fig, ax = plt.subplots()
    df = df.drop('en', axis=1)
    df.plot(ax=ax)
    plt.title("Daily language count without english in tweets")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    plt.ticklabel_format(style='plain', axis='y')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('language_count_no_en.jpg', format='jpg', dpi=500)


def main():
    plt.rcParams['figure.figsize'] = [19.20, 10.80]
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]
    plot_lang_pie()
    write_count_pd(db)
    plot_count_pd()

    client.close()


if __name__ == '__main__':
    main()
