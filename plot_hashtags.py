import pandas as pd
import matplotlib.pyplot as plt
import collections
import csv
import os.path
import calendar
import re


def write_hashtags_by_month():
    for month in range(2, 8):
        hashtags = collections.defaultdict(int)
        for day in range(1, 32):
            file = f"hashtags_count_day_2020-{month}-{day}.csv"
            if os.path.exists(file):
                with open(file, encoding='utf-8') as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter='\t')
                    next(csv_reader)
                    for row in csv_reader:
                        hashtags[row[0]] += int(row[1])
        file = open(f"hashtags_month_{month}.csv", "w", encoding="utf-8")
        list_hts = ""
        list_hts += "Hashtag\tcount"

        for ht in hashtags:
            list_hts += f"\n{ht}\t{hashtags[ht]}"
        file.write(list_hts)
        file.close()


def plot_ht_by_month():
    for month in range(2, 8):
        file = f"hashtags_month_{month}.csv"
        df = pd.read_csv(file, delimiter='\t')
        df = df.sort_values(by="count", ascending=False)

        hashtags = list(df.iloc[0:10]['Hashtag'])
        count = list(df.iloc[0:10]['count'])

        fig = plt.figure(figsize=(10, 5))

        # creating the bar plot
        plt.bar(hashtags, count, color='blue', width=0.4)
        fig.autofmt_xdate()
        plt.ticklabel_format(style='plain', axis='y')
        fig.set_size_inches((20.92, 11.77), forward=False)

        plt.title(f"Top 10 popular hashtags for {calendar.month_name[month]}")
        plt.xlabel("Hashtags")
        plt.ylabel("Number of times shown in tweets")
        plt.savefig(f"top10_hashtags_month_{month}.jpg", dpi=500)


def plot_other_hashtags():
    for month in range(2, 8):
        file = f"hashtags_month_{month}.csv"
        d = pd.read_csv(file, delimiter='\t')
        d = d.sort_values(by="count", ascending=False)

        patternDel = "(covid|coronavirus)+"
        df = d[d['Hashtag'].str.contains(patternDel, flags=re.IGNORECASE) == False]
        hashtags = list(df.iloc[0:10]['Hashtag'])
        count = list(df.iloc[0:10]['count'])

        fig = plt.figure(figsize=(10, 5))

        # creating the bar plot
        plt.bar(hashtags, count, color='blue', width=0.4)
        fig.autofmt_xdate()
        plt.ticklabel_format(style='plain', axis='y')
        fig.set_size_inches((20.92, 11.77), forward=False)

        plt.title(f"Top 10 other hashtags for {calendar.month_name[month]}")
        plt.xlabel("Hashtags")
        plt.ylabel("Number of times shown in tweets")
        plt.savefig(f"top10_other_hashtags_month_{month}.jpg", dpi=500)


def plot_comparing_top_hts():
    hashtags = collections.defaultdict(int)
    for month in range(2, 8):
        file = f"hashtags_month_{month}.csv"
        with open(file, encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t')
            next(csv_reader)
            for row in csv_reader:
                hashtags[row[0]] += int(row[1])

    df = pd.DataFrame.from_dict(hashtags, orient='index', columns=["count"])
    df = df.sort_values(by="count", ascending=False)
    hts = list(df.iloc[0:5].index)



def main():
    write_hashtags_by_month()
    plot_ht_by_month()
    plot_other_hashtags()
    #plot_comparing_top_hts()


if __name__ == '__main__':
    main()
