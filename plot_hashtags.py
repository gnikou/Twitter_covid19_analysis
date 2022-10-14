import pandas as pd
import collections
import csv
import calendar
import re
import matplotlib.pyplot as plt


def plot_ht_by_month():
    for month in range(2, 8):
        file = f"hashtags_month_{month}.csv"
        df = pd.read_csv(file, delimiter='\t')
        df = df.sort_values(by="count", ascending=False)

        hashtags = list(df.iloc[0:10]['Hashtag'])
        count = list(df.iloc[0:10]['count'])

        fig = plt.figure()

        # creating the bar plot
        plt.bar(hashtags, count, color='blue', width=0.4)
        fig.autofmt_xdate()

        plt.ticklabel_format(style='plain', axis='y')
        plt.xticks(fontname="MS Gothic")  # This argument will change the font.
        plt.title(f"Top 10 popular hashtags for {calendar.month_name[month]}")
        plt.xlabel("Hashtags")
        plt.ylabel("Number of times shown in tweets")
        fig.tight_layout()
        plt.savefig(f"top10_hashtags_month_{month}.jpg", format='jpg', dpi=500)


def plot_other_hashtags():
    for month in range(2, 8):
        file = f"hashtags_month_{month}.csv"
        d = pd.read_csv(file, delimiter='\t', encoding='utf-8')
        d = d.sort_values(by="count", ascending=False)

        patternDel = "(covid|coronavirus)+"
        df = d[d['Hashtag'].str.contains(patternDel, flags=re.IGNORECASE) == False]
        hashtags = list(df.iloc[0:10]['Hashtag'])
        count = list(df.iloc[0:10]['count'])

        fig = plt.figure()

        plt.bar(hashtags, count, color='blue', width=0.4)
        plt.xticks(fontname="MS Gothic")
        fig.autofmt_xdate()
        plt.ticklabel_format(style='plain', axis='y')
        plt.title(f"Top 10 other hashtags for {calendar.month_name[month]}")
        plt.xlabel("Hashtags")
        plt.ylabel("Number of times shown in tweets")
        fig.tight_layout()
        plt.savefig(f"top10_other_hashtags_month_{month}.jpg", format='jpg', dpi=500)


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

    d = pd.DataFrame(columns=['month'] + hts)
    for month in range(2, 8):
        nums = [calendar.month_name[month]]
        file = f"hashtags_month_{month}.csv"
        df = pd.read_csv(file, delimiter='\t')
        df.set_index("Hashtag", inplace=True, drop=True)
        for i in hts:
            nums.append(df.loc[i, 'count'])
        d.loc[d.shape[0]] = nums
    d.set_index("month", inplace=True)

    fig = plt.figure()
    d.plot(kind='bar')
    plt.ticklabel_format(style='plain', axis='y')
    plt.title("Monthly top hashtags volume")
    plt.ylabel("Count")
    plt.xlabel("Month")
    fig.tight_layout()
    plt.savefig("top_hashtags_comparison.jpg", format='jpg', dpi=500)


def main():
    plt.rcParams['figure.figsize'] = [19.20, 10.80]
    plot_ht_by_month()
    plot_other_hashtags()
    plot_comparing_top_hts()


if __name__ == '__main__':
    main()
