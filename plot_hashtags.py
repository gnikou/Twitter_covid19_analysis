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

        plt.bar(hashtags, count, color='blue', width=0.4)
        fig.autofmt_xdate()

        plt.ticklabel_format(style='plain', axis='y')
        plt.xticks(fontname="MS Gothic")
        plt.title(f"Top 10 popular hashtags for {calendar.month_name[month]}", fontsize=24, fontweight="bold")
        plt.xlabel("Hashtags")
        plt.ylabel("Number of times shown in tweets")
        fig.tight_layout()
        plt.savefig(f"top10_hashtags_month_{month}.pdf", format='pdf', dpi=300)


def plot_other_hashtags():
    for month in range(2, 8):
        file = f"hashtags_month_{month}.csv"
        d = pd.read_csv(file, delimiter='\t', encoding='utf-8')
        d = d.sort_values(by="count", ascending=False)

        patternDel = "(covid|coronavirus|coronavírus)+"
        df = d[d['Hashtag'].str.contains(patternDel, flags=re.IGNORECASE) == False]
        hashtags = list(df.iloc[0:10]['Hashtag'])
        count = list(df.iloc[0:10]['count'])

        fig = plt.figure()

        plt.bar(hashtags, count, color='blue', width=0.4)
        plt.xticks(fontname="MS Gothic")
        fig.autofmt_xdate()
        plt.ticklabel_format(style='plain', axis='y')
        plt.title(f"Top 10 other hashtags for {calendar.month_name[month]}", fontsize=24, fontweight="bold")
        plt.xlabel("Hashtags")
        plt.ylabel("Number of times shown in tweets")
        fig.tight_layout()
        plt.savefig(f"top10_other_hashtags_month_{month}.pdf", format='pdf', dpi=300)


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
    plt.title("Monthly top hashtags volume", fontsize=24, fontweight="bold")
    plt.ylabel("Count")
    plt.xlabel("Month")
    fig.tight_layout()
    plt.yticks(fontname="MS Gothic")
    plt.xticks(rotation=0)
    plt.savefig("top_hashtags_comparison.pdf", format='pdf', dpi=300)


def plot_noncovid_comparing_top_hts():
    hashtags = collections.defaultdict(int)
    for month in range(2, 8):
        file = f"hashtags_month_{month}.csv"
        with open(file, encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t')
            next(csv_reader)
            for row in csv_reader:
                hashtags[row[0]] += int(row[1])

    df = pd.DataFrame.from_dict(hashtags, orient='index', columns=["count"])
    df.reset_index(inplace=True)
    df = df.rename(columns={'index': 'Hashtag'})
    patternDel = "(covid|coronavirus|coronavírus|โควิด19)+"
    df = df[df['Hashtag'].str.contains(patternDel, flags=re.IGNORECASE) == False]
    df.set_index("Hashtag", inplace=True, drop=True)

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
    plt.title("Monthly top hashtags volume", fontsize=24, fontweight="bold")
    plt.ylabel("Count")
    plt.xlabel("Month")
    fig.tight_layout()
    plt.yticks(fontname="MS Gothic")
    plt.xticks(rotation=0)
    plt.savefig("top_noncovid_hashtags_comparison.pdf", format='pdf', dpi=300)


def main():
    plt.rcParams.update({
        'figure.figsize': [19.20, 10.80],
        'font.size': 16,
        'axes.labelsize': 22,
        'legend.fontsize': 16,
        'xtick.labelsize': 18,
        'lines.linewidth': 2

    })
    # plot_ht_by_month()
    # plot_other_hashtags()
    # plot_comparing_top_hts()
    plot_noncovid_comparing_top_hts()


if __name__ == '__main__':
    main()
