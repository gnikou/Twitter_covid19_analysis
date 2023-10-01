import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates


def all_tweets():
    filename = "tweets_users_count_pd.csv"
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    d = df.describe()

    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    plt.title("Volume of tweets-users per day", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    plt.margins(x=0)
    fig.tight_layout()
    plt.axvline(x='2020-3-11', linewidth=2.5, linestyle='--', color='black')
    ax.annotate('WHO declares COVID-19 a pandemic', xy=('2020-3-11', 3), xytext=(15, 15),
                textcoords='offset points', arrowprops=dict(arrowstyle='-|>'), fontsize=20)
    plt.savefig('daily-tweets-users.pdf', dpi=300)


def suspended_only():
    filename = "daily_suspended_volume.csv"
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df['day'] = pd.to_datetime(df['day'])

    df.set_index("day", inplace=True, drop=True)
    c = df.describe()

    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    plt.title("Volume of suspended tweets-users per day", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    plt.margins(x=0)
    fig.tight_layout()
    ax.legend(["tweets", "users"])

    plt.axvline(x='2020-3-11', linewidth=2.5, linestyle='--', color='black')
    ax.annotate('WHO declares COVID-19 a pandemic', xy=('2020-3-11', 3), xytext=(15, 15),
                textcoords='offset points', arrowprops=dict(arrowstyle='-|>'), fontsize=20)
    plt.savefig('daily-suspended tweets-users.pdf', dpi=300)


def compare_suspended_all():
    filename1 = "tweets_users_count_pd.csv"
    filename2 = "daily_suspended_volume.csv"
    fig, ax = plt.subplots()

    df1 = pd.read_csv(filename1, delimiter='\t')
    df1['day'] = pd.to_datetime(df1['day'])
    df1.set_index("day", inplace=True, drop=True)
    df2 = pd.read_csv(filename2, delimiter='\t')
    df2['day'] = pd.to_datetime(df2['day'])
    df2.set_index("day", inplace=True, drop=True)

    df = pd.concat([df1, df2], axis=1)

    # remove suspended users/tweets from all tweets
    for i in df.index:
        df.loc[i, "tweets"] = df.loc[i, "tweets"] - df.loc[i, "suspended_count"]
        df.loc[i, "users"] = df.loc[i, "users"] - df.loc[i, "suspended_users"]
    # df["non suspended ratio"] = df["tweets"] / df["users"]
    # df["suspended ratio"] = df["suspended_count"] / df["suspended_users"]
    # df = df.drop("tweets", axis=1)
    # df = df.drop("users", axis=1)
    # df = df.drop("suspended_count", axis=1)
    # df = df.drop("suspended_users", axis=1)

    # print(f"Tweets/unique users ratio (Suspended users): {df['suspended ratio'].mean()}")
    # print(f"Tweets/unique users ratio (Non suspended users): {df['non suspended ratio'].mean()}")

    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    plt.title("Daily comparison of tweets/unique users' ratio")
    plt.xlabel('Dates')
    plt.ylabel('Ratio (tweets/unique users)')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('daily-comparison-ratio.pdf', dpi=300)


def volume_per_month():
    filename = "tweets_users_count_pd.csv"
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    d = df.groupby(pd.Grouper(freq='M')).sum()
    d.index = d.index.month

    d.plot(kind='bar', ax=ax)

    plt.title("Volume of tweets-users per month", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('volume_per_month.pdf', dpi=300)


def average_volume_per_month():
    filename = "tweets_users_count_pd.csv"
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    d = df.groupby(pd.Grouper(freq='M')).mean()
    d.index = d.index.month

    d.plot(kind='bar', ax=ax)

    plt.title("Average daily volume of tweets-users per month", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('average_volume_per_month.pdf', dpi=300)


def main():
    plt.rcParams.update({
        'figure.figsize': [19.20, 10.80],
        'font.size': 16,
        'axes.labelsize': 22,
        'legend.fontsize': 20,
        'lines.linewidth': 2
    })
    all_tweets()
    suspended_only()
    compare_suspended_all()
    # volume_per_month()
    # average_volume_per_month()


if __name__ == '__main__':
    main()
