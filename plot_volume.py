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
    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    # ax.set_xlim(["2020-02-19", "2020-07-11"])

    plt.title("Volume of tweets-users per day")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    fig.set_size_inches((20.92, 11.77), forward=False)
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('daily-tweets-users.jpg', dpi=500)


def suspended_only():
    filename = "daily_suspended_volume.csv"
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df['day'] = pd.to_datetime(df['day'])

    df.set_index("day", inplace=True, drop=True)
    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    # ax.set_xlim(["2020-02-19", "2020-07-11"])

    plt.title("Volume of suspended tweets-users per day")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    fig.set_size_inches((20.92, 11.77), forward=False)
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('daily-suspended tweets-users.jpg', dpi=500)


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
        df.loc[i, "tweets"] = df.loc[i, "tweets"] - df.loc[i, "suspended_tweets"]
        df.loc[i, "users"] = df.loc[i, "users"] - df.loc[i, "suspended_users"]

    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    # ax.set_xlim(["2020-02-19", "2020-07-11"])

    plt.title("Daily comparison of suspended-all tweets-users")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    plt.legend(["non-suspended users' tweets", "non-suspended users", "suspended users' tweets", "suspended users"])
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    fig.set_size_inches((20.92, 11.77), forward=False)
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('daily-comparison-volume.jpg', dpi=500)


def main():
    all_tweets()
    suspended_only()
    compare_suspended_all()


if __name__ == '__main__':
    main()
