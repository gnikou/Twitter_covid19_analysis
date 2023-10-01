import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter  # , AutoDateLocator, AutoDateFormatter
import matplotlib.dates as mdates


def plot_positive_sentiment(filename):
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df = df[['day', 'positive_for_covid', 'positive_for_lockdown',
             'positive_for_vaccine', 'positive_for_conspiracy',
             'positive_for_masks', 'positive_for_cases',
             'positive_for_deaths', 'positive_for_propaganda']].copy()

    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    plt.title("Positive sentiment score of covid-related tweets", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Sentiment score')
    fig.autofmt_xdate()
    ax.legend(loc='upper center')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('positive-sentiment-plot.pdf', format='pdf', dpi=300)


def plot_negative_sentiment(filename):
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df = df[['day', 'negative_for_covid', 'negative_for_lockdown',
             'negative_for_vaccine', 'negative_for_conspiracy',
             'negative_for_masks', 'negative_for_cases',
             'negative_for_deaths', 'negative_for_propaganda']].copy()
    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    c = df.describe()
    df = -df

    df.plot(ax=ax, x_compat=True)
    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    plt.title("Negative sentiment score of covid-related tweets", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Sentiment score')
    fig.autofmt_xdate()
    ax.legend(loc='lower center')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('negative-sentiment-plot.pdf', format='pdf', dpi=300)


def plot_suspended_positive_sentiment(filename):
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t', index_col=False)
    df = df[['day', 'positive_for_covid', 'positive_for_lockdown',
             'positive_for_vaccine', 'positive_for_conspiracy',
             'positive_for_masks', 'positive_for_cases',
             'positive_for_deaths', 'positive_for_propaganda']].copy()

    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    df.plot(ax=ax, x_compat=True)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    plt.title("Positive sentiment score of suspended accounts on covid-related tweets", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Sentiment score')
    fig.autofmt_xdate()
    plt.margins(x=0)
    fig.tight_layout()
    ax.legend(loc="upper left", ncol=6)
    plt.savefig('suspended-positive-sentiment-plot.pdf', format='pdf', dpi=300)


def plot_suspended_negative_sentiment(filename):
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t', index_col=False)
    df = df[['day', 'negative_for_covid', 'negative_for_lockdown',
             'negative_for_vaccine', 'negative_for_conspiracy',
             'negative_for_masks', 'negative_for_cases',
             'negative_for_deaths', 'negative_for_propaganda']].copy()

    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    d = df.describe()

    df = -df

    df.plot(ax=ax, x_compat=True)
    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    # ax.set_xlim(["2020-02-19", "2020-07-11"])

    plt.title("Negative sentiment score of suspended accounts on covid-related tweets", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Sentiment score')
    fig.autofmt_xdate()
    ax.legend(loc='lower center', ncol=6)
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('suspended-negative-sentiment-plot.pdf', format='pdf', dpi=300)


def plot_rolling_average(filename):
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df = df[['day', 'positive_for_covid', 'negative_for_covid', 'positive_for_conspiracy',
             'negative_for_conspiracy', 'positive_for_lockdown', 'negative_for_lockdown']].copy()

    df['day'] = pd.to_datetime(df['day'])
    df.set_index("day", inplace=True, drop=True)
    df = df.rolling(7).mean()
    ax = df.plot(ax=ax, x_compat=True)
    improve_legend(ax)

    ax.xaxis.set_major_formatter(DateFormatter("%d-%m-%Y"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    plt.title("Sentiment score of covid-related tweets", fontsize=24, fontweight="bold")
    plt.xlabel('Dates')
    plt.ylabel('Sentiment score')
    fig.autofmt_xdate()
    # ax.legend(loc='upper center')
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('test.pdf', format='pdf', dpi=300)


def improve_legend(ax=None):
    if ax is None:
        ax = plt.gca()

    # for spine in ax.spines:
    ax.spines["right"].set_visible(False)

    for line in ax.lines:
        data_x, data_y = line.get_data()
        right_most_x = data_x[-1]
        right_most_y = data_y[-1]
        ax.annotate(
            line.get_label(),
            xy=(right_most_x, right_most_y),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            color=line.get_color(),
        )
    ax.legend().set_visible(False)


def main():
    plt.rcParams.update({
        'figure.figsize': [19.20, 10.80],
        'font.size': 14,
        'axes.labelsize': 22,
        'legend.fontsize': 11,
        'lines.linewidth': 2
    })
    filename = "twitter_covid_sentiment.csv"
    susp_filename = "suspended_twitter_covid_sentiment.csv"

    plot_positive_sentiment(filename)
    plot_negative_sentiment(filename)
    plot_suspended_positive_sentiment(susp_filename)
    plot_suspended_negative_sentiment(susp_filename)


if __name__ == '__main__':
    main()
