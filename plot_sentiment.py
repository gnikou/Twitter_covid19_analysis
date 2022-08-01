import pandas as pd
import matplotlib.pyplot as plt


def plot_positive_sentiment(filename):
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df = df[['day', 'positive_for_covid', 'positive_for_lockdown',
             'positive_for_vaccine', 'positive_for_conspiracy',
             'positive_for_masks', 'positive_for_cases',
             'positive_for_deaths', 'positive_for_propaganda',
             'positive_for_5G']].copy()
    df.set_index("day", inplace=True, drop=True)
    df.plot(ax=ax)

    plt.title("Positive sentiment score of covid-related tweets")
    plt.xlabel('Dates')
    plt.ylabel('Sentiment score')
    fig.autofmt_xdate()
    fig.set_size_inches((20.92, 11.77), forward=False)
    plt.savefig('positive-sentiment-plot.png', dpi=500)
    # plt.show()


def plot_negative_sentiment(filename):
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df = df[['day', 'negative_for_covid', 'negative_for_lockdown',
             'negative_for_vaccine', 'negative_for_conspiracy',
             'negative_for_masks', 'negative_for_cases',
             'negative_for_deaths', 'negative_for_propaganda',
             'negative_for_5G']].copy()
    df.set_index("day", inplace=True, drop=True)
    df = -df
    df.plot(ax=ax)

    plt.title("negative sentiment score of covid-related tweets")
    plt.xlabel('Dates')
    plt.ylabel('Sentiment score')
    fig.autofmt_xdate()
    fig.set_size_inches((20.92, 11.77), forward=False)
    plt.savefig('negative-sentiment-plot.png', dpi=500)
    # plt.show()


def main():
    filename = "twitter_covid_sentiment.csv"
    plot_positive_sentiment(filename)
    plot_negative_sentiment(filename)


if __name__ == '__main__':
    main()