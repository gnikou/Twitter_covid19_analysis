import pandas as pd
import matplotlib.pyplot as plt


def main():
    filename = "tweets_users_count_pd.csv"
    fig, ax = plt.subplots()

    df = pd.read_csv(filename, delimiter='\t')
    df.set_index("day", inplace=True, drop=True)
    df.plot(ax=ax)

    plt.title("Volume of tweets-users per day")
    plt.xlabel('Dates')
    plt.ylabel('Volume')
    fig.autofmt_xdate()
    ax.ticklabel_format(style='plain', axis='y')
    fig.set_size_inches((20.92, 11.77), forward=False)
    plt.margins(x=0)
    fig.tight_layout()
    plt.savefig('tweets-users-pd.jpg', dpi=500)


if __name__ == '__main__':
    main()
