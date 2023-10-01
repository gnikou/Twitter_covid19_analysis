import os
import pandas as pd
import matplotlib.pyplot as plt


def plots():
    file = "graph_retweets_positive_for_conspiracy.csv"
    df = pd.read_csv(file, delimiter=',')
    group_users = df.groupby(by="User_id").sum()
    group_users = group_users.drop("Tweet ID", axis=1)
    group_users["Non suspended users' retweets"] = group_users["Number of Total Retweets"] - group_users[
        "Suspended users retweets"]
    group_users = group_users.drop("Number of Total Retweets", axis=1)
    group_users = group_users.sort_values(by="Suspended users retweets", ascending=True)

    fig, ax = plt.subplots()
    group_users.plot(kind='barh', stacked=True)
    plt.ticklabel_format(style='plain', axis='x')
    plt.title("Number of center user being retweeted in crowded graph clusters on label 'positive for conspiracy'", fontsize=22, fontweight="bold")
    plt.ylabel("User ID")
    plt.xlabel("Count")
    plt.legend(loc="lower right")
    fig.tight_layout()
    plt.savefig("rts_graph_positive_for_conspiracy.pdf", format='pdf', dpi=300)

    fig, ax = plt.subplots()
    file = "graph_retweets_negative_for_conspiracy.csv"
    df = pd.read_csv(file, delimiter=',')
    group_users = df.groupby(by="User_id").sum()
    group_users = group_users.drop("Tweet ID", axis=1)
    group_users["Non suspended users' retweets"] = group_users["Number of Total Retweets"] - group_users[
        "Suspended users retweets"]
    group_users = group_users.drop("Number of Total Retweets", axis=1)
    group_users = group_users.sort_values(by="Suspended users retweets", ascending=True)
    group_users.plot(kind='barh', stacked=True)
    plt.ticklabel_format(style='plain', axis='x')
    plt.title("Number of center user being retweeted in crowded graph clusters on label 'negative for conspiracy'", fontsize=22, fontweight="bold")
    plt.ylabel("User ID")
    plt.xlabel("Count")
    fig.tight_layout()
    plt.legend(loc='lower right')
    plt.savefig("rts_graph_negative_for_conspiracy.pdf", format='pdf', dpi=300)


def plots_by_day():
    filenames = os.listdir()
    files = [name for name in filenames if
             name.startswith("graph_retweets_positive_for_conspiracy_2020") or name.startswith(
                 "graph_retweets_negative_for_conspiracy_2020")]
    for file in files:
        df = pd.read_csv(file, delimiter=',')
        df = df.groupby(by="Tweet ID").sum()
        df = df.drop("User_id", axis=1)
        df["Non suspended users' retweets"] = df["Number of Total Retweets"] - df[
            "Suspended users retweets"]
        df = df.drop("Number of Total Retweets", axis=1)
        df = df.sort_values(by="Suspended users retweets", ascending=True)

        fig, ax = plt.subplots()
        df.plot(kind='barh', stacked=True)
        plt.ticklabel_format(style='plain', axis='x')
        plt.title("Number of retweets per tweet in crowded graph clusters", fontsize=24, fontweight="bold")
        plt.ylabel("Tweet ID")
        plt.xlabel("Count")
        # ax.legend(loc='lower right')
        fig.tight_layout()
        plt.savefig(f"rts-{file[15:48]}.pdf", format='pdf', dpi=300)


def percentage(group_users):
    return (group_users["Suspended accounts retweets"] / group_users["Number of Total Retweets"]) * 100


def main():
    plt.rcParams.update({
        'figure.figsize': [19.20, 10.80],
        'font.size': 20,
        'axes.labelsize': 20,
        'legend.fontsize': 20,
        'lines.linewidth': 2,
        'ytick.labelsize': 11
    })
    plots()
    # plots_by_day()


if __name__ == '__main__':
    main()
