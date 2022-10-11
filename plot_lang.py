import pymongo
import collections
from mongoConfig import mongoConfig
import pandas as pd
import matplotlib.pyplot as plt


def get_languages(db):
    languages = collections.defaultdict(lambda: 0)
    for i in db.tweets.find({}, {"lang": 1, "_id": 0}):
        for lang in i["lang"]:
            languages[lang] += 1

    file = open("languages_count.csv", "w", encoding="utf-8")
    list_lang = ""
    list_lang += "Language\tcount"

    for lang in languages:
        list_lang += f"\n{lang}\t{languages[lang]}"
    file.write(list_lang)
    file.close()


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


def main():
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]
    get_languages(db)
    plot_lang_pie()

    client.close()


if __name__ == '__main__':
    main()
