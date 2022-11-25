import os
import pandas as pd
import gensim
from matplotlib import pyplot as plt
from nltk.corpus import stopwords
import gensim.corpora as corpora
from gensim.models import LdaModel
from gensim.models.coherencemodel import CoherenceModel
import pymongo
from mongoConfig import mongoConfig
import pyLDAvis
from pprint import pprint
import pyLDAvis.gensim_models
import nltk
from nltk.corpus import wordnet as wn
from dateutil import parser
import collections


def get_text(db, date, label):
    file = f"/Storage/gnikou/sentiment_per_day/suspended_tweets_day_2020-{date.month}-{date.day}.csv"
    if os.path.exists(file) is False:
        print(f"File {file} isn't here ")
        return

    df = pd.read_csv(file, sep='\t', index_col=False)

    if label.split()[0] == "positive":
        df = df[['tweet_id', 'positive for covid', 'positive for lockdown',
                 'positive for vaccine', 'positive for conspiracy',
                 'positive for masks', 'positive for cases',
                 'positive for deaths', 'positive for propaganda',
                 'positive for 5G']].copy()
    else:
        df = df[['tweet_id', 'negative for covid', 'negative for lockdown',
                 'negative for vaccine', 'negative for conspiracy',
                 'negative for masks', 'negative for cases',
                 'negative for deaths', 'negative for propaganda',
                 'negative for 5G']].copy()

    df.set_index("tweet_id", inplace=True, drop=True)
    df = df[df.idxmax(axis="columns") == label]
    tweets_ids = [int(i) for i in df.index]

    text_dict = dict()
    for tweet_id in tweets_ids:
        for tweet in db.tweets.find({"id": tweet_id, "lang": "en"}, {"id": 1, "text": 1, "_id": 0}):
            text = tweet['text'].replace('\n', '')
            text_dict[tweet['id']] = text

    df = pd.DataFrame.from_dict(text_dict, orient='index', columns=['text'])
    df.reset_index(inplace=True)
    df = df.rename(columns={'index': 'id'})
    file = f"/Storage/gnikou/suspended_texts/suspended_texts-{label.replace(' ', '_')}-2020-{date.month}-{date.day}.csv"
    df.to_csv(file, index=False, sep='\t')


def lda_susp(db, label, date):
    file = f"/Storage/gnikou/suspended_texts/suspended_texts-{label}-2020-{parser.parse(date).month}-{parser.parse(date).day}.csv"
    print(f"{label}\t{date}")
    stop_words = stopwords.words('english')
    stop_words.extend(['from', 'subject', 're', 'edu', 'use', 'https', '&amp', '&amp;'])

    df = pd.read_csv(file, sep='\t', encoding='utf-8')

    data = df.text.values.tolist()

    texts = [clean(t) for t in data]

    id2word = corpora.Dictionary(texts)

    # Term Document Frequency
    corpus = [id2word.doc2bow(text) for text in texts]

    results = []

    for t in range(2, 31):
        lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus, id2word=id2word, num_topics=t)
        corpus_lda = lda_model[corpus]
        cm = CoherenceModel(model=lda_model, corpus=corpus_lda, coherence='u_mass')
        score = cm.get_coherence()
        tup = t, score
        results.append(tup)

    results = pd.DataFrame(results, columns=['topic', 'score'])

    s = pd.Series(results.score.values, index=results.topic.values)
    # s.plot()
    # plt.show()
    num_topics = s.idxmax()

    print(f'The coherence score is highest with {num_topics} topics.')

    lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                                id2word=id2word,
                                                num_topics=num_topics,
                                                passes=10,
                                                per_word_topics=True,
                                                minimum_probability=0)
    text_to_write = ""
    pprint(lda_model.print_topics())
    topics = lda_model.show_topics(formatted=False)
    text_to_write = extract_key_tweets(db, topics, df, data, texts, text_to_write)
    file_w = open(
        f"/Storage/gnikou/prevalent_tweets_stats/susp_tweets_stats-{label}-2020-{parser.parse(date).month}-{parser.parse(date).day}.txt",
        "w+")
    file_w.write(text_to_write)
    file_w.close()

    # Compute Perplexity
    print('\nPerplexity: ', lda_model.log_perplexity(corpus))  # a measure of how good the model is. lower the better.

    vis = pyLDAvis.gensim_models.prepare(lda_model, corpus, id2word)
    try:
        pyLDAvis.save_html(vis, f"/Storage/gnikou/LDA_files/LDA-{label}-{date}.html")
    except:
        print("Saving failed")
    print_topics(num_topics, topics, label, date)


'''
def lda_susp2(db, label, date):
    # file = f"/Storage/gnikou/sentiment_per_day/suspended_texts-{label}-2020-{parser.parse(date).month}-{parser.parse(date).day}.csv"
    file = open("susp_texts.csv")
    print(f"{label}\t{date}")
    stop_words = stopwords.words('english')
    stop_words.extend(['from', 'subject', 're', 'edu', 'use', 'https', '&amp', '&amp;'])

    # print(d.size)
    df = pd.read_csv(file, sep='\t', encoding='utf-8')

    data = df.text.values.tolist()

    texts = [clean(t) for t in data]
    # print(texts[:1])

    id2word = corpora.Dictionary(texts)

    # Term Document Frequency
    corpus = [id2word.doc2bow(text) for text in texts]

    # View
    # print([[(id2word[i], freq) for i, freq in cp] for cp in corpus[:1]])

    results = []

    for t in range(2, 31):
        lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus, id2word=id2word, num_topics=t)
        corpus_lda = lda_model[corpus]
        cm = CoherenceModel(model=lda_model, corpus=corpus_lda, coherence='u_mass')
        score = cm.get_coherence()
        tup = t, score
        results.append(tup)

    results = pd.DataFrame(results, columns=['topic', 'score'])

    s = pd.Series(results.score.values, index=results.topic.values)
    # s.plot()
    # plt.show()
    num_topics = s.idxmax()

    print(f'The coherence score is highest with {num_topics} topics.')

    lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                                id2word=id2word,
                                                num_topics=num_topics,
                                                passes=10,
                                                per_word_topics=True,
                                                minimum_probability=0)

    pprint(lda_model.print_topics())
'''


def extract_key_tweets(db, topics, df, data, texts, text_to_write):
    dct = dict()
    dct2 = collections.defaultdict(lambda: 0)

    dct_topic = dict()
    for item in topics:
        d = dict(item[1])
        names = list(d.keys())
        for idx, i in enumerate(texts):
            common = set(names).intersection(set(i))
            if len(common) > 9:
                dct2[data[idx]] += 1
                if data[idx] not in dct.keys():
                    dct[data[idx]] = df.iloc[idx]['id']
                    dct_topic[data[idx]] = item[0]

    for i in dct2.keys():
        if dct2[i] < 5:
            del dct[i]
            del dct_topic[i]

    for i in dct_topic.keys():
        print(f"{i} Topic:{dct_topic[i]}")
        text_to_write += f"{i} Topic:{dct_topic[i]}\n"

    if dct:
        text_to_write = retweets_stats(db, dct, text_to_write)
    return text_to_write


def retweets_stats(db, dct, text_to_write):
    file = "/Storage/gnikou/sentiment_per_day/tweets_from_suspended_users"  #
    df = pd.read_csv(file, header=None)
    susp_tweets_ids = set(int(i) for i in df[0].unique())
    tweets = dct.keys()

    for i in tweets:
        suspended_rts = 0
        non_susp_rts = 0
        tweet_id = int(dct[i])
        for tweet in db.tweets.find({"id": tweet_id}):
            try:
                original_id = (tweet["retweeted_status"]["id"])
            except KeyError:
                original_id = (tweet["id"])

        for tweet in db.tweets.find({"id": original_id}):
            original_author = tweet["user_id"]

        for tweet in db.tweets.find({"retweeted_status.id": original_id}):
            if tweet["id"] in susp_tweets_ids:
                suspended_rts += 1
            else:
                non_susp_rts += 1
        print(f"\nOriginal id: {original_id} Original author:{original_author}")
        print(f"Text: {i}")
        print(f"Total: {suspended_rts + non_susp_rts}")
        print(f"Suspended: {suspended_rts}")
        print(f"Non Suspended: {non_susp_rts}")

        text_to_write += f"\n==========\nOriginal id: {original_id} Original author:{original_author}\nText: {i}" \
                         f"\nTotal: {suspended_rts + non_susp_rts}\n" \
                         f"Suspended: {suspended_rts}\nNon Suspended: {non_susp_rts}"

        try:
            print(f"Ratio: {suspended_rts / (suspended_rts + non_susp_rts)}\n")
            text_to_write += f"\nRatio: {suspended_rts / (suspended_rts + non_susp_rts)}"
        except ZeroDivisionError:
            pass

    return text_to_write


def print_topics(num_topics, t, label, date):
    if num_topics < 3:
        nrows = 1
        ncols = 2
    elif num_topics < 5:
        nrows = 2
        ncols = 2
    elif num_topics < 10:
        nrows = 3
        ncols = 3
    elif num_topics < 17:
        nrows = 4
        ncols = 4
    elif num_topics < 26:
        nrows = 5
        ncols = 5
    else:
        nrows = 6
        ncols = 6

    fig, ax = plt.subplots()

    for item in t:
        d = dict(item[1])
        names = list(d.keys())
        names.reverse()
        values = list(d.values())
        values.reverse()

        plt.subplot(nrows, ncols, item[0] + 1)
        ax.set_xticks([])  # values
        ax.set_xticklabels([])  # labels
        plt.title(f"most significant words for topic {item[0] + 1}")
        plt.xlabel('Score')
        plt.barh(names, values, tick_label=names)

    plt.savefig(f"/Storage/gnikou/LDA_files/LDA-{label}-{date}.jpg", format='jpg', dpi=500)


def clean(text):
    t = str(text)
    t = t.lower().strip()
    t = t.split()
    t = remove_stop_words(t)
    t = [get_lemma(w) for w in t]
    return t


def get_lemma(w):
    lemma = wn.morphy(w)
    return w if lemma is None else lemma


def remove_stop_words(tokens):
    stop_words = nltk.corpus.stopwords.words('english')
    return [token for token in tokens if token not in stop_words]


def get_outliers(label="positive for covid"):
    file = "/Storage/gnikou/sentiment_per_day/suspended_twitter_covid_sentiment.csv"  #
    df = pd.read_csv(file, sep='\t', index_col=False)
    q = df[label].quantile(0.97)
    d = df[df[label] > q]
    print(d)
    return d['day'].values.flatten().tolist()


def main():
    plt.rcParams['figure.figsize'] = [19.20, 10.80]
    client = pymongo.MongoClient(mongoConfig["address"])
    db = client[mongoConfig["db"]]
    labels_list = ['positive_for_covid', 'positive_for_conspiracy', 'positive_for_cases', 'negative_for_conspiracy',
                   'positive_for_lockdown', 'positive_for_vaccine', 'positive_for_conspiracy',
                   'positive_for_masks', 'positive_for_cases', 'positive_for_deaths', 'positive_for_propaganda',
                   'positive_for_5G', 'negative_for_covid', 'negative_for_lockdown', 'negative_for_vaccine',
                   'negative_for_conspiracy', 'negative_for_masks', 'negative_for_cases', 'negative_for_deaths',
                   'negative_for_propaganda', 'negative_for_5G']

    for label in labels_list:
        print(label)
        days = get_outliers(label)
        for date in days:
            get_text(db, parser.parse(date), label.replace("_", " "))
            lda_susp(db, label, date)


if __name__ == '__main__':
    main()
