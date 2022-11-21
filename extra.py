import re

"""
Remove emojis from text. List of emojis and clear text is returned
"""


def remove_url(tweet, text):

    for url in get_urls(tweet):
        text = text.replace(url, ' ')
    text = re.sub(
        r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))''',
        " ", text)
    return text


"""
remove double spaces from text
"""


def remove_double_space(text):
    while "  " in text:
        text = text.replace("  ", " ")
    return text


"""
Filter the tweet text from non-necessary characters 
"""


def fix_dots(text):
    text = text.replace("\\n", " ").replace("…", " ").replace("...", " ")
    text = re.sub('[\n\t:]', ' ', text)

    return remove_double_space(text)


def text_cleanup(tweet_text):
    clean_tweet_text = re.sub('[@#]', '', tweet_text)
    clean_tweet_text = fix_dots(clean_tweet_text)

    return clean_tweet_text


def legit_length(text):
    text = remove_double_space(text)
    words = [word for word in text.split(" ") if len(word) >= 1 and word[0] != "@"]
    score = len(words) / len("".join(words)) if len("".join(words)) != 0 else 0.0
    if 0.0 < score < 1.0:
        return True
    return False


def remove_rt(text):
    """Remove multiple RT's in text start"""
    while text.startswith("RT"):
        text = text[2:]
        while text.startswith(":") or text.startswith(" "):
            text = text[1:]

    if legit_length(text):
        text = text_cleanup(text)
        if legit_length(text):
            return text
    return None

def get_urls(tweet):
    urls = set([])
    tweet_portion = []
    if "extended_tweet" in tweet and "entities" in tweet["extended_tweet"]:
        tweet_portion = tweet["extended_tweet"]["entities"]["urls"]
    else:
        tweet_portion = tweet["entities"]["urls"]
    for pair in tweet_portion:
        urls.add(pair["url"])
    if "retweeted_status" in tweet:
        return  urls.union(get_urls(tweet["retweeted_status"]))
    return urls


# extract text field of twitter object
def get_text(tweet):
    
    if "extended_tweet" in tweet.keys() and "full_text" in tweet["extended_tweet"].keys():
        # case of extended tweet object
        return tweet['extended_tweet']['full_text']
    elif "retweeted_status" in tweet.keys() and "extended_tweet" in tweet["retweeted_status"].keys() and "full_text" \
            in tweet["retweeted_status"]["extended_tweet"].keys():
        # case of retweet object with extednded status
        return tweet['retweeted_status']['extended_tweet']['full_text']
    elif "retweeted_status" in tweet.keys() and "full_text" in tweet["retweeted_status"].keys():
        # case of retweetedd object with full_text
        return tweet['retweeted_status']['full_text']
    elif "retweeted_status" in tweet.keys():
        tweet_text = tweet["full_text"] if "full_text" in tweet else tweet["text"]
        tweet_text = merge_tw_rt(tweet, tweet_text,
                                 tweet["retweeted_status"]["full_text"] if "full_text" in
                                                                           tweet["retweeted_status"] else
                                 tweet["retweeted_status"]["text"])
        return tweet_text
    elif "full_text" in tweet.keys():
        # tweet object with full_text
        return tweet['full_text']
    elif "text" in tweet.keys():
        # case of simple text field in tweet object
        return tweet['text']
    return None


"""
Merge the original tweet text and retweet from Twitter object, in order to get full text in case if it possibl
"""


def merge_tw_rt(tweet, tweet_text, retweet_text):
    tweet_text = remove_url(tweet, tweet_text)
    retweet_text = remove_url(tweet, retweet_text)

    while tweet_text.startswith("RT"):
        ind = tweet_text.index(":") + 1 if ":" in tweet_text else tweet_text.index("RT") + 2
        tweet_text = tweet_text[ind:].strip()

    if "…" in tweet_text:
        tweet_text = tweet_text.replace("…", " ").strip()

    while retweet_text.startswith("RT"):
        ind = retweet_text.index(":") + 1 if ":" in retweet_text else retweet_text.index("RT") + 2
        retweet_text = retweet_text[ind:].strip()

    if "…" in retweet_text:
        retweet_text = retweet_text.replace("…", " ").strip()

    division = 3 if len(retweet_text) < 50 else 4

    ind = tweet_text.index(retweet_text[: int(len(retweet_text) / division)]) if retweet_text[: int(len(retweet_text) / division)] in tweet_text else -1
    if ind == 0:
        if len(retweet_text) > len(tweet_text):
            return retweet_text
        elif len(retweet_text) <= len(tweet_text):
            return tweet_text
    elif ind > 0:
        return tweet_text[: ind] + retweet_text
    elif ind < 0:
        return tweet_text
