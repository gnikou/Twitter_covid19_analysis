import re

"""
Remove emojis from text. List of emojis and clear text is returned
"""


def remove_url(text):
    text = re.sub(
        r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))''',
        " ", text)
    return text
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = [x[0] for x in re.findall(regex, text)]
    for link in url:
        text = text.replace(link, " ")

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
    clean_tweet_text = remove_url(tweet_text)
    clean_tweet_text = re.sub('[@#]', '', clean_tweet_text)
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
        tweet_text = merge_tw_rt(tweet_text,
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


def merge_tw_rt(tweet_text, retweet_text):
    if ": " not in tweet_text:
        print("Tweet:->{}\nRetweet:->{}\n\n".format(tweet_text, retweet_text))
        ind = 0
    else:
        ind = tweet_text.index(": ") + 2

    start_ind_tw = None
    start_ind_rt = None

    if len(tweet_text) - ind <= 6:
        if tweet_text[ind:] in retweet_text:
            start_ind_rt = retweet_text.index(tweet_text[ind:])
            start_ind_tw = ind
    else:
        for i in range(ind, len(tweet_text) - 6):
            if tweet_text[i:i + 6] in retweet_text:
                start_ind_rt = retweet_text.index(tweet_text[i:i + 6])
                start_ind_tw = i
                break
    if start_ind_tw != None:
        new = tweet_text[:start_ind_tw] + retweet_text[start_ind_rt:]
    else:
        print("No intersection between tw:{}\nrt:{}\n".format(tweet_text, retweet_text))
        new = tweet_text

    if "…" not in tweet_text and len(new) <= len(tweet_text):
        new = tweet_text
    return new
