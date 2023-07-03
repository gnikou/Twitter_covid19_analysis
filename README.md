## An experimental analysis of Twitter suspension during the first COVID period

#### Analyzing COVID-19 Tweets: Sentiment and Data Analysis and a study of the tweets written by suspended users

The thesis is the study of a large Twitter dataset about COVID19 during the pandemic outbreak. This study can be break down to these parts:

* Parsed, cleaned, and stored a dataset of ∼200M covid-related tweets from JSON format to a MongoDB
database
* Implemented sentiment analysis with machine learning using the Transformer-based model ”XLM-RoBERTa”
* Conducted data analysis to gain more insights into this large dataset on statistics metrics such as daily tweet
volume, most prominent languages, most popular hashtags etc.
* Filtered the database through Twitter API to exclusively retrieve suspended accounts. Then I implemented the Latent
Dirichlet Allocation algorithm to analyze the tweets' content to discover the topics that were discussing
* Created retweet graphs for suspended users searching for patterns and communities that that potentially engaged in coordinated actions
