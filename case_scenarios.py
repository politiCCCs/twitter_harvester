# ============= COMP90024 - Assignment 2 ============= #
#                               
# The University of Melbourne           
# Team 5
#
# ** Authors: **
# 
# Aleksandar Pasquini (912504)
# Amelia Fleischer-Boermans (389511)
# Isaac Daly (1129173)
# Mahardini Rizky Putri (921790)
# Richard Yang (1215150)
# 
# Location: Melbourne
# ==================================================== 

import tweepy
from tweepy import OAuthHandler
from better_profanity import profanity
from afinn import Afinn
from config import consumer_key, consumer_secret, access_token, access_token_secret
from config import host, port, username, password, db_name,  labor_mp, liberal_mp, green_mp, user_list
import couchdb
import time
import datetime
import json
import emojis



def connect_to_couch_db_server():
    secure_remote_server = couchdb.Server('http://' + username + ':' + password + '@' + host + ':' + port)
    return secure_remote_server


def connect_to_database(server):
    try:
        return server[db_name]
    except:
        return server.create(db_name)


server = connect_to_couch_db_server()
db = connect_to_database(server)


def initializedDB():
    server = connect_to_couch_db_server()
    db = connect_to_database(server)


def run_batch_for_all_users():
    invalidator = ["-", "Twitter Username", " "]
    count_of_users = 0
    for user in user_list:
        try:
            if user not in invalidator and user != None:
                count_of_users = count_of_users + 1
                print("current user")
                print(user)
                print("\n")
                if user[0] != '@':
                    user = '@' + user
                    get_tweets_and_save(user)
                else:
                    get_tweets_and_save(user)
        except:
            print("user not found {}".format(user))
            continue



def get_tweets_and_save(user):
    duplicate_count = 0
    tweets = custom_runner(user)
    for tweet in tweets:
        try:
            if server is None or db is None:
                initializedDB()
            MyDocId = tweet.id_str
            tweet = json.dumps(tweet._json)
            tweet = json.loads(tweet)

            temp_record = db.get(MyDocId)
            # Duplicate check
            if temp_record is not None:
                duplicate_count = duplicate_count + 1
                print("There was a duplicate tweet")
                print(duplicate_count)
                print("\n")
                data = tweet['doc']
                doc = temp_record
                doc['emojis'] = get_emojis(data['text'])
                doc['contains_emojis'] = len(doc['emojis']) > 0
                doc['sentiment_score'] = sentiment_score(data['text'], data['lang'], doc['contains_emojis'])
                db.save(doc)
                print("\n")
            else:
                tweet = get_enriched_data(tweet)
                if tweet is not None:
                    db.save(tweet)



        except BaseException as e:
            print("Error on_data: %s" % str(e))


def custom_runner(id):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    # client = tweepy.Client(auth)
    startDate = datetime.datetime(2011, 6, 1, 0, 0, 0)
    endDate = datetime.datetime(2022, 1, 1, 0, 0, 0)

    tweets = []
    # fetching the user
    user = api.get_user(id)

    # fetching the statuses_count attribute
    statuses_count = user.statuses_count
    print(statuses_count)
    print('***********************************')
    try:
        # initialize a list to hold all the tweepy Tweets
        alltweets = []

        # make initial request for most recent tweets (200 is the maximum allowed count)
        new_tweets = api.user_timeline(screen_name=id, count=200)

        # save most recent tweets
        alltweets.extend(new_tweets)

        oldest = alltweets[-1].id - 1

        if (alltweets is not None and len(alltweets) == 200):
            # keep grabbing tweets until there are no tweets left to grab
            while len(new_tweets) > 0:
                print(f"getting tweets before {oldest}")

                # all subsequent requests use the max_id param to prevent duplicates
                new_tweets = api.user_timeline(screen_name=id, count=200, max_id=oldest)

                # save most recent tweets
                alltweets.extend(new_tweets)

                # update the id of the oldest tweet less one
                oldest = alltweets[-1].id - 1

    except:
        pass

    return alltweets





def get_enriched_data(data):
    print(data)
    doc = {}
    if data['text']:
        doc['_id'] = data['id_str']
        doc['user_name'] = data['user']['screen_name']
        doc['emojis'] = get_emojis(data['text'])
        doc['contains_emojis'] = len(doc['emojis']) > 0
        doc['sentiment_score'] = sentiment_score(data['text'], data['lang'], doc['contains_emojis'])
        doc['tweet'] = data['text']
        doc["vulgarity"] = is_vulgar(data['text'])
        doc["location"] = data['coordinates']
        doc["geo"] = data['geo']
        doc["hashtags"] = data['entities']['hashtags']
        doc["retweet_count"] = data['retweet_count']
        doc["likes"] = data['favorite_count']
        doc["followers_count"] = data['user']['followers_count']
        doc["date_created"] = data['created_at']
        doc["is_leader"] =True
        doc["regular_stream"] = False
        doc["is_political"] = is_political(data['text'], data['entities']['user_mentions'])
        doc["is_political_general"] = is_general_political(doc["tweet"], doc["hashtags"])
        doc["is_liberals"] = is_liberals(data['user']['screen_name'])
        print("user_screen_name")
        print(data['user']['screen_name'])
        doc["is_labor"] = is_labor(data['user']['screen_name'])
        doc["is_greens"] = is_greens(data['user']['screen_name'])
    return (doc)

def is_liberals(user):
    return user in liberal_mp

def is_labor(user):
    return user in labor_mp

def is_greens(user):
    return user in green_mp

def is_general_political(text, hashtags):
    # returns true as quoted by political candidate
    return True


def is_political(text, user_mentions):
    # returns true as quoted by political candidate
    return True


def is_vulgar(text):
    return (profanity.contains_profanity(text))


def get_emojis(s):
    new_list= emojis.get(s)
    return list(new_list)


def sentiment_score(text, language="en", emo=False):
    try:
        afinn = Afinn(language=language, emoticons=emo)
        return(afinn.score(text))
    except:
        afinn = Afinn()
        return(afinn.score(text))


run_batch_for_all_users()


