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
import emojis
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
from config import consumer_key,consumer_secret,access_token,access_token_secret
from config import host, port, username, password, db_name,user_list, first_name, last_name
from config import politics_hashtag_list, politics_list, liberals_hashtag_list, liberals_list,labor_hashtags_list, labor_list, greens_hashtags_list,greens_list
import json
import couchdb
from better_profanity import profanity
from afinn import Afinn
from emoji import UNICODE_EMOJI



duplicate_count = 0
current_count = 0

def getPolitianDictionary():
    dict = {}
    for k, v in zip(first_name, last_name):
        dict[k] = v
    return dict

POLITICAN_DICT = getPolitianDictionary()


def connect_to_couch_db_server():
    secure_remote_server = couchdb.Server('http://' + username + ':' + password + '@' + host + ':' + port)
    return secure_remote_server


def connect_to_database( server):
    try:
        return server[db_name]
    except:
        return server.create(db_name)


class MyListener(StreamListener):


    def __init__(self):
        self.server = connect_to_couch_db_server()
        self.db = connect_to_database(self.server)


    def on_data(self, data):
        global duplicate_count
        global current_count
        try:
            json_data = json.loads(data)
            MyDocId=json_data['id_str']
            temp_record=self.db.get(MyDocId)
            #Duplicate check
            if temp_record is not None:
                duplicate_count=  duplicate_count+1
                print("There was a duplicate tweet")
                print(duplicate_count)
                print("\n")
                return True
            json_data = get_enriched_data(json_data)
            if json_data is None:
                return True

            self.db.save(json_data)
            current_count = current_count +1
            print("\n**********************************\n")
            print("Current tweet count: ")
            print(current_count)
            print("\n**********************************\n")
            return True

        except BaseException as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):
        print(status)
        return True


def runner():

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    twitter_stream = Stream(auth, MyListener())
    mel = [144.59,-38.43,145.51,-37.51]
    syd = [150.52,-34.12,151.34,-33.58]
    vic = [140.96,-39.18,144.04,-33.98,144.04,-39.16,149.98,-35.91]
    twitter_stream.filter(locations=vic)


def get_enriched_data(data):
    doc={}
    if data['text']:
        doc['_id']=data['id_str']
        doc['user_name'] = data['user']['screen_name']
        doc['emojis'] = get_emojis(data['text'])
        doc['contains_emojis'] = len(doc['emojis']) > 0
        doc['sentiment_score']=sentiment_score(data['text'], data['lang'])
        doc['tweet']=data['text']
        print(data['text'])
        doc["vulgarity"]=is_vulgar(data['text'])
        doc["location"]=data['coordinates']
        doc["geo"]=data['geo']
        doc["location_name"]=data['place']['name']
        doc["location_type"]=data['place']['place_type']
        doc["hashtags"]=data['entities']['hashtags']
        doc["retweet_count"]=data['retweet_count']
        doc["likes"] = data['favorite_count']
        doc["followers_count"] = data['user']['followers_count']
        doc["date_created"] = data['created_at']
        doc["is_leader"] = is_leader(doc["user_name"])
        doc["regular_stream"]=True
        text_tokens = data['text'].split()
        text_tokens = [x.lower() for x in text_tokens]
        doc["hashtags"] = [x['text'].lower() for x in doc["hashtags"]]
        doc["is_political"] = is_political(text_tokens, data['entities']['user_mentions'], data['user']['screen_name'])
        doc["is_liberals"] = is_liberals(text_tokens, doc["hashtags"])
        doc["is_labor"] = is_labor(text_tokens, doc["hashtags"])
        doc["is_greens"] = is_greens(text_tokens, doc["hashtags"])
        if (doc["is_liberals"] or doc['is_labor'] or doc['is_greens']) == True:
            doc["is_political_general"] = True
        else:
            doc["is_political_general"] =  is_general_political(text_tokens, doc["hashtags"])
        doc["mentions"] = data['entities']['user_mentions']
    return (doc)


def is_liberals(tokens, hashtags):
    try:
        internal_list = liberals_list
        internal_list2 = liberals_hashtag_list
        return bool(set(hashtags).intersection(internal_list2)) or bool(set(tokens).intersection(internal_list))
    except:
        return False


def is_labor(tokens, hashtags):
    try:
        internal_list = labor_list
        internal_list2 = labor_hashtags_list
        return bool(set(hashtags).intersection(internal_list2)) or bool(set(tokens).intersection(internal_list))
    except:
        return False


def is_greens(tokens, hashtags):
    try:
        internal_list = greens_list
        internal_list2 = greens_hashtags_list
        return bool(set(hashtags).intersection(internal_list2)) or bool(set(tokens).intersection(internal_list))
    except:
        return False


def is_general_political(tokens, hashtags):
    try:
        return hashtags in politics_hashtag_list[0] or tokens in politics_list[0]
    except:
        return False
    

def is_political(text_tokens, user_mentions, user_screen_name):
    try:
        if user_mentions is not None:
            for user in user_mentions:
                if user is not None:
                    mentions_screen_name = user['screen_name'].lower()
                    if ((mentions_screen_name in user_list) or (user_screen_name.lower in user_list)):
                        return True

            dict =POLITICAN_DICT
            matches = set(text_tokens).intersection(dict.keys())
            for match in matches:
                if dict[match] in set(text_tokens):
                    return True
            return False
    except:
        return False


def is_leader(user):
    return user in user_list


def is_vulgar(text):
    return(profanity.contains_profanity(text))


def get_emojis(s):
    new_list= emojis.get(s)
    return list(new_list)


def sentiment_score(text, language="en", emo=False):
    try:
        emo = is_emoji(text)
        afinn = Afinn(language=language, emoticons=emo)
        return(afinn.score(text))
    except:
        afinn = Afinn()
        return(afinn.score(text))

runner()
