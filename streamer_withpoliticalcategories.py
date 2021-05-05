import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
from config import consumer_key,consumer_secret,access_token,access_token_secret
from config import host, port, username, password, db_name,user_list,politics_list,politics_hashtag_list,labor_list,labor_hashtags_list,greens_list,greens_hashtags_list,liberals_hashtag_list,liberals_list
import json
import couchdb
from better_profanity import profanity
from afinn import Afinn
from emoji import UNICODE_EMOJI

duplicate_count=0

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
        duplicate_count = 0

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
                return False
            json_data = get_enriched_data(json_data)
            if json_data is None:
                return False
            self.db.save(json_data)
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
    aus = [113.338953078, -43.6345972634, 153.569469029, -10.6681857235]
    mel = [144.59,-38.43,145.51,-37.51]
    syd = [150.52,-34.12,151.34,-33.58]
    vic = [140.96,-39.18,144.04,-33.98,144.04,-39.16,149.98,-35.91]
    twitter_stream.filter(locations=aus)

def get_enriched_data(data):
    print(data)
    doc={}
    if data['text']:
        doc['_id']=data['id_str']
        doc['emojis'] = get_emojis(data['text'])
        doc['sentiment_score'] = sentiment_score(data['text'], data['lang'], len(doc['emojis']) > 0)
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
        doc["date_created"]=data['created_at']
        doc["regular_stream"]=True
        # 100% confidence
        doc["is_political"] = is_political(data['text'], data['entities']['user_mentions'], data['user']['screen_name'])
        # This is research based and subject to perception
        doc["is_political_general"] = is_general_political(doc["tweet"], doc["hashtags"])
        doc["is_liberals"] = is_liberals(doc["tweet"], doc["hashtags"])
        doc["is_labor"] = is_labor(doc["tweet"], doc["hashtags"])
        doc["is_greens"] = is_greens(doc["tweet"], doc["hashtags"])
        doc["full_tweet"] = data
    return(doc)


def is_liberals(text, hashtags):
    return hashtags in liberals_hashtag_list or text.split() in liberals_list

def is_labor(text, hashtags):
    return hashtags in labor_hashtags_list or text.split() in labor_list

def is_greens(text, hashtags):
    return hashtags in greens_hashtags_list or text.split() in greens_list

def is_general_political(text, hashtags):
    return hashtags in politics_hashtag_list or text.split() in politics_list


def is_political(text, user_mentions,user_screen_name):
    # The    tweet is a    candidate’s    retweet;
    # The    tweet    targets    at   least    one    candidate;
    # The    tweet    mentions    at    least    one    candidate;
    # The    tweet    has    a    candidate’s    proper    name ;
    # https://jisajournal.springeropen.com/articles/10.1186/s13174-018-0089-0

    if user_mentions is not None:
        for user in user_mentions:
            if user is not None:
                if((user['screen_name'],user_screen_name in user_list)[1]):
                    return True
    return False



def is_vulgar(text):
    return(profanity.contains_profanity(text))


def get_emojis(s):
    return ''.join(c for c in s if c in UNICODE_EMOJI)


def sentiment_score(text, language="en", emo=False):
    try:
        afinn = Afinn(language=language, emoticons=emo)
        return(afinn.score(text))
    except:
        afinn = Afinn()
        return(afinn.score(text))


runner()
