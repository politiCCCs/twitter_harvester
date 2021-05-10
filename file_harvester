from config import host, port, username, password, db_name,user_list
from config import politics_hashtag_list, politics_list, liberals_hashtag_list, liberals_list,labor_hashtags_list, labor_list, greens_hashtags_list,greens_list
from better_profanity import profanity
from afinn import Afinn
from emoji import UNICODE_EMOJI
import time
import couchdb
import json
import os

duplicate_count = 0

FOLDER ='JSONS/'
FILE_NAME = 'JSONS/tinyTwitter.json'

def connect_to_couch_db_server():
    secure_remote_server = couchdb.Server('http://' + username + ':' + password + '@' + host + ':' + port)
    return secure_remote_server


def connect_to_database( server):
    try:
        return server[db_name]
    except:
        return server.create(db_name)


server = connect_to_couch_db_server()
db = connect_to_database(server)

def initializedDB():
    server = connect_to_couch_db_server()
    db = connect_to_database(server)

def get_tweet_and_save(tweet):
    global duplicate_count
    try:
        if server is None or db is None:
            initializedDB()
        MyDocId = tweet["id"]
        temp_record = db.get(MyDocId)
        # Duplicate check
        if temp_record is not None:
            duplicate_count = duplicate_count + 1
            print("There was a duplicate tweet")
            print(duplicate_count)
            print("\n")
        else:
            tweet = get_enriched_data(tweet['doc'])
            if tweet is not None:
                db.save(tweet)

    except BaseException as e:
        print("Error on_data: %s" % str(e))



def load_from_file_to_db(fname):
    with open(fname,encoding='utf-8',mode='r') as json_file:
        data = json.loads(json_file.read())

    for tweet in data['rows']:
        print(tweet['doc'])
        get_tweet_and_save(tweet)


def get_enriched_data(data):
    print(data)
    doc={}
    if data['text']:
        start = time.time()

        doc['_id']=data['id_str']
        doc['user_name'] = data['user']['screen_name']
        doc['emojis'] = is_emoji(data['text'])
        doc['sentiment_score']=sentiment_score(data['text'], data['lang'],doc['emojis'])
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
        doc["is_political"]=is_political(text_tokens,data['entities']['user_mentions'],data['user']['screen_name'])
        doc["is_political_general"] = is_general_political(doc["tweet"], doc["hashtags"])
        doc["is_liberals"] = is_liberals(text_tokens, doc["hashtags"])
        doc["is_labor"] = is_labor(text_tokens, doc["hashtags"])
        doc["is_greens"] = is_greens(text_tokens, doc["hashtags"])

        end = time.time()
        print(end - start)
    return (doc)


def is_liberals(tokens, hashtags):

    try:
        result = hashtags in liberals_hashtag_list or tokens in liberals_list
        return result[0]
    except:
        return False

def is_labor(tokens, hashtags):
    try:
        return hashtags in labor_hashtags_list or tokens in labor_list[0]
    except:
        return False

def is_greens(tokens, hashtags):
    try:
        return hashtags in greens_hashtags_list or tokens in greens_list[0]
    except:
        return False


def is_general_political(tokens, hashtags):
    try:
        return hashtags in politics_hashtag_list[0] or tokens in politics_list[0]
    except:
        return False

def is_political(text, user_mentions,user_screen_name):

    # The    tweet is a    candidate’s    retweet;
    # The    tweet    targets    at   least    one    candidate;
    # The    tweet    mentions    at    least    one    candidate;
    # The    tweet    has    a    candidate’s    proper    name;
    # https://jisajournal.springeropen.com/articles/10.1186/s13174-018-0089-0

    try:
        if user_mentions is not None:
            for user in user_mentions:
                if user is not None:
                    if((user['screen_name'],user_screen_name in user_list)[1]):
                        return True
        return False
    except:
        return False

def is_leader(user):
    return user in user_list

def is_vulgar(text):
    return(profanity.contains_profanity(text))


def is_emoji(s):
    return s in UNICODE_EMOJI


def sentiment_score(text, language="en", emo=False):
    try:
        afinn = Afinn(language=language, emoticons=emo)
        return(afinn.score(text))
    except:
        afinn = Afinn()
        return(afinn.score(text))

# enable this for single name
#load_from_file_to_db(FILE_NAME)


# enable this folder full of json files
def file_runner():
    arr = os.listdir(FOLDER)
    for file in arr:
        if(file.split('.')[1]=='json'):
            try:
                load_from_file_to_db(FOLDER+str(file))
            except Exception as e:
                print(file + ' is broken, make sure JSON download is 100% and format is right')
                pass
file_runner()
