from config import host, port, username, password, db_name, user_list
from config import politics_hashtag_list, politics_list, liberals_hashtag_list, liberals_list, labor_hashtags_list, 
    labor_list, greens_hashtags_list, greens_list, first_name, last_name
from better_profanity import profanity
from afinn import Afinn
from emoji import UNICODE_EMOJI
import time
import couchdb
import json
import os
import emojis

duplicate_count = 0

FOLDER = 'JSONS/'
FILE_NAME = 'JSONS/geoJsonSmall.json'
CURRENT_LOCATION = 'Melbourne'
BOUNDING_BOX = "Polygon"

def getPolitianDictionary():
    dict = {}
    for k, v in zip(first_name, last_name):
        dict[k] = v
    return dict



POLITICAN_DICT = getPolitianDictionary()

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


def get_tweet_and_save(tweet):
    global duplicate_count
    try:
        if server is None or db is None:
            initializedDB()
        tweet = get_enriched_data(tweet)
        if tweet is not None:
            db.save(tweet)


    except BaseException as e:
        print("Error on_data: %s" % str(e))


def load_from_file_to_db(fname):
    with open(fname, encoding='utf-8', mode='r') as json_file:
        data = json.loads(json_file.read())

    for tweet in data["features"]:
        print(tweet['properties'])
        get_tweet_and_save(tweet)


def get_enriched_data(data):
    print(data)
    doc = {}
    if data['properties']['text']:
        start = time.time()

        tweet = data['properties']['text']
        text_tokens = tweet.split()
        text_tokens = [x.lower() for x in text_tokens]

        doc['emojis'] = get_emojis(tweet)
        doc['contains_emojis'] = len(doc['emojis']) > 0
        doc['sentiment_score'] = sentiment_score(data['text'], data['lang'], doc['contains_emojis'])
        doc['tweet'] = tweet
        print(tweet)
        doc["vulgarity"] = is_vulgar(tweet)
        loc_type = data["geometry"]['type']
        if loc_type =="Point":
            doc["location"] = data["geometry"]['coordinates']
        # if loc_type == BOUNDING_BOX:
        #     coords = data["geometry"]['coordinates']
        #     x, y = zip(*coords)
        #     center = (max(x) + min(x)) / 2., (max(y) + min(y)) / 2.
        #     doc["location"] = center


        doc["geo"] = data['geo']
        doc["created_at"] = data['properties']['created_at']
        doc["location_name"] = data['properties']['location']
        # change manually
        doc["location_type"] = "City"
        
        doc["regular_stream"] = True
        doc["is_political"] = is_political(text_tokens)
        doc["is_liberals"] = is_liberals(text_tokens)
        doc["is_labor"] = is_labor(text_tokens)
        doc["is_greens"] = is_greens(text_tokens)
        if (doc["is_liberals"] or doc['is_labor'] or doc['is_greens']) == True:
            doc["is_political_general"] = True
        else:
            doc["is_political_general"] = is_general_political(text_tokens)

        end = time.time()
        print(end - start)
    return (doc)

def hashtags(list):
    print_list=[]
    for i in list:
        if i[0] != '#':
            i= '#'+i
            print_list.append(i)
    return(print_list)



def is_liberals(tokens):
    try:
        internal_list = liberals_list
        internal_list2 = hashtags(liberals_hashtag_list)
        return bool(set(tokens).intersection(internal_list2)) or bool(set(tokens).intersection(internal_list))
    except:
        return False


def is_labor(tokens):
    try:
        internal_list = labor_list
        internal_list2 = hashtags(labor_hashtags_list)
        return bool(set(tokens).intersection(internal_list2)) or bool(set(tokens).intersection(internal_list))
    except:
        return False


def is_greens(tokens):
    try:
        internal_list = greens_list
        internal_list2 = hashtags(greens_hashtags_list)
        return bool(set(tokens).intersection(internal_list2)) or bool(set(tokens).intersection(internal_list))
    except:
        return False


def is_general_political(tokens, liberal=False, labor=False, greens=False):
    try:
        internal_list = politics_list
        internal_list2 = hashtags(politics_hashtag_list)
        return bool(set(tokens).intersection(internal_list2)) or bool(set(tokens).intersection(internal_list))
    except:
        return False


def is_political(text_tokens):

    # The    tweet is a    candidate’s    retweet;
    # The    tweet    targets    at   least    one    candidate;
    # The    tweet    mentions    at    least    one    candidate;
    # The    tweet    has    a    candidate’s    proper    name;
    # https://jisajournal.springeropen.com/articles/10.1186/s13174-018-0089-0

    try:
            dict = POLITICAN_DICT
            matches = set(text_tokens).intersection(dict.keys())
            for match in matches:
                if dict[match] in set(text_tokens):
                    return True
            return False
    except:
        return False


def is_vulgar(text):
    return (profanity.contains_profanity(text))



def get_emojis(s):
    new_list= emojis.get(s)
    return list(new_list)



def sentiment_score(text, language="en", emo=False):
    try:
        afinn = Afinn(language=language, emoticons=emo)
        return (afinn.score(text))
    except:
        afinn = Afinn()
        return (afinn.score(text))


# enable this for single name
load_from_file_to_db(FILE_NAME)


# enable this folder full of json files
def file_runner():
    arr = os.listdir(FOLDER)
    for file in arr:
        if (file.split('.')[1] == 'json'):
            try:
                load_from_file_to_db(FOLDER + str(file))
            except Exception as e:
                print(file + ' is broken, make sure JSON download is 100% and format is right')
                pass


#file_runner()
