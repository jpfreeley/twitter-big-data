import json
from mysql import connector
import mysql.connector
from mysql.connector import errorcode
import config
from dateutil import parser
import re

emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs

    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]

tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)

def tokenize(s):
    return tokens_re.findall(s)

def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens

class Db():
    def __init__(self):
        """Creates necessary db and table if they don't exist"""
        self.create_table()

    def parse_tweet(self, json_str):
        t = json.loads(json_str)
        tweet_id = str(t['id_str'])
        user_id = str(t["user"]["id_str"])
        screen_name = str(t['user']['screen_name'])
        created_at = parser.parse(t['created_at']).strftime('%Y-%m-%d %H:%M:%S')
        lang = t['lang']
        text = t['text'].encode('utf8','ignore')
        text = preprocess(text)
        return (lang, tweet_id, user_id, screen_name, created_at, text)


    def store_tweet(self, json_str):
        db=mysql.connector.connect(host=config.HOST, user=config.USER, passwd=config.PASSWD, db=config.DATABASE, charset="utf8")
        cursor = db.cursor()
        lang, tweet_id, user_id, screen_name, created_at, text = self.parse_tweet(json_str)
        if lang == "en":
            insert_query = ('INSERT INTO %s (tweet_id, user_id, screen_name, created_at, text) VALUES ("%s", "%s", "%s", "%s", "%s")' %(config.TABLE, tweet_id, user_id, screen_name, created_at, text))
            cursor.execute(insert_query)
            db.commit()
            cursor.close()
            db.close()
            print('SUCCESS: '+insert_query)
        return

    def create_database(self, cursor):
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(config.DATABASE))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def create_table(self):
        db = mysql.connector.connect(host=config.HOST, user=config.USER, passwd=config.PASSWD, charset='utf8')
        cursor = db.cursor()

        # create database if database doesn't exist
        try:
            db.database = config.DATABASE
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database(cursor)
                db.database = config.DATABASE
            else:
                print(err)
                exit(1)

        # create table if table doesn't exist
        ddl = "CREATE TABLE `%s` (          `id` int(11) NOT NULL AUTO_INCREMENT,          `tweet_id` varchar(250) DEFAULT NULL,          `user_id` varchar(128) DEFAULT NULL,         `screen_name` varchar(128) DEFAULT NULL,          `created_at` timestamp NULL DEFAULT NULL,          `text` text,          PRIMARY KEY (`id`)         ) AUTO_INCREMENT=56 DEFAULT CHARSET=utf8;" %(config.TABLE)
        try:
            print("Checking if table '%s' exists: "%(config.TABLE))
            cursor.execute(ddl)
            print("table created.")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        db.commit()
        cursor.close()
        db.close()
