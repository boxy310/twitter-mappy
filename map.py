import sqlite3
import sys
import time
import pprint
from twython import Twython, TwythonError, TwythonRateLimitError
from urllib.error import URLError
from http.client import BadStatusLine
from operator import itemgetter
from math import log1p as ln

from keys import *
from testusers import *

class DBConnect:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cursor = self.conn.cursor()
    def sql(self, sql, *args):
        self.cursor.execute(sql, *args)
        self.conn.commit()
    def query(self, query):
        self.sql(query, rowlimit=1000)
        self.cursor.fetchall()[0:rowlimit]
    def bulkload(self, table, values):
        for row in values:
            self.sql("INSERT INTO "+table+" VALUES ("+','.join(row)+")")
        self.conn.commit()

class TwitterConnect(Twython):
    def partition(self, uids, n):
        parts = []
        while (len(uids) > n):
            parts.append(uids[0:n])
            del uids[0:n]
        parts.append(uids)
        return parts
    def parse_user(self, json):
        uid = str(json['id'])
        screen_name = str(json['screen_name'])
        name = str(json['name']).replace("'", "''")
        description = str(json['description']).replace("'", "''")
        location = str(json['location']).replace("'", "''")
        url = (" '"+ str(json['url']).replace("'", "''") +"' ").replace("'None'", "NULL")
        friends_count = str(json['friends_count'])
        followers_count = str(json['followers_count'])
        statuses_count = str(json['statuses_count'])
        lang = str(json['lang'])
        geo_enabled = str(json['geo_enabled'])
        twitter_date_created = str(json['created_at'])
        date_created = "DATETIME(CURRENT_TIMESTAMP, 'localtime')"
        date_last_modified = "DATETIME(CURRENT_TIMESTAMP, 'localtime')"
        id_str = str(json['id_str'])
        date_sync = "NULL"
        parse_array = [uid, screen_name, name, description, location, url, friends_count, followers_count, statuses_count, lang, geo_enabled, twitter_date_created, date_created, date_last_modified, id_str, date_sync]
        return parse_array
    def helper_part(self, uids, CallFn, ParseFn, n=100, file='collect.tmp'):
        uids = self.partition(uids,n)
        tbl = []
        for batch in uids:
            call = CallFn(batch)
            tbl.extend(list(map(ParseFn, call)))
        return tbl
    def user_demo_part(self, uids):
        call = self.lookup_user(user_id = ','.join(map(str,uids)))
        return call
    def user_demo(self, uids):
        return self.helper_part(uids, self.user_demo_part, self.parse_user)
    def followers (self, uid):
        cursor = -1
        followers = []
        while (cursor != 0):
            call = self.get_followers_ids(user_id = uid, cursor = cursor)
            followers.extend(call['ids'])
            cursor = call['next_cursor']
        return followers

db = DBConnect('twitter.db')
tw = TwitterConnect(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

class Mappr:
    def __init__(self, uid, db=db, tw=tw):
        self.uid = uid
        self.db = db
        self.tw = tw
    def load_demos(self, uids):
        demos = tw.user_demo(uids)
        self.db.bulkload('user', demos)
    def get_followers(self, uid):
        def helper_fn(self, uid, follower):
            return [uid, follower]
        fols = tw.followers(uid)
        folsmap = list(map(helper_fn, fols))
        self.db.bulkload('follower', folsmap)
    def trunc_exist_users(self, uids):
        exist_users = self.db.sql("SELECT id FROM user WHERE id IN ("+','.map(str,uids)+")")
        new_users = [x for x in uids if x not in exist_users]
        return new_users
        
            





