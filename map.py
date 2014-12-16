import sys
import time
import sqlite3
import pprint
import csv
from operator import itemgetter
from math import log1p as ln

from keys import *
from testusers import *
from twitterconnect import *

class DBConnect:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)
        self.cursor = self.conn.cursor()
    def sql(self, sql, *args):
        self.cursor.execute(sql, *args)
        self.conn.commit()
    def query(self, query, rowlimit=1000):
        self.sql(query)
        return self.cursor.fetchall()[0:rowlimit]
    def bulkload(self, table, values):
        for row in values:
            self.sql("INSERT INTO "+table+" VALUES ("+','.join(list(map(str,row)))+")")
        self.conn.commit()

db = DBConnect('twitter.db')
tw = TwitterConnect(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

class Mappr:
    def __init__(self, uid, db=db, tw=tw):
        self.uid = uid
        self.db = db
        self.tw = tw
        self.fols = self.load_followers(uid)
    def load_demos(self, uids):
        uids = self.trunc_exist_users(uids)
        self.db.sql("DELETE FROM user WHERE id IN ("+','.join(list(map(str,uids)))+")")
        demos = tw.user_demo(uids)
        self.db.bulkload('user', demos)
    def get_followers(self, uid):
        self.db.sql("DELETE FROM follower WHERE user_id = "+str(uid))
        fols = tw.followers(uid)
        parse_fols = tw.parse_follower(uid, fols)
        self.db.bulkload('follower', parse_fols)
        self.fols.extend(fols)
    def bulk_followers(self, uids):
        uids = self.trim_users_size(uids)
        n_uids = len(uids)
        for i in range(n_uids):
            print ("Pulling followers for follower %i of %i (%i%% complete)..." % (i+1, n_uids, ((100*(i+1)//n_uids)))
            self.get_followers(uids[i])
    def load_followers(self, uid):
        rawlist = self.db.query("SELECT follower_id FROM follower WHERE user_id = "+str(uid))
        parselist = []
        for item in rawlist:
            parselist.append(item[0])
        return parselist
    def trim_users_size(self, uids):
        raw_trim = self.db.query("SELECT id FROM user WHERE followers_count BETWEEN 100 AND 10000 AND id IN ("+','.join(list(map(str,uids)))+")")
        parselist = []
        for item in raw_trim:
            parselist.append(item[0])
        return parselist
    def trunc_exist_users(self, uids):
        def trunc_helper(x):
            return x[0]
        exist_users = self.db.query("SELECT id FROM user WHERE id IN ("+','.join(list(map(str,uids)))+")")
        exist_users = list(map(trunc_helper, exist_users))
        new_users = [x for x in uids if x not in exist_users]
        return new_users
    def list_users(self, uids, order='followers_count DESC'):
        q = self.db.query("""
SELECT id, screen_name, name, description, location, followers_count
FROM user
WHERE id IN ("""+','.join(list(map(str,uids)))+") ORDER BY "+order)
        return q
    def save_query(self, q, file):
        with open(file, 'w', encoding='utf-16', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in q:
                writer.writerow(row)

        
mappy = Mappr(andrew)
