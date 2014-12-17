import sqlite3
from twython import Twython

#connect to DB
conn = sqlite3.connect("twitter.db")
cursor = conn.cursor()

cursor.execute("""CREATE TABLE user (
id INT,
screen_name VARCHAR(15),
name VARCHAR(20),
description TEXT,
location VARCHAR(50),
url VARCHAR(50),
friends_count INT,
followers_count INT,
statuses_count INT,
lang VARCHAR(5),
geo_enabled BOOL,
twitter_date_created SHORTDATETIME,
date_created SHORTDATETIME,
date_last_modified SHORTDATETIME,
id_str VARCHAR(15),
date_sync SHORTDATETIME
) """)

cursor.execute("CREATE TABLE follower (user_id INT, follower_id INT)")

cursor.execute("CREATE INDEX id ON user(id)")
cursor.execute("CREATE INDEX user_id ON follower(user_id)")
cursor.execute("CREATE INDEX follower_id ON follower(follower_id)")
conn.commit()
