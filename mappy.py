import sqlite3
import sys
import time
import pprint
from twython import Twython, TwythonError, TwythonRateLimitError
from urllib.error import URLError
from http.client import BadStatusLine
from operator import itemgetter
from math import log1p as ln

#connect to twitter
def ConnectTwitter():
    APP_KEY = 'XXX'
    APP_SECRET = 'XXX'

    OAUTH_TOKEN = 'XXX'
    OAUTH_TOKEN_SECRET = 'XXX'
  # TODO: Implement tokens as separate file that's excluded from git repo

    twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    return twitter

# wrap Twitter API in a robust error-handling call
def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw): 
    
    # A nested helper function that handles common HTTPErrors. Return an updated
    # value for wait_period if the problem is a 500 level error. Block until the
    # rate limit is reset if it's a rate limiting issue (429 error). Returns None
    # for 401 and 404 errors, which requires special handling by the caller.
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
    
        if wait_period > 3600: # Seconds
            print ('Too many retries. Quitting.', file=sys.stderr)
            raise e
    
        # See https://dev.twitter.com/docs/error-codes-responses for common codes
    
        if e.error_code == 401:
            print ('Encountered 401 Error (Not Authorized)', file=sys.stderr)
            return None
        elif e.error_code == 404:
            print ('Encountered 404 Error (Not Found)', file=sys.stderr)
            return None
        elif e.error_code == 429: 
            print ('Encountered 429 Error (Rate Limit Exceeded)', file=sys.stderr)
            if sleep_when_rate_limited:
                print ("Retrying in 5 minutes...ZzZ...", file=sys.stderr)
                sys.stderr.flush()
                time.sleep(60*5 + 5) # modified to be a 5 minute wait (also above)
                print ('...ZzZ...Awake now and trying again.', file=sys.stderr)
                return 2
            else:
                raise e # Caller must handle the rate limiting issue
        elif e.error_code in (500, 502, 503, 504):
            print ('Encountered %i Error. Retrying in %i seconds' % \
                (e.e.code, wait_period), file=sys.stderr)
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise e

    # End of nested helper function
    
    wait_period = 2 
    error_count = 0 

    while True:
        try:
            return twitter_api_func(*args, **kw)
        except TwythonError as e:
            # original: twitter.api.TwitterHTTPError
            # note that Twython has TwythonError (generic) and TwythonRateLimit for 429 code (must still be imported)
            error_count = 0 
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except TwythonRateLimitError as e:
            error_count = 0
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except URLError as e:
            error_count += 1
            print >> sys.stderr, "URLError encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise
        except BadStatusLine as e:
            error_count += 1
            print >> sys.stderr, "BadStatusLine encountered. Continuing."
            if error_count > max_errors:
                print >> sys.stderr, "Too many consecutive errors...bailing out."
                raise

#load user data
def LoadUserDemo(uids): # note this has changed to be an array of uids
    cursor.execute("DELETE FROM user WHERE id IN ("+','.join(list(map(str,uids)))+")")
    me = make_twitter_request(twitter.lookup_user, user_id = ','.join(list(map(str,uids))))
    cursor.execute("BEGIN TRANSACTION")
    for i in range(len(me)):
        print ("Loading demographics data for user "+ str(me[i]['id']) +"...")
        cursor.execute("INSERT INTO user VALUES (" + str(me[i]['id']) +", '"+ str(me[i]['screen_name'])+"', '"+ str(me[i]['name']).replace("'", "''") +"', '"+ str(me[i]['description']).replace("'", "''") +"', '"+ str(me[i]['location']).replace("'", "''") +"', "+ (" '"+ str(me[i]['url']).replace("'", "''") +"' ").replace("'None'", "NULL") +", "+ str(me[i]['friends_count'])+", "+ str(me[i]['followers_count'])+", "+ str(me[i]['statuses_count'])+", '"+ str(me[i]['lang'])+"', '"+ str(me[i]['geo_enabled'])+"', '"+ str(me[i]['created_at'])+ "', DATETIME(CURRENT_TIMESTAMP, 'localtime'), DATETIME(CURRENT_TIMESTAMP, 'localtime'), '"+ str(me[i]['id_str']) +"', NULL)")
    cursor.execute("END TRANSACTION")
    # note that the Twitter API can handle up to 100 ID lookups per call

# from O'Reilly's "Mining the Social Web":
# You can get around the 5,000 followers/friends API call limit
# by "walking the cursor of results to systematically fetch all of these ids."

#load key data for list of followers (limit to 25 per call)
def LoadFollowersDemo(followers): #probably deprecated -- UserDemo call can handle multiples now
    apicall = 0
    for i in range(len(followers)):
        uid = followers[i][0]
        cursor.execute("SELECT EXISTS(SELECT id FROM user WHERE id = "+str(uid)+")")
        if (cursor.fetchall()[0][0] == 0):
            print ("Loading follower demographics for user "+ str(uid) +"...")
            me = twitter.lookup_user(user_id = uid)
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("INSERT INTO user VALUES (" + str(me[0]['id']) +", '"+ str(me[0]['screen_name'])+"', '"+ str(me[0]['name']).replace("'", "''") +"', '"+ str(me[0]['description']).replace("'", "''") +"', '"+ str(me[0]['location']).replace("'", "''") +"', "+ (" '"+ str(me[0]['url']).replace("'", "''") +"' ").replace("'None'", "NULL") +", "+ str(me[0]['friends_count'])+", "+ str(me[0]['followers_count'])+", "+ str(me[0]['statuses_count'])+", '"+ str(me[0]['lang'])+"', '"+ str(me[0]['geo_enabled'])+"', '"+ str(me[0]['created_at'])+ "', DATETIME(CURRENT_TIMESTAMP, 'localtime'), DATETIME(CURRENT_TIMESTAMP, 'localtime'), '"+ str(me[0]['id_str']) +"', NULL)")
            cursor.execute("END TRANSACTION")
            apicall += 1
            if (apicall >= 100):
                print ("Max API calls per function reached. Function has halted.")
                return

#load followers data
def LoadFollowers(uids):
    for uid in uids: #returned followers object isn't populated with a uid. Must call separately.
        query = make_twitter_request(twitter.get_followers_ids, user_id = uid)
        if query.get('ids'):
            followers = query['ids']
            cursor.execute("DELETE FROM follower WHERE user_id = "+ str(uid))
            cursor.execute("BEGIN TRANSACTION")
            for i in range(len(followers)):
                #cursor.execute("SELECT COUNT(*) FROM follower WHERE user_id = "+ str(uid) +" AND follower_id = "+ str(followers[i]))
                #if (cursor.fetchall()[0][0] == 0):            
                cursor.execute("INSERT INTO follower VALUES (" + str(uid) +", "+str(followers[i])+", NULL)")
            cursor.execute("END TRANSACTION")
            #print ("Inserting user data into 'follower' table...")

#load following ("friends") data
def LoadFollowing(uids):
    for uid in uids:
        query = make_twitter_request(twitter.get_friends_ids, user_id = uid)
        if query.get('ids'):
            following = query['ids']
            cursor.execute("DELETE FROM follower WHERE follower_id = "+ str(uid))
            cursor.execute("BEGIN TRANSACTION")
            for i in range(len(following)):
                #cursor.execute("SELECT COUNT(*) FROM follower WHERE user_id = "+ str(following[i]) +" AND follower_id = "+ str(uid))
                #if (cursor.fetchall()[0][0] == 0):
                cursor.execute("INSERT INTO follower VALUES (" + str(following[i]) +", "+str(uid)+", NULL)")
            cursor.execute("END TRANSACTION")
            #print ("Inserting user data into 'follower' table...")

#refresh mutual users
def MutualizeUsers():
    cursor.execute("""
UPDATE follower
SET mutual_flag = 'True'
WHERE CAST(user_id AS VARCHAR(15)) + CAST(follower_id AS VARCHAR(15)) IN 
(SELECT CAST (f1.user_id AS VARCHAR(15)) + CAST(f1.follower_id AS VARCHAR(15))
FROM follower f1 JOIN follower f2
		ON f1.user_id = f2.follower_id 
		AND f1.follower_id = f2.user_id)
""")

def SyncUsers(uids):
    start = time.time()
    print ("Syncing users "+ ','.join(list(map(str, uids))) +" started at "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+"...")
    LoadFollowers(uids)
    LoadFollowing(uids)
    LoadUserDemo(uids)
    #MutualizeUsers()
    end = time.time()
    print ("Action completed in %f seconds." % (end - start))
    
def GetMFollowers(uid):
    cursor.execute("""
SELECT DISTINCT follower_id
FROM follower 
WHERE user_id = """+ str(uid) +"""
AND mutual_flag = 'True'
""")
    followers = cursor.fetchall()
    cleanlist = [x[0] for x in followers]
    return cleanlist

def GetMMFollowers(uid1, uid2):
    cursor.execute("""
SELECT DISTINCT f1.follower_id
FROM follower f1 JOIN follower f2 ON f1.follower_id = f2.follower_id AND f1.user_id <> f2.user_id
WHERE (f1.user_id = """+ str(uid1) +" OR f1.user_id = "+ str(uid2) + """)
AND f1.mutual_flag = 'True' AND f2.mutual_flag = 'True'
""")
    followers = cursor.fetchall()
    cleanlist = [x[0] for x in followers]
    return cleanlist

def GetPeers(uid):
    initlist = GetMFollowers(uid)
    lufollowers = ln(cursor.execute("SELECT followers_count FROM user WHERE id = "+ str(uid)).fetchall()[0][0])
    proclist = []
    for i in range(min([250,len(initlist)])):
        cursor.execute("SELECT EXISTS(SELECT id FROM user WHERE id = "+str(initlist[i][0])+")")
        if (cursor.fetchall()[0][0] > 0):
            procresult = cursor.execute("SELECT id, followers_count, 0 FROM user WHERE id = "+ str(initlist[i][0])).fetchall()[0]
            if (len(procresult) > 0):
                proclist.append(procresult)
    rawset = []
    for i in range(len(proclist)):
        tluf = ln(proclist[i][1])
        if (tluf >= 0.95 * lufollowers and tluf <= 1.05 * lufollowers):
            rawset.append([proclist[i][0], proclist[i][1], tluf])
    resultset = []
    for i in range(len(rawset)):
        resultset.append([rawset[i][0],])
    return resultset

def PrintFollowers(followers):
    resultset = []
    for i in range(len(followers)):
        uid = followers[i][0]
        cursor.execute("SELECT EXISTS(SELECT id FROM user WHERE id = "+str(uid)+")")
        if (cursor.fetchall()[0][0] == 0):
            LoadUserDemo(followers[i][0])
        rowset = cursor.execute("SELECT id, screen_name, followers_count, location FROM user WHERE id = " + str(followers[i][0])).fetchall()
        resultset.append(rowset[0])
    resultset.sort(key = itemgetter(2), reverse = True)
    for i in range(len(resultset)):
        print (str(resultset[i][0]) +"\t"+ str(resultset[i][1]) +"\t"+ str(resultset[i][2]) +"\t"+ resultset[i][3] )

def CompareFollowers(f1, f2):
    followers = []
    for i in range(len(f1)):
        for j in range(len(f2)):
            if (f1[i][0] == f2[j][0]):
                followers.append(f1[i])
    return followers

def CompareFiveUserOverlap(u1, u2, u3, u4, u5):
    for u in (u1, u2, u3, u4, u5):
        SyncUser(u)    
    fu1u2 = GetMMFollowers(u1,u2)
    fu3u4 = GetMMFollowers(u3,u4)
    fu5 = GetMFollowers(u5)
    fu14 = CompareFollowers(fu1u2, fu3u4)
    fu15 = CompareFollowers(fu14,fu5)
    print ("Returning overlapping follower set...")
    return fu15

def GetFollowerOverlap(users):
    fui = GetMFollowers(users[0])
    if len(users) > 1:
        for i in range(1,len(users)):
            fui = CompareFollowers(fui,GetMFollowers(users[i]))
    return fui

def GetFollowerMatrix(followers):
    resultset = [[0 for x in range(len(followers))] for x in range(len(followers))] 
    for i in range(len(followers)):
        for j in range(len(followers)):
            if (i != j):
                print ("Comparing users "+ str(follower[i][0]) +" and "+ str(follower[j][0]) +"...")
                cursor.execute("SELECT COUNT(*) FROM follower WHERE user_id = "+ str(followers[i][0]) +" AND follower_id = "+ str(followers[j][0]))
                if (cursor.fetchall()[0][0] > 0):
                    resultset[i][j] = 1
    return resultset

def PrintMatrix(matrix):
    for i in range(len(matrix)):
        row = ""
        for j in range(len(matrix[i])):
            row += str(matrix[i][j]) + "\t"
        print (row)

#connect to DB
conn = sqlite3.connect("twython.db", isolation_level=None)
cursor = conn.cursor()

#connect to twitter
twitter = ConnectTwitter()

#testing defaults
andrew = 91921157

u1 = 128935830 #AllisonLCarter
u2 = 19498091 #jdisis
u3 = 14765044 #HollyBolton
u4 = 167508932 #randyclarktko
u5 = 328698237 #BizWeaver
u = [u1, u2, u3, u4, u5]

w1 = 15841925 #GregCooper
w2 = 17543210 #brewhouse
w3 = 14820299 #AmyStark
w4 = 19118802 #PaulPoteet
w = [w1, w2, w3, w4]

t1 = 75017032 #TechPointInd
t2 = 426785112 #SpeakEasyIndy
t3 = 141220659 #VergeIndy
t4 = 15359664 #oneclick
t = [t1, t2, t3, t4]

m1 = 40518949 #angelabuchman
m2 = 19118802 #PaulPoteet

