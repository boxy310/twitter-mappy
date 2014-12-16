# holds definition for the Twython extended class
import sys
import time
from urllib.error import URLError
from http.client import BadStatusLine
from twython import Twython, TwythonError, TwythonRateLimitError

class TwitterConnect(Twython):
    # wrap Twitter API in a robust error-handling call
    def make_twitter_request(self, twitter_api_func, max_errors=10, *args, **kw): 
        
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
                    print ("Retrying in 15 minutes...ZzZ...", file=sys.stderr)
                    sys.stderr.flush()
                    time.sleep(60*15 + 5) 
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
    def partition(self, uids, n):
        parts = []
        while (len(uids) > n):
            parts.append(uids[0:n])
            del uids[0:n]
        parts.append(uids)
        return parts
    def parse_user(self, json):
        uid = str(json['id'])
        screen_name = "'"+str(json['screen_name'])+"'"
        name = "'"+str(json['name']).replace("'", "''")+"'"
        description = "'"+str(json['description']).replace("'", "''")+"'"
        location = "'"+str(json['location']).replace("'", "''")+"'"
        url = (" '"+ str(json['url']).replace("'", "''") +"' ").replace("'None'", "NULL")
        friends_count = str(json['friends_count'])
        followers_count = str(json['followers_count'])
        statuses_count = str(json['statuses_count'])
        lang = "'"+str(json['lang'])+"'"
        geo_enabled = "'"+str(json['geo_enabled'])+"'"
        twitter_date_created = "'"+str(json['created_at'])+"'"
        date_created = "DATETIME(CURRENT_TIMESTAMP, 'localtime')"
        date_last_modified = "DATETIME(CURRENT_TIMESTAMP, 'localtime')"
        id_str = "'"+str(json['id_str'])+"'"
        date_sync = "NULL"
        parse_array = [uid, screen_name, name, description, location, url, friends_count, followers_count, statuses_count, lang, geo_enabled, twitter_date_created, date_created, date_last_modified, id_str, date_sync]
        return parse_array
    def parse_follower(self, uid, fols):
        fols_parse = []
        for fol in fols:
            fols_parse.append([uid, fol])
        return fols_parse
    def helper_part(self, uids, CallFn, ParseFn, n=100, file='collect.tmp'):
        uids = self.partition(uids,n)
        tbl = []
        for batch in uids:
            call = CallFn(batch)
            tbl.extend(list(map(ParseFn, call)))
        return tbl
    def user_demo_part(self, uids):
        call = self.make_twitter_request(self.lookup_user, user_id = ','.join(map(str,uids)))
        return call
    def user_demo(self, uids):
        return self.helper_part(uids, self.user_demo_part, self.parse_user)
    def followers (self, uid):
        cursor = -1
        followers = []
        while (cursor != 0):
            call = self.make_twitter_request(self.get_followers_ids, user_id = uid, cursor = cursor)
            followers.extend(call['ids'])
            cursor = call['next_cursor']
        return followers
