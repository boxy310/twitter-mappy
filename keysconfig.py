import requests
from twython import Twython

# pulls together the OAUTH tokens from the APP_KEY and APP_SECRET

APP_KEY = 'XXXX'
APP_SECRET = 'XXXX'

twitter = Twython(APP_KEY, APP_SECRET)
auth = twitter.get_authentication_tokens()

OAUTH_TOKEN = auth['oauth_token']
OAUTH_TOKEN_SECRET = auth['oauth_token_secret']

# follow this URL and grab the PIN
auth['auth_url']

oauth_verifier = 4004778

twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

final_step = twitter.get_authorized_tokens(oauth_verifier)

# final tokens
OAUTH_TOKEN = final_step['oauth_token']
OAUTH_TOKEN_SECRET = final_step['oauth_token_secret']

