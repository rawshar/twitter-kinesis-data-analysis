import boto3
import json
from datetime import datetime
import calendar
import random
import time
import sys
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
from tweepy import StreamingClient, StreamRule

consumer_key = 'xxxxxx' # add consumer key
consumer_secret = 'xxxxx' # add consumer secret key
access_token_key = 'xxxxxx' # add token secret
access_token_secret = 'xxxx' # add token secret
bearer_token = "xxxx" # add bearer token

class TweetPrinterV2(tweepy.StreamingClient):   
    
    def on_data(self, data):
        # decode json
        tweet = json.loads(data)
        # print(tweet)
        # print((tweet["data"]))
       
        if "text" in tweet["data"].keys():
            # print("inside if")
            payload = {'id': str(tweet["data"]['id']),
                                  'tweet': str(tweet["data"]['text'].encode('utf8', 'replace')),
                                   'ts': ' '.join(str(tweet["data"]['created_at']).split("T"))
            },
            print(payload)
            # print("am--------")
            try:
                put_response = kinesis_client.put_record(
                                StreamName=stream_name,
                                Data=json.dumps(payload),
                                PartitionKey=str(tweet['data']['id']))
                # print(put_response)
            except (AttributeError, Exception) as e:
                print (e)
                pass
        return True
        
    # on failure
    def on_error(self, status):
        print(status)
stream_name = 'kds-twitter-sda'
if __name__ == '__main__':
  printer = TweetPrinterV2(bearer_token)
  kinesis_client = boto3.client('kinesis', 
                                    region_name= "us-east-1",  # enter the region
                                    aws_access_key_id='xxx',  # fill your AWS access key id
                                    aws_secret_access_key='xxxx')
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token_key, access_token_secret)
  # add new rules    
  rule = StreamRule(value="Arts")   # add filter of yuor choice
  printer.add_rules(rule)  
  printer.filter(expansions="author_id",tweet_fields="created_at")