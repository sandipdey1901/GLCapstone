from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import datetime
from botocore.exceptions import ClientError

 
import twitterapiCredentials
 
# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(twitterapiCredentials.CONSUMER_KEY, twitterapiCredentials.CONSUMER_SECRET)
        auth.set_access_token(twitterapiCredentials.ACCESS_TOKEN, twitterapiCredentials.ACCESS_TOKEN_SECRET)
        

        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords: 
        stream.filter(track=hash_tag_list)



# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            #print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            DeliveryStreamName = 'SendToES'
            aws_key_id = twitterapiCredentials.AWS_KEY_ID
            aws_key = twitterapiCredentials.AWS_KEY

            client = boto3.client('firehose', region_name='us-east-1',
                          aws_access_key_id=aws_key_id,
                          aws_secret_access_key=aws_key
                          )

            client.put_record(DeliveryStreamName=DeliveryStreamName,Record={'Data': json.loads(data)["text"]})
            print (json.loads(data)["text"])

            client = boto3.client('s3', region_name='us-east-1',
                          aws_access_key_id=aws_key_id,
                          aws_secret_access_key=aws_key
                          )
            client.upload_file(fetched_tweets_filename, 'gl-capstone-processed-tweets', fetched_tweets_filename)

            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
          

    def on_error(self, status):
        print(status)

    def load_raw_tweets(self, fetched_tweets_filename,bucket,object_name=None):
        
        client = boto3.client('s3', region_name='us-east-1',
              aws_access_key_id=aws_key_id,
              aws_secret_access_key=aws_key
              )
        if object_name is None:
            object_name = fetched_tweets_filename

        try:
            client.upload_file(fetched_tweets_filename, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

 
if __name__ == '__main__':
 
    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = ["@narendramodi","NAMO","Narendra Modi","Donald Trump","@realDonaldTrump","COVID19","coronavirus"]
    fetched_tweets_filename = "tweets_"+str(datetime.datetime.now())[0:10]+".txt"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)
    load_raw_tweets(fetched_tweets_filename,'gl-capstone-processed-tweets')
    