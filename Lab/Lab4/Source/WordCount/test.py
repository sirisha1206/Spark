import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
consumer_key = "1Uvka6qW3BXrCApO3Xb5HF7Ni"
consumer_secret = "QzyedhCq0Hak7bpFMHg1DO0lYNeVNHS30x91z1ax9T9Ra0vddA"
access_token = "2923721431-d85O3ExEbRGGQoWzgE6g0OUnJhYUkoQsvdjT3fV"
access_token_secret = "sAlzxwuHylrX3HOeHQWV8IFDLBJQcGq3cScS1hrNIZeID"
# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object while passing in auth information
api = tweepy.API(auth)


class StdOutListener(StreamListener):

    def on_data(self, data):
        try:
            with open('tweetsdataemotions1.json', 'a') as f:
                f.write(data)
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'stock market'
    stream.filter(track=['emotions','happy','sad','joyful','depression','mood','cheerful','glad','carefree','joyous','stress','hectic','delight','please','sorry','sorrowful','gloomy','miserable','depressed','unhappy','fear','surprise','disgust','anger','anxiety','evny','regret','shame','sympathy','love','hate','hunger','hope','frustation'])
