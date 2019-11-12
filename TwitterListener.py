import socket
import json
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import twittercredentials
import awscredentials
import datetime
import re
import time
from textblob import TextBlob
from collections import namedtuple
import boto3
import datetime





ts = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
def file_len(i):
    with open("/home/crom/tweet.csv") as f:
        for  l in  enumerate(f):
            i+=1
    return i 
#Stream Listener Class
class TweetsListener(StreamListener):
    

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data): 
        msg = json.loads(data)
        

        tweet_text = msg['text']
        hashtags = '#'.join(hashtag['text'] for hashtag in
                    msg['entities']['hashtags'])
        if ('retweeted_status' in msg):
            
            if ('extended_tweet' in msg['retweeted_status']):
                
                print( tweet_text + "\n"  + hashtags + "\n"  )
                self.client_socket.send(str(  tweet_text + "\n" + hashtags + "\n"  ).encode('utf-8'))
                if (file_len(0) < 100):
                    save_file = open("/home/crom/tweet.csv","a")     

                    save_file.write(hashtags)
                    save_file.write('\n')
                    
                    save_file.close()
                          
        elif ('extended_status' in msg):
           
            print(tweet_text + "\n"  + hashtags + "\n"  )
            self.client_socket.send(str( tweet_text + "\n" + hashtags + "\n"  ).encode('utf-8'))
            if (file_len(0) < 100):
                save_file = open("/home/crom/tweet.csv","a")     
                save_file.write(hashtags)
                save_file.write('\n')
                
                save_file.close()
          

        else:
       
            print(  tweet_text + "\n"  + hashtags + "\n"   )
            self.client_socket.send(str(tweet_text + "\n" + hashtags + "\n" ).encode('utf-8'))
            
            if (file_len(0) < 100):
                save_file = open("/home/crom/tweet.csv","a")     
                save_file.write(hashtags )
                save_file.write('\n')
                time.sleep(1)
                save_file.close()
            elif (file_len(0) == 100):
                end = time.time()
                save_file = open("/home/crom/tweet.csv","a")
                save_file.write("GET ENOUGH 100 TWEET IN " + str(round(end-start)) + " SECOND")
                save_file.close()     

           

    def on_error(self, status):
        print(status)
        return True


#Initializing the port and host
host = 'localhost'
port = 5555
address = (host, port)

#Initializing the socket

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(address)
server_socket.listen(5)
start = time.time()
print("Listening for client port 5555 ")
conn, address = server_socket.accept()
print("Connected to Client at port 5555 " + str(address))
#authen                                                                         ````````````                        ticating
auth = OAuthHandler(twittercredentials.consumer_key, twittercredentials.consumer_secret)
auth.set_access_token(twittercredentials.access_token, twittercredentials.access_token_secret)
twitter_stream = Stream(auth, TweetsListener(conn), tweet_mode="extended_tweet")
twitter_stream.filter(track=['holiday'] , locations=[-74.1687,40.5722,-73.8062,40.9467] , languages = ['en'])



   
    
    


#location of NewYork

    

   






    
