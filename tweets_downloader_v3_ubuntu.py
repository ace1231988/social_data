#!/usr/bin/env python
# -*- coding: utf-8 -*-

from TwitterSearch import *
from mysql.connector import *
from datetime import datetime
import os
import csv
import sys
import time
import codecs

def unicode_tweets_reader(tweets):
    for tweet in tweets:
        yield [unicode(cell, 'utf-8') for cell in tweet]

def get_db_connection():
    try:
        con = connect(host='localhost', user='root', password='123456', database='socialdata', use_unicode = False, charset = 'utf8mb4')
        return con
    except Exception as e:
        # print "Error occured when connecting database: %s!" %sys.exc_info()[0]
        print 'Error@get_db_connection(): %s!' %e
            
def get_api(key, secret, acs_key, acs_secret, prxy):
    """
        INPUT: key, secret, acs_key, acs_secret -- Twitter developer's information
               prxy -- proxy used to send http request
        OUTPUT: an instance of Twitter search object
    """
    print 'Creating TwitterSearch API'
    return TwitterSearch(consumer_key = key, consumer_secret = secret, access_token = acs_key, access_token_secret = acs_secret)

def search_tweets(api, keyword, since_id):
    """
        INPUT: api -- TwitterSearch object 
               keyword -- a string to be searched
               since_id -- id of the last tweet obtained
        OUTPUT: a list of returned Tweet objects
    """
    tso =  TwitterSearchOrder()
    tso.set_language('en')
    if since_id!='':
        tso.set_since_id(long(since_id))
    tso.set_keywords([keyword])
    results = api.search_tweets_iterable(tso)
    return results
    
def get_last_tweet_id(db_con, keyword):
    """
        INPUT: db_con -- database handler
               keyword -- a string to be searched
        OUTPUT: the largest id of the tweets returned by previous search queries
    """
    cursor = db_con.cursor()
    ###get the id of last searched tweet to avoid redundant search
    sql = 'select max(max_tweet_id) from twitter_logs where keyword=%s'
    try:
        cursor.execute(sql, (keyword,))
        last_tweet_id = cursor.fetchall()[0][0]
        if last_tweet_id is None:
            last_tweet_id = ''
        cursor.close()
        print 'last_tweet_id = %s' %last_tweet_id
        return last_tweet_id
    # file_start_id = open('start_id_' + keyword + '.txt', 'r')
    # last_tweet_id = file_start_id.readline()
    # file_start_id.close()
    except Exception as e:
        print 'Error@get_last_tweet_id(): %s!' %e
    
    
def write2db (db_con, keyword, max_id, res_list):
    """
        INPUT: db_con -- database handler
               keyword -- a string to be searched
               max_id -- the largest id of the tweets returned by recent search queries
               res_list -- values to be stored in database
        OUTPUT: no return value
    """
    cursor = db_con.cursor()
    ###insert tweets to database
    try:
        sql = 'insert into tweets(keyword, id, created_at, text, reply_to, author_name, author_id, author_followers, author_followings, author_location, author_favorite_cnt, author_status_cnt, author_created_at) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        # cursor.executemany(sql, res_list)
        for e in res_list:
            # print 'insert tweets: %s' % e[3]
            cursor.execute(sql, e)
            db_con.commit()
        sql = 'insert into twitter_logs(keyword, max_tweet_id, tweets_obtained) values(%s, %s, %s)'
        cursor.execute(sql, (keyword, max_id, len(res_list)))
        db_con.commit()
    except Exception as e:
        # db_con.rollback()
        print 'Error@write2db(): %s!' %e
        
    ###record the max tweet id
    # file_start_id = open('start_id_' + keyword + '.txt', 'w')
    # file_start_id.write(max_id)
    # file_start_id.close()

    
def infor_to_extract(tweet):
    """
        INPUT: tweet -- a json file of a tweet returned by the search query
        OUTPUT: a list consisting of all fields in the tweet of our interests
    """
    tweet_id = tweet['id_str']
    tweet_timestamp = datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y").strftime('%Y-%m-%d %H:%M:%S')
    tweet_text = tweet['text'].encode('utf-8')
    tweet_reply_to = tweet['in_reply_to_user_id']
    if tweet_reply_to is None:
        tweet_reply_to = ''
    # print 'tweet_reply_to: %s' %tweet_reply_to
    tweet_author = tweet['user']['screen_name']
    tweet_author_id = tweet['user']['id_str']
    tweet_author_followers = tweet['user']['followers_count']
    tweet_author_followings = tweet['user']['friends_count']
    tweet_author_location = tweet['user']['location']
    tweet_author_favorite_cnt = tweet['user']['favourites_count']
    tweet_author_status_cnt = tweet['user']['statuses_count']
    tweet_author_created_at = datetime.strptime(tweet['user']['created_at'], "%a %b %d %H:%M:%S +0000 %Y").strftime('%Y-%m-%d %H:%M:%S')
    line =  [tweet_id, tweet_timestamp, tweet_text, tweet_reply_to, tweet_author, tweet_author_id, tweet_author_followers, tweet_author_followings, tweet_author_location, tweet_author_favorite_cnt, tweet_author_status_cnt, tweet_author_created_at]
    # print line
    return line
    
def main():
    ###Set default encoding
    # reload(sys)
    # sys.setdefaultencoding('utf-8')
    
    ###process starts
    print "-----------------Hey we go!!! Starting at %s-----------------" %datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    ###Set working directory
    # os.chdir(os.getcwd()+'\Data')

        ###Provide network proxy
    prxy = 'http://limeilm:bmw19646288!@singtelproxy.is:80'
    ###Provide Twitter API account
    key= '2fXCImGI1rymynSSzGRoa0hK8'
    secret = 'yzovXYYgBXqfVmxQaLwxWu8fNQhkKMCLRe5LqnXUQldy4oQkzS'
    acs_key = '184330704-LLPBJwGKRYZ4Dtc8DxJblqvA5ql6v4ZxWPjIWSD7'
    acs_secret = 'qYDbaGVVj2SygBH0dvZ9ZOhxjz3spoj5Fgnw1qdvGhe4u'
    ###Specify keywords interested
    keyword_list = ['singtel', 'starhub', 'm1']
    #keyword_list = ['singtel', 'starhub', 'm1', 'iphone', 'samsung', 'nokia', 'galaxy', 'xiaomi']

    ###Get database connection
    db_con = get_db_connection()
    ###Get Twitter API
    twitter_api =  get_api(key, secret, acs_key, acs_secret, prxy)
    #twitter_api =  TwitterSearch(consumer_key = key, consumer_secret = secret, access_token = acs_key, access_token_secret = acs_secret, proxy = prxy)
    
    ###Get tweets for each keyword (search window is recent 7 days)
    #??????do we need to update the status of existing tweets?
    # output = open('temp.csv', 'ab')
    # out_writer = csv.writer(output, delimiter=',', quoting=csv.QUOTE_MINIMAL) 
    #out_writer = UnicodeWriter(output, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    for keyword in keyword_list:
        print 'keyword is %s' %keyword
        ###get the id of last searched tweet to avoid redundant search
        start_id  = get_last_tweet_id(db_con, keyword)
        db_con.close()
        # print 'start_id: %s' %start_id
        ###results list for output
        lines2write = []
        lines= []
        ###call api to search twitter
        print 'Start search!'
        res = search_tweets(twitter_api, keyword, start_id)
        ###get the stats and content of first query
        stats = (res.get_tweets())['search_metadata']
        tweets = (res.get_tweets())['statuses']
        ###record max id of this round of queries
        max_id = stats['max_id_str']
        ########DEBUG PURPOSE
        print str(len(tweets)) + ' tweets have been obtained'
        print 'tweets id: ' + stats['since_id_str'] + ' to ' + stats['max_id_str']
        ###prepare results list
        for tweet in tweets:
            # print tweet['created_at'] + '==> @ '+ tweet['user']['screen_name'] + ':' + tweet['id_str']
            line = infor_to_extract(tweet)
            lines.append(line)
            line.insert(0, keyword)
            # print line
            lines2write.append(tuple(line))
        ###continue search as only 100 tweets at max can be returned per query
        while(True):
            try:
                print '\n (^-^)V  Sleeping zzZ..zZZZ...\n'
                time.sleep(5)
                twitter_api.search_next_results()
                stats = (twitter_api.get_tweets())['search_metadata']
                tweets = (twitter_api.get_tweets())['statuses']
                print str(len(tweets)) + ' tweets have been obtained'
                print 'tweets id: ' + stats['since_id_str'] + ' to ' + stats['max_id_str']   
                ###prepare results list
                for tweet in tweets:
                    # print tweet['created_at'] + '==> @ '+ tweet['user']['screen_name'] + ':' + tweet['id_str']
                    line = infor_to_extract(tweet)
                    lines.append(line)
                    line.insert(0, keyword)
                    # print line
                    lines2write.append(tuple(line))
            except TwitterSearchException as e:
                if e.code == 1011:
                    break
                else:
                    print("Error@main() exception: %i - %s" % (e.code, e.message))
            except Exception as e:
                print("Error@main() exception: %s" % (e.message))
        # output = codecs.open('tweets.csv', 'ab', encoding='utf-8')
        # out_writer = csv.writer(output, delimiter=',', quoting=csv.QUOTE_MINIMAL) 
        # out_writer.writerows(lines)
        db_con = get_db_connection()
        ###update database
        write2db(db_con, keyword, max_id, lines2write)
        print 'Wrote %d tweets to database' %len(lines2write)
    db_con.close()
    
    ###process ends
    print "-----------------Ooh yeah!!! Ended at %s-----------------" %datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
if __name__ == '__main__':
    main()

                
