#!/usr/bin/env python
# -*- coding: utf-8 -*-

import facebook
from mysql.connector import *
from datetime import datetime
import requests
import time

###Provide Facebook API account
app_id = '237499316408482'
app_secret = '924e4a56caaeec02c3ac1699ae2cff79'
# Current Date: July 14, 2016, 6:50:33 pm
# Token Expiration Date: September 12, 2016, 6:50:33 pm
# Get new token at http://www.slickremix.com/facebook-60-day-user-access-token-generator/
access_token = 'EAADYAR5jkKIBABUrISfIOecSBpYVkdiDGHiFHtH1YNqZBZBnZCuUwlTRZC2ev1uHcgWs1cIDLOOx3O0ZCSchklfUzqxiOH3Kp0HrrC1rAFoOOuYgT5lGV6JMGtbvuJVlnIgcKTfVFW0b6tj0QDAFpCklfhVaH4ecZD'


######################################################
####TODO: extend access token automatically 
######################################################

def get_db_connection():
    try:
        con = connect(host='localhost', user='root', password='123456', database='socialdata', use_unicode = False, charset = 'utf8mb4')
        return con
    except Exception as e:
        # print "Error occured when connecting database: %s!" %sys.exc_info()[0]
        print 'Error@get_db_connection(): %s!' %e
        
def get_fb_api():
    """
        INPUT: acs_token -- Facebook developer's information
        OUTPUT: an instance of Facebook Graph query object
    """
    print 'Creating Facebook Graph API'
    try:             
        return facebook.GraphAPI(access_token = access_token, version = '2.5')
    except Exception as e:
        print 'Error@get_fb_api(): %s!' %e
        
# def get_fb_api():
    # """
        # INPUT: acs_token -- Facebook developer's information
        # OUTPUT: an instance of Facebook Graph query object
    # """
    # print 'Creating Facebook Graph API'
    # try:
        # with open('/home/chenwei/socialdata/fb_acs_token.txt', 'r') as f:
            # token = f.read()
        # api = facebook.GraphAPI(access_token = token)
        # ext_resp = api.extend_access_token(app_id, app_secret)
        # with open('/home/chenwei/socialdata/fb_acs_token.txt', 'w') as f:
            # f.write(ext_resp['access_token'])                
        # return facebook.GraphAPI(access_token = ext_resp['access_token'])
    # except Exception as e:
        # print 'Error@get_fb_api(): %s!' %e

def value_from_dict(dict, field):
    """
        INPUT: dict -- a dictionary to be parsed
               field -- field to extract
        OUTPUT: value of the field, if it is an invalide field, return ''
    """
    if dict.has_key(field):
        return dict[field]
    else:
        return ''

def set_limit(last_post_time):
    """
        INPUT: last_post_time -- time of the latest Facebook post
        OUTPUT: 100 if the first time to collect posts, otherwise 25
    """
    if last_post_time < datetime.strptime('1901-1-1 0:0:0', "%Y-%m-%d %H:%M:%S"):
        limit = 150
    else:
        limit = 25
    return limit


def get_page_infor(fanpage_name, api):
    """
        INPUT: api -- Facebook Graph object 
               fanpage_name -- Facebook fanpage's name
               db_con -- database handler
        OUTPUT: a tuple consisting of fanpage infor
    """
    ###send graph query
    try:
        req_fields = 'id, website, company_overview, description, general_info, founded, mission, products'
        fanpage_obj = api.get_object(id=fanpage_name, fields=req_fields)
    except Exception as e:
        print 'Error when search for fanpage infor@get_fanpage_infor(): %s!' %e
    ###parse Facebook Graph object
    page_id = fanpage_obj['id']
    page_website = value_from_dict(fanpage_obj, 'website')
    page_company_overview = value_from_dict(fanpage_obj, 'company_overview')
    page_description = value_from_dict(fanpage_obj, 'description')
    if page_description == '':
        page_description = value_from_dict(fanpage_obj, 'general_info')
    page_founded = value_from_dict(fanpage_obj,'founded')
    page_mission = value_from_dict(fanpage_obj, 'mission')
    page_prodcuts = value_from_dict(fanpage_obj, 'products')
    ###return fanpage infor in a list
    return [page_id, fanpage_name, page_website, page_company_overview, page_description, page_founded, page_mission, page_prodcuts]

def write2db_page_infor(fanpage_infor, db_con):
    """
        INPUT: fanpage_infor -- a list of fanpage entries
               db_con -- database handler
        OUTPUT: no return value
    """
    cursor = db_con.cursor()
    ###insert tweets to database
    try:
        sql = 'insert into fb_page(page_id, page_name, page_website, page_company_overview, page_description, page_founded, page_mission, page_products) values(%s, %s, %s, %s, %s, %s, %s, %s)'
        for e in fanpage_infor:
            # print 'insert tweets: %s' % e[3]
            cursor.execute(sql, e)
        db_con.commit()
    except Exception as e:
        db_con.rollback()
        print 'Error@write2db_page_infor(): %s!' %e


def page_updater(db_con, api):
    """
        INPUT: db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: a list consisting of all fanpage ids
    """
    print '==> updating Facebook fanpage infor ...'
    ###Get a list of fanpage ids stored in database
    cursor = db_con.cursor()
    try:
        cursor.execute('select page_id from fb_page')
        fanpage_ids = list(e[0] for e in cursor.fetchall())
    except Exception as e:
        print 'Error in reading existing fanpage ids@page_updater(): %s!' %e
    ###Update the numbers of fans, checkins and talk abouts fanpages received
    page_infor_list = []
    req_fields = 'likes, checkins, talking_about_count'
    for fanpage in fanpage_ids:
        fanpage_obj = api.get_object(id = str(fanpage), fields=req_fields)
        fans = fanpage_obj['likes']
        checkins = fanpage_obj['checkins']
        talkabt = fanpage_obj['talking_about_count']
        page_infor_list.append([fanpage, fans, checkins, talkabt])
    ###Write to database
    try:
        sql = 'insert into fb_page_fans(page_id, page_fans, page_checkins, page_talkabt) values(%s, %s, %s, %s)'
        for e in page_infor_list:
            cursor.execute(sql, e)
        db_con.commit()
        print "inserted %d records into fb_page_fans" %len(page_infor_list)
    except Exception as e:
        db_con.rollback()
        print 'Error in updating fanpage infor to database@page_updater(): %s!' %e
    return fanpage_ids


def new_posts_collector(fanpage_ids, db_con, api):
    """
        INPUT: fanpage_ids: a list consisting of all fanpage ids
               db_con -- database handler
               api -- Facebook Graph object 
        OUTPUT: no return value
    """
    print '==> collecting new posts on fanpages'
    posts2write = []
    ###Collect new posts on each fanpage one by one
    for fanpage in fanpage_ids:
        ###get the creation time of the latest post on the focal fanpage
        # last_post_time = datetime.strptime(get_last_post_time(fanpage, db_con), '%Y-%m-%d %H:%M:%S')
        last_post_time = get_last_post_time(fanpage, db_con)
        ###If it is the first time to collect posts, set a larger limit to reduce page numbers
        lmt = set_limit(last_post_time)
        req_fields = 'from, created_time, message, type, caption, name, link, object_id, description, picture'
        posts_obj = api.get_connections(id=str(fanpage), connection_name='posts', limit = lmt, fields=req_fields)
        posts_list = object_traversal(posts_obj, last_post_time, 'posts')
        posts_parsed = list(post_parser(p) for p in posts_list)
        posts2write.extend(posts_parsed)
    ###Write parsed posts to database
        ###Write to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_p_content(p_id, p_uid, p_uname, p_created_at, p_message, p_type, p_caption, p_name, p_link, p_obj_id, p_description, p_picture) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for e in posts2write:
            # print 'p_name: %s' %e[7]
            cursor.execute(sql, e)
        db_con.commit()
        print "inserted %d records into fb_p_content" %len(posts2write)
    except Exception as e:
        db_con.rollback()
        print 'Error in updating posts content to database@new_posts_collector(): %s!' %e
        
def old_posts_collector(fanpage_ids, db_con, api):
    """
        INPUT: fanpage_ids: a list consisting of all fanpage ids
               db_con -- database handler
               api -- Facebook Graph object 
        OUTPUT: no return value
    """
    print '==> collecting old posts on fanpages'
    posts2write = []
    ###Collect new posts on each fanpage one by one
    for fanpage in fanpage_ids:
        ###get the creation time of the latest post on the focal fanpage
        # last_post_time = datetime.strptime(get_last_post_time(fanpage, db_con), '%Y-%m-%d %H:%M:%S')
        first_post_time = get_first_post_time(fanpage, db_con)
        ###If it is the first time to collect posts, set a larger limit to reduce page numbers
        #lmt = set_limit(last_post_time)
        lmt = 250
        posts_obj = api.get_connections(id=str(fanpage), connection_name='posts')
        posts_list = object_traversal_new(posts_obj, first_post_time, 'posts')
        posts_parsed = list(post_parser(p) for p in posts_list)
        posts2write.extend(posts_parsed)
    ###Write parsed posts to database
        ###Write to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_p_content(p_id, p_uid, p_uname, p_created_at, p_message, p_type, p_caption, p_name, p_link, p_obj_id, p_description, p_picture) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for e in posts2write:
            # print 'p_name: %s' %e[7]
            cursor.execute(sql, e)
        db_con.commit()
        print "inserted %d records into fb_p_content" %len(posts2write)
    except Exception as e:
        db_con.rollback()
        print 'Error in updating posts content to database@new_posts_collector(): %s!' %e
        
def get_last_post_time(fanpage_id, db_con):
    """
        INPUT: fanpage_id -- id of the focal fanpage
               db_con -- database handler
        OUTPUT: creation time of the latest fanpage post in database
    """
    ###get the creation time of the latest post on the focal fanpage
    cursor = db_con.cursor()
    sql = 'select max(p_created_at) from fb_p_content where p_uid = %s'
    try:
        cursor.execute(sql, (fanpage_id,))
        last_post_time = cursor.fetchall()[0][0]
        if last_post_time is None:
            last_post_time = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        cursor.close()
        print 'last_post_time of %s = %s' %(fanpage_id, last_post_time)
        return last_post_time
    except Exception as e:
        print 'Error@get_last_post_time(): %s!' %e

def get_first_post_time(fanpage_id, db_con):
    """
        INPUT: fanpage_id -- id of the focal fanpage
               db_con -- database handler
        OUTPUT: creation time of the latest fanpage post in database
    """
    ###get the creation time of the latest post on the focal fanpage
    cursor = db_con.cursor()
    sql = 'select min(p_created_at) from fb_p_content where p_uid = %s'
    try:
        cursor.execute(sql, (fanpage_id,))
        last_post_time = cursor.fetchall()[0][0]
        if last_post_time is None:
            last_post_time = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        cursor.close()
        print 'first_post_time of %s = %s' %(fanpage_id, last_post_time)
        return last_post_time
    except Exception as e:
        print 'Error@get_first_post_time(): %s!' %e
        
def object_traversal(graph_obj, last_element_time, obj_type):
    """
        INPUT: graph_obj -- Facebook Graph object containing posts/comments returned
               last_element_time -- creation time of the latest element
               obj_type -- posts, comments or replies (the time ordering returned by Facebook API varies)
        OUTPUT: a list of newly added fanpage posts
    """
    # print 'entry@object_traversal()'
    data_json = {}
    ###Process current Facebook Graph object
    if graph_obj.has_key('data'):
        ###Add new posts to results list
        for e in graph_obj['data']:
            # print '@object_traversal(): created_time %s, last_element_time %s' %(e['created_time'], last_element_time.strftime('%Y-%m-%d %H:%M:%S'))
            if datetime.strptime(e['created_time'], '%Y-%m-%dT%H:%M:%S+0000') > last_element_time:
                post_id = e['id']
                if not data_json.has_key(post_id):
                    data_json[post_id] = e
            elif obj_type == 'posts':
                return list(data_json.values())
        ###Visit next pages
        while(True):
            # print '@object_traversal(): in while'
            ###If paging infor is available
            if graph_obj.has_key('paging'):
                # print '@object_traversal(): has key -- paging'
                ###If next page is available
                if graph_obj['paging'].has_key('next'):
                    # print '@object_traversal(): has key -- paging->next'
                    # print 'pos3@object_traversal()'
                    next_page = graph_obj['paging']['next']
                    if next_page != '':
                        try:
                            # print '@object_traversal(): get next page'
                            ###Get next page
                            graph_obj = requests.get(next_page).json()
                            time.sleep(1)
                        except Exception as e:
                            print 'Error in reading next page@object_traversal(): %s!' %e
                        ###Add new posts in next page to results list
                        if graph_obj.has_key('data'):
                            for e in graph_obj['data']:
                                if datetime.strptime(e['created_time'], '%Y-%m-%dT%H:%M:%S+0000') > last_element_time:
                                    post_id = e['id']
                                    if not data_json.has_key(post_id):
                                        data_json[post_id] = e
                                elif obj_type == 'posts':
                                    return list(data_json.values())
                        ###Stop iteration if no new posts
                        else:
                            break
                    ###Stop iteration if no next page
                    else:
                        break
                ###Stop iteration if there is no next page
                else:
                    break
            ###Stop iteration if there is no paging infor
            else:
                break
    ###Return results list
    return list(data_json.values())

def object_traversal_new(graph_obj, last_element_time, obj_type):
    """
        INPUT: graph_obj -- Facebook Graph object containing posts/comments returned
               last_element_time -- creation time of the latest element
               obj_type -- posts, comments or replies (the time ordering returned by Facebook API varies)
        OUTPUT: a list of newly added fanpage posts
    """
    # print 'entry@object_traversal()'
    data_json = {}
    ###Process current Facebook Graph object
    if graph_obj.has_key('data'):
        ###Add new posts to results list
        for e in graph_obj['data']:
            print e['id'] +':'+ e['created_time']
            # print '@object_traversal(): created_time %s, last_element_time %s' %(e['created_time'], last_element_time.strftime('%Y-%m-%d %H:%M:%S'))
            if datetime.strptime(e['created_time'], '%Y-%m-%dT%H:%M:%S+0000') < last_element_time:
                post_id = e['id']
                if not data_json.has_key(post_id):
                    data_json[post_id] = e
        ###Visit next pages
        while(True):
            print '@object_traversal(): in while'
            ###If paging infor is available
            if graph_obj.has_key('paging'):
                print '@object_traversal(): has key -- paging'
                ###If next page is available
                if graph_obj['paging'].has_key('next'):
                    print '@object_traversal(): has key -- paging->next'
                    # print 'pos3@object_traversal()'
                    next_page = graph_obj['paging']['next']
                    if next_page != '':
                        try:
                            print '@object_traversal(): get next page'
                            ###Get next page
                            graph_obj = requests.get(next_page).json()
                            time.sleep(1)
                        except Exception as e:
                            print 'Error in reading next page@object_traversal(): %s!' %e
                        ###Add new posts in next page to results list
                        if graph_obj.has_key('data'):
                            for e in graph_obj['data']:
                                print e['id'] +':'+ e['created_time']
                                if datetime.strptime(e['created_time'], '%Y-%m-%dT%H:%M:%S+0000') < last_element_time:
                                    post_id = e['id']
                                    print post_id +':'+ e['created_time']
                                    if not data_json.has_key(post_id):
                                        data_json[post_id] = e
                        ###Stop iteration if no new posts
                        else:
                            print 'not has_key data'
                            print graph_obj
                            break
                    ###Stop iteration if no next page
                    else:
                        print 'next page is null'
                        print graph_obj
                        break
                ###Stop iteration if there is no next page
                else:
                    print 'not has_key next'
                    print graph_obj
                    break
            ###Stop iteration if there is no paging infor
            else:
                print 'not has_key paging'
                print graph_obj
                break
    ###Return results list
    return list(data_json.values())
    
def post_parser(post):
    """
        INPUT: post -- json file of fanpage post, to be parsed
        OUTPUT: a list consisting of uesful information of the focal post
    """
    post_id = post['id']
    post_uid = post['from']['id']
    post_uname = post['from']['name']
    post_created_at = datetime.strptime(post['created_time'], '%Y-%m-%dT%H:%M:%S+0000').strftime('%Y-%m-%d %H:%M:%S')
    post_message = value_from_dict(post, 'message').decode('utf-8')
    post_type = value_from_dict(post, 'type').decode('utf-8')
    post_caption = value_from_dict(post, 'caption').decode('utf-8')
    post_name = value_from_dict(post, 'name').decode('utf-8')
    post_link = value_from_dict(post, 'link')
    post_obj_id = value_from_dict(post, 'object_id')
    post_description = value_from_dict(post, 'description').decode('utf-8')
    post_picture = value_from_dict(post, 'picture')
    return [post_id, post_uid, post_uname, post_created_at, post_message, post_type, post_caption, post_name, post_link, post_obj_id, post_description, post_picture]


def existing_posts_updater(db_con, api):
    """
        INPUT: db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: a list consisting of all posts ids that published in recent one month
    """
    print '==> updating existing posts ...'
    ###Get a id list consisting of all posts published in recent 30 days
    recent_posts = get_recent_posts(db_con)
    print '@existing_posts_updater: %d posts to update' %len(recent_posts)
    update_post_likes(recent_posts, db_con, api)
    update_post_shares(recent_posts, db_con, api)
    return recent_posts

def old_posts_updater(db_con, api):
    """
        INPUT: db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: a list consisting of all posts ids that published in recent one month
    """
    print '==> updating old posts ...'
    ###Get a id list consisting of all posts published in recent 30 days
    recent_posts = get_old_posts(db_con)
    print '@old_posts_updater: %d posts to update' %len(recent_posts)
    update_post_likes(recent_posts, db_con, api)
    update_post_shares(recent_posts, db_con, api)  

def old_posts_updater_shares(db_con, api):
    """
        INPUT: db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: a list consisting of all posts ids that published in recent one month
    """
    print '==> updating old posts ...'
    ###Get a id list consisting of all posts published in recent 30 days
    recent_posts = get_old_posts(db_con)
    print '@old_posts_updater: %d posts to update' %len(recent_posts)
    # update_post_likes(recent_posts, db_con, api)
    update_post_shares(recent_posts, db_con, api) 
    
def get_old_posts(db_con):
    """
        INPUT: db_con -- database handler
        OUTPUT: a id list consisting of fanpage posts published in recent 30 days
    """
    cursor = db_con.cursor()
    sql = "select p_id from fb_p_content where p_created_at<'2015-12-08 02:00'"
    try:
        cursor.execute(sql)
        recent_posts = list(str(e[0]) for e in cursor.fetchall())
        return recent_posts
    except Exception as e:
        print 'Error@get_recent_posts(): %s!' %e
        
def get_recent_posts(db_con):
    """
        INPUT: db_con -- database handler
        OUTPUT: a id list consisting of fanpage posts published in recent 30 days
    """
    cursor = db_con.cursor()
    sql = 'select p_id from fb_p_content where datediff(now(), p_created_at)<=30'
    try:
        cursor.execute(sql)
        recent_posts = list(str(e[0]) for e in cursor.fetchall())
        return recent_posts
    except Exception as e:
        print 'Error@get_recent_posts(): %s!' %e

def update_post_likes(recent_posts, db_con, api):
    """
        INPUT: recent_posts -- a id list consisting of fanpage posts published in recent 30 days
               db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: no return value
    """
    ###Get likes for each post
    new_likes = []
    for p_id in recent_posts:
        existing_likes = get_db_post_likes(p_id, db_con)    #a dictionary of likes
        fb_likes = get_post_new_likes(p_id, api, existing_likes)
        new_likes.extend(fb_likes)
    ###Write to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_p_likes(p_id, like_uid, like_uname) values(%s, %s, %s)'
        for e in new_likes:
            cursor.execute(sql, e)
            db_con.commit()
        print "inserted %d records to fb_p_likes" %len(new_likes) 
    except Exception as e:
        db_con.rollback()
        print 'Error in updating post likes to database@update_post_likes(): %s!' %e

def get_post_likes(post_id, api):
    """
        INPUT: post_id -- id of the focal post
               api -- Facebook Graph object
        OUTPUT: a list consisting of all likes of the post
    """
    ###Get like infor through Facebook Graph API
    try:
        post_obj = api.get_connections(id=post_id, connection_name='likes')
        return post_obj['data']
    except Exception as e:
        print 'Error in query for post likes with Facebook API@get_post_likes(): %s!' %e

def get_post_new_likes(post_id, api, existing_likes):
    """
        INPUT: post_id -- id of the focal post
               api -- Facebook Graph object
               existing_likes -- a dictionary of like records of post_id in database
        OUTPUT: a list consisting of new likes of the post (if the post does not exist, return an empty list)
    """
    ###Get like infor through Facebook Graph API
    try:
        like_obj = api.get_connections(id=post_id, connection_name='likes')
    except Exception as e:
        print 'Error in query for post likes with Facebook API@get_post_new_likes(): %s!' %e
        like_obj = {}
    new_likes = post_like_traversal(like_obj, existing_likes, post_id)
    return new_likes

def post_like_traversal(like_obj, existing_likes, post_id):
    """
        INPUT: like_obj -- Facebook Graph object containing post likes returned
               existing_likes -- a dictionary of like records of post_id in database
        OUTPUT: a list of new post likes
    """
    new_likes = []
    ###Process current like object
    if like_obj.has_key('data'):
        ###Add new likes to results list
        for e in like_obj['data']:
            if not existing_likes.has_key(e['id']):
                new_likes.append([post_id, e['id'], e['name']])
                # print 'new like: %s %s' %(e['id'], e['name'])
            else:
                return new_likes
        ###Visit next pages
        while(True):
            ###If paging infor is available
            if like_obj.has_key('paging'):
                ###If next page is available
                if like_obj['paging'].has_key('next'):
                    next_page = like_obj['paging']['next']
                    if next_page != '':
                        try:
                            ###Get next page
                            like_obj = requests.get(next_page).json()
                            time.sleep(1)
                        except Exception as e:
                            print 'Error in reading next page@post_like_traversal(): %s!' %e
                        ###Add new likes in next page to results list
                        if like_obj.has_key('data'):
                            ###Add new likes to results list
                            for e in like_obj['data']:
                                if not existing_likes.has_key(e['id']):
                                    new_likes.append([post_id, e['id'], e['name']])
                                    # print 'new like: %s %s' %(e['id'], e['name'])
                                else:
                                    return new_likes
                        ###Stop iteration if no new like
                        else:
                            break
                    ###Stop iteration if next page is empty
                    else:
                        break
                ###Stop iteration if there is no next page
                else:
                    break
            ###Stop iteration if there is no paging infor
            else:
                break
    ###Return results list
    return new_likes
    
    
def get_db_post_likes_cnt(post_id, db_con):
    """
        INPUT: post_id -- id of the focal post
               db_con -- database handler
        OUTPUT: the number of like records of post_id in database
    """
    try:
        cursor = db_con.cursor()
        sql = 'select count(*) from fb_p_likes where p_id = %s'
        cursor.execute(sql, (post_id, ))
        return cursor.fetchall()[0][0]
    except Exception as e:
        print 'Error in query database@get_db_post_likes_cnt(): %s!' %e

def get_db_post_likes(post_id, db_con):
    """
        INPUT: post_id -- id of the focal post
               db_con -- database handler
        OUTPUT: a dictionary of like records of post_id in database
    """
    try:
        cursor = db_con.cursor()
        sql = 'select like_uid, like_uname from fb_p_likes where p_id = %s'
        cursor.execute(sql, (post_id, ))
    except Exception as e:
        print 'Error in query database@get_db_post_likes(): %s!' %e
    return {str(e[0]):str(e[1]) for e in cursor.fetchall()}

def update_post_shares(recent_posts, db_con, api):
    """
        INPUT: recent_posts -- a id list fanpage posts published in recent 30 days
               db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: no return value
    """
    post_shares = []
    ###Get shares for each post
    for p_id in recent_posts:
        p_shares = get_shares_cnt(p_id, api)
        post_shares.append([p_id, p_shares])
        time.sleep(1)
    ###Write to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_p_shares(p_id, p_shares) values(%s, %s)'
        for e in post_shares:
            cursor.execute(sql, e)
            db_con.commit()
        print "inserted %d records to fb_p_shares" %len(post_shares) 
    except Exception as e:
        db_con.rollback()
        print 'Error in updating post shares to database@update_post_shares(): %s!' %e

def get_shares_cnt(post_id, api):
    """
        INPUT: post_id -- id of the focal post
               api -- Facebook Graph object
        OUTPUT: the number of shares received by the post (if the post does not exist, return -1)
    """
    ###Get shares number through Facebook Graph API
    try:
        req_fields = 'shares'
        post_obj = api.get_object(id=post_id, fields=req_fields)
        if post_obj.has_key('shares'):
            return post_obj['shares']['count']
        else:
            return 0
    except Exception as e:
        print 'Error in query for post shares with Facebook API@get_shares_cnt(): %s!' %e
        ###Help to detect the status of a post (existed or deleted)
        return -1


def new_comments_collector(post_ids, db_con, api):
    """
        INPUT: post_ids: a list consisting of all recent posts
               db_con -- database handler
               api -- Facebook Graph object 
        OUTPUT: no return value
    """
    print '==> collecting new comments to fanpage posts ...'
    comments2write = []
    ###Collect new comments of each post one by one
    for post in post_ids:
        ###Get the creation time of the latest comment of the focal post
        last_comment_time = get_last_comment_time(post, db_con)
        ###If it is the first time to collect comments, set a larger limit to reduce page numbers
        lmt = set_limit(last_comment_time)
        try:
            comments_obj = api.get_connections(id=str(post), connection_name='comments', limit = lmt)
        except Exception as e:
            print 'Error in query for post comments with Facebook API@new_comments_collector(): %s!' %e
            ###Assign an empty dict for deleted post
            comments_obj = {}
        comments_list = object_traversal(comments_obj, last_comment_time, 'comments')
        # print 'length of comments list = %d' %len(comments_list)
        comments_parsed = []
        for c in comments_list:
            comment = comment_parser(c, api)
            comment.insert(1, post)
            comments_parsed.append(comment)
        # comments_parsed = list((comment=comment_parser(c)).insert(1, post) for c in comments_list)
        # print comments_parsed[0]
        comments2write.extend(comments_parsed)
    ###Write parsed comments to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_c_content(c_id, p_id, c_uid, c_uname, c_message, c_created_at) values(%s, %s, %s, %s, %s, %s)'
        for e in comments2write:
            # print 'p_name: %s' %e[7]
            cursor.execute(sql, e)
        db_con.commit()
        print "inserted %d records into fb_c_content" %len(comments2write)
    except Exception as e:
        db_con.rollback()
        print 'Error in updating comments content to database@new_comments_collector(): %s!' %e
        
def get_last_comment_time(post_id, db_con):
    """
        INPUT: post_id -- id of the focal post
               db_con -- database handler
        OUTPUT: creation time of the latest comment to the focal post in database
    """
    ###get the creation time of the latest post on the focal fanpage
    cursor = db_con.cursor()
    sql = 'select max(c_created_at) from fb_c_content where p_id = %s'
    try:
        cursor.execute(sql, (post_id,))
        last_comment_time = cursor.fetchall()[0][0]
        if last_comment_time is None:
            last_comment_time = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        cursor.close()
        # print 'last_comment_time of %s = %s' %(post_id, last_comment_time)
        return last_comment_time
    except Exception as e:
        print 'Error@get_last_comment_time(): %s!' %e

def comment_parser(comment, api):
    """
        INPUT: comment -- json file of fanpage comment, to be parsed
        OUTPUT: a list consisting of uesful information of the focal comment
    """
    # print comment
    comment_id = comment['id']
    while(1):
        if comment.has_key('from'):
            comment_uid = comment['from']['id']
            comment_uname = comment['from']['name']
            break
        else:
            req_fields = 'from, created_time, message'
            comment = api.get_object(id=comment_id, fields=req_fields)    
    comment_created_at = datetime.strptime(comment['created_time'], '%Y-%m-%dT%H:%M:%S+0000').strftime('%Y-%m-%d %H:%M:%S')
    # comment_message = value_from_dict(comment, 'message').decode('utf-8')
    comment_message = value_from_dict(comment, 'message')
    cmt = [comment_id, comment_uid, comment_uname, comment_message, comment_created_at]
    # print cmt
    return cmt

def existing_comments_updater(db_con, api):
    """
        INPUT: db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: a list consisting of all comment ids that published in recent one month
    """
    print '==> updating existing comments ...'
    recent_comments = get_recent_comments(db_con)
    print '@existing_comments_updater: %d comments to update' %len(recent_comments)
    update_comment_likes(recent_comments, db_con, api)
    return recent_comments

def get_recent_comments(db_con):
    """
        INPUT: db_con -- database handler
        OUTPUT: a id list consisting of comments of the fanpage posts that were published in recent 30 days
    """
    cursor = db_con.cursor()
    sql = 'select c_id from fb_c_content where p_id in (select p_id from fb_p_content where datediff(now(), p_created_at)<=30)'
    try:
        cursor.execute(sql)
        recent_comments = list(str(e[0]) for e in cursor.fetchall())
        return recent_comments
    except Exception as e:
        print 'Error@get_recent_comments(): %s!' %e

def get_old_comments(db_con):
    """
        INPUT: db_con -- database handler
        OUTPUT: a id list consisting of comments of the fanpage posts that were published in recent 30 days
    """
    cursor = db_con.cursor()
    sql = "select c_id from fb_c_content where p_id in (select p_id from fb_p_content where p_created_at<= p_created_at<'2015-12-08 02:00')"
    try:
        cursor.execute(sql)
        recent_comments = list(str(e[0]) for e in cursor.fetchall())
        return recent_comments
    except Exception as e:
        print 'Error@get_recent_comments(): %s!' %e
        
def update_comment_likes(recent_comments, db_con, api):
    """
        INPUT: recent_comments -- a id list consisting of comments of the fanpage posts that were published in recent 30 days
               db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: no return value
    """
    ###Get likes for each post
    new_likes = []
    for c_id in recent_comments:
        # print 'requesting comment %d' %i
        c_likes_cnt = get_likes_cnt(c_id, api)
        # print 'comment %s likes: %d' %(c_id, c_likes_cnt)
        new_likes.append([c_id, c_likes_cnt])
        time.sleep(1)
    ###Write to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_c_likes(c_id, like_cnt) values(%s, %s)'
        for e in new_likes:
            cursor.execute(sql, e)
            db_con.commit()
        print "inserted %d records to fb_c_likes" %len(new_likes) 
    except Exception as e:
        db_con.rollback()
        print 'Error in updating comment likes to database@update_comment_likes(): %s!' %e

def get_likes_cnt(obj_id, api):
    """
        INPUT: obj_id -- id of Facebook Graph object (could be comment id or reply id)
               api -- Facebook Graph object
        OUTPUT: the number of likes the object has (if the object does not exist, return -1)
    """
    ###Get likes number through Facebook Graph API
    try:
        req_fields = 'like_count'
        obj = api.get_object(id=obj_id, fields=req_fields)
        return obj['like_count']
    except Exception as e:
        print 'obj_id = %s, Error in query for object likes with Facebook API@get_likes_cnt(): %s!' %(obj_id, e)
         ###Help to detect the status of a comment (existed or deleted)
        return -1


def new_replies_collector(comment_ids, db_con, api):
    """
        INPUT: comment_ids: a list consisting of the comments of all recent posts
               db_con -- database handler
               api -- Facebook Graph object 
        OUTPUT: no return value
    """
    print '==> collecting new replies to comments on fanpages ...'
    replies2write = []
    ###Collect new replies of each comment one by one
    for comment in comment_ids:
        ###Get the creation time of the latest replies of the focal comment
        last_reply_time = get_last_reply_time(comment, db_con)
        ###If it is the first time to collect replies, set a larger limit to reduce page numbers
        lmt = set_limit(last_reply_time)
        try:
            replies_obj = api.get_connections(id=comment, connection_name='comments', limit = lmt)
        except Exception as e:
            print 'comment_id = %s, Error in query for replies with Facebook API@new_replies_collector(): %s!' %(comment, e)
            ###Assign an empty dict
            replies_obj = {}
        time.sleep(1)
        replies_list = object_traversal(replies_obj, last_reply_time, 'replies')
        replies_parsed = []
        for r in replies_list:
            reply = comment_parser(r, api)
            reply.insert(1, comment)
            replies_parsed.append(reply)
        # replies_parsed = list(comment_parser(c).insert(1, comment) for c in replies_list)
        replies2write.extend(replies_parsed)
    ###Write parsed replies to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_r_content(r_id, c_id, r_uid, r_uname, r_message, r_created_at) values(%s, %s, %s, %s, %s, %s)'
        for e in replies2write:
            # print 'p_name: %s' %e[7]
            cursor.execute(sql, e)
        db_con.commit()
        print "inserted %d records into fb_r_content" %len(replies2write)
    except Exception as e:
        db_con.rollback()
        print 'Error in updating comments content to database@new_replies_collector(): %s!' %e

def get_last_reply_time(comment_id, db_con):
    """
        INPUT: comment_id -- id of the focal comment
               db_con -- database handler
        OUTPUT: creation time of the latest reply to the focal comment in database
    """
    ###get the creation time of the latest reply to the focal comment
    cursor = db_con.cursor()
    sql = 'select max(r_created_at) from fb_r_content where c_id = %s'
    try:
        cursor.execute(sql, (comment_id,))
        last_reply_time = cursor.fetchall()[0][0]
        if last_reply_time is None:
            last_reply_time = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        cursor.close()
        # print 'last_comment_time of %s = %s' %(post_id, last_comment_time)
        return last_reply_time
    except Exception as e:
        print 'Error@get_last_reply_time(): %s!' %e


def existing_replies_updater(db_con, api):
    """
        INPUT: db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: no return value
    """
    print '==> updating existing replies ...'
    recent_replies = get_recent_replies(db_con)
    print '@existing_replies_updater: %d replies to update' %len(recent_replies)
    update_reply_likes(recent_replies, db_con, api)

def get_recent_replies(db_con):
    """
        INPUT: db_con -- database handler
        OUTPUT: a id list consisting of comment replies of the fanpage posts that were published in recent 30 days
    """
    cursor = db_con.cursor()
    sql = 'select r_id from fb_r_content where c_id in (select t1.c_id from fb_c_content as t1,  fb_p_content as t2 where t1.p_id = t2.p_id and datediff(now(), p_created_at)<=30)'
    try:
        cursor.execute(sql)
        recent_replies = list(str(e[0]) for e in cursor.fetchall())
        return recent_replies
    except Exception as e:
        print 'Error@get_recent_replies(): %s!' %e

def update_reply_likes(recent_replies, db_con, api):
    """
        INPUT: recent_replies -- a id list consisting of comment replies to the fanpage posts that were published in recent 30 days
               db_con -- database handler
               api -- Facebook Graph object
        OUTPUT: no return value
    """
    ###Get likes for each post
    new_likes = [] 
    for r_id in recent_replies:
        r_likes_cnt = get_likes_cnt(r_id, api)
        new_likes.append([r_id, r_likes_cnt])
        time.sleep(1)
    ###Write to database
    try:
        cursor = db_con.cursor()
        sql = 'insert into fb_r_likes(r_id, like_cnt) values(%s, %s)'
        for e in new_likes:
            cursor.execute(sql, e)
            db_con.commit()
        print "inserted %d records to fb_r_likes" %len(new_likes) 
    except Exception as e:
        db_con.rollback()
        print 'Error in updating replies likes to database@update_reply_likes(): %s!' %e



