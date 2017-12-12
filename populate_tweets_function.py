# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 09:30:09 2016

@author: Timothy

Revised: Matt
"""

from __future__ import absolute_import, print_function
import tweepy
import time
import sys
import mysql.connector as mdb


def populate(consumer_key, consumer_secret, access_token, access_token_secret, table_name, order='ASC'):

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.secure = True
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    # If the authentication was successful, you should
    # see the name of the account print out
    # print(api.me().name)
    # print(api.get_status)

    time.sleep(2)
    limit = api.rate_limit_status()
    remain = limit['resources']['statuses']['/statuses/lookup']['remaining']
    if remain == 0:
        print("-", end=' ', flush=True)
        return 60

    if not (order == 'ASC' or order == 'DESC'):
        order = 'ASC'

    cnx = mdb.connect(host='localhost',
                      user='root',
                      password='yaochen',
                      database='test_db_twitter',
                      use_unicode=True)
    cur = cnx.cursor()
    cur.execute("SELECT tweetid FROM " + table_name +
                " WHERE checked = 0 ORDER BY tweetid " + order +
                " LIMIT 180")
    tweetids = cur.fetchall()
    if not tweetids:
        print("Empty set...")
        return 0

    # for row in rows:
    #     tweetids.append(row)
        #print (row)
    # print(tweetids)

    # for tid in tweetids:
    #     cur = con.cursor()
    #     cur.execute("UPDATE " + table_name + " SET checked=%s WHERE tweetid=%s ", (True, tid[0]))

    # 862328512405004288, 872408494934065152
    #tweetids = [('872083936788312064',)]
    valid = False
    checked_ids = []

    for tid in tweetids:
        print("\n")
        print(tid[0])
        try:
            response = api._statuses_lookup(tid[0], tweet_mode='extended')
            if response:
                try:
                    try:
                        ent = response[0].entities
                    except Exception as e:
                        print("Error: {}".format(e.args[0]))
                        continue

                    if response[0].coordinates:
                        geo = response[0].coordinates['coordinates']
                        lat = geo[0]
                        lon = geo[1]
                    else:
                        geo = ''
                        lat = ''
                        lon = ''

                    #If the tweet is a response to an other tweet
                    if response[0].in_reply_to_status_id:
                        reply_to_tweet_id = response[0].in_reply_to_status_id_str
                        reply_to_tweet_user_id = response[0].in_reply_to_user_id_str
                    else:
                        reply_to_tweet_id = ''
                        reply_to_tweet_user_id = ''

                    #If the tweet is a quote tweet
                    try:
                        #quote=response[0].quoted_status
                        quote_id = response[0].quoted_status_id_str
                    except Exception as e:
                        print("Error: {}".format(e.args[0]))
                        quote_id = ''

                    text = response[0].full_text.encode('ascii', 'ignore')
                    truncated = response[0].truncated

                    try:
                        like_count = response[0].favorite_count
                    except Exception as e:
                        print("Error: {}".format(e.args[0]))
                        like_count = ''

                    try:
                        sensitive_url = response[0].possibly_sensitive
                    except Exception as e:
                        print("Error: {}".format(e.args[0]))
                        sensitive_url = False

                    timestamp = response[0].created_at
                    retweet_count = response[0].retweet_count
                    source = response[0].source.encode('ascii', 'ignore')
                    lang = response[0].lang

                    if response[0].place:
                        place = response[0].place
                        place1 = place.full_name.encode('ascii', 'ignore')
                        country_code = place.country_code
                        place_id = place.id
                    else:
                        place1 = ''
                        country_code = ''
                        place_id = ''

                    # User Info
                    user = response[0].user
                    user_contributors_enabled = user.contributors_enabled
                    user_default_profile = user.default_profile
                    user_default_profile_image = user.default_profile_image
                    user_description = user.description.encode('ascii', 'ignore')
                    user_geo_enabled = user.geo_enabled
                    user_is_translator = user.is_translator
                    user_listed_count = user.listed_count
                    user_notification = user.notifications
                    user_profile_background_color = user.profile_background_color
                    try:
                        user_profile_background_image_url = user.profile_background_image_url
                        user_profile_background_image_url_https = user.profile_background_image_url
                    except Exception as e:
                        print("Error: {}".format(e.args[0]))
                        user_profile_background_image_url = ''
                        user_profile_background_image_url_https = ''
                    user_profile_background_tile = user.profile_background_tile

                    user_profile_image_url = user.profile_image_url
                    user_profile_image_url_https = user.profile_image_url_https
                    user_profile_link_color = user.profile_link_color
                    user_profile_sidebar_border_color = user.profile_sidebar_border_color
                    user_profile_sidebar_fill_color = user.profile_sidebar_fill_color
                    user_profile_text_color = user.profile_text_color
                    user_profile_use_background_image = user.profile_use_background_image
                    user_protected = user.protected
                    try:
                        user_profile_banner_url = user.profile_banner_url
                    except Exception as e:
                        print("Error: {}".format(e.args[0]))
                        user_profile_banner_url = ''

                    if user.time_zone:
                        user_time_zone = user.time_zone
                    else:
                        user_time_zone = ''

                    if user.url:
                        user_url = user.url
                    else:
                        user_url = ''

                    if user.utc_offset:
                        user_utc_offset = user.utc_offset
                    else:
                        user_utc_offset = 0

                    user_verified = user.verified
                    userlocation = response[0].user.location.encode('ascii', 'ignore')

                    userid = response[0].user.id_str
                    username = response[0].user.name.encode('ascii', 'ignore')
                    screenname = response[0].user.screen_name.encode('ascii', 'ignore')
                    userlang = response[0].user.lang
                    statusescount = response[0].user.statuses_count
                    userfollowers = response[0].user.followers_count
                    userfav = response[0].user.favourites_count
                    friends = response[0].user.friends_count
                    accountcreated = response[0].user.created_at

                    rt_tweetid = 0
                    rt_userid = 0
                    if hasattr(response[0], 'retweeted_status'):
                        print('Retweetd from ')
                        print(response[0].retweeted_status)
                        #retweet=1
                        #RT_created_at=response[0].retweeted_status.created_at
                        rt_tweetid = response[0].retweeted_status.id
                        rt_userid = response[0].retweeted_status.user.id
                        print("Retweet id")
                        print(rt_tweetid)

                    print(userid)
                    hashtags = ''
                    text_url = ''
                    text_display_url = ''
                    if ent:
                        try:
                            ht_ls = list(map(lambda x: str(x['text'].encode('ascii', 'ignore')), ent['hashtags']))
                            hashtags = ', '.join(ht_ls)
                        except Exception as e:
                            print("Error: {}".format(e.args[1]))
                            #continue
                        try:
                            url_ls = list(map(lambda x: str(x['url'].encode('ascii', 'ignore')), ent['urls']))
                            text_url = ', '.join(url_ls)
                            d_url_ls = list(map(lambda x: str(x['display_url'].encode('ascii', 'ignore')), ent['urls']))
                            text_display_url = ', '.join(d_url_ls)
                        except Exception as e:
                            print("Error: {}".format(e.args[0]))
                    else:
                        pass

                    #con.set_character_set('utf-8')
                    cur.execute('SET NAMES utf8mb4;')
                    cur.execute('SET CHARACTER SET utf8mb4 ;')
                    cur.execute('SET character_set_connection=utf8mb4 ;')
                    try:
                        valid = True
                        print("i'm here")
                        cur.execute("UPDATE " + table_name +
                                    " SET userid = %s, timestamp = %s, gmt_date = STR_TO_DATE(%s, '%Y-%m-%d %T'),"
                                    " language = %s, text = %s, hashtags = %s, text_url = %s, text_display_url = %s,"
                                    " lat = %s, lon = %s, retweetedcount = %s, tweetplace = %s, country_code = %s,"
                                    " place_id = %s, geolocation = %s, reply_to_tweet_id = %s,"
                                    " reply_to_tweet_user_id = %s, quote_id = %s, truncated = %s, like_count = %s,"
                                    " sensitive_url = %s, source = %s, retweet_tid = %s, retweet_user_id = %s,"
                                    " validated = %s, checked = %s WHERE tweetid = %s",
                                    (userid, timestamp, timestamp, lang, text, hashtags, text_url, text_display_url,
                                     str(lat), str(lon), retweet_count, place1, country_code, place_id, str(geo),
                                     reply_to_tweet_id, reply_to_tweet_user_id, quote_id, truncated, like_count,
                                     sensitive_url, source, rt_tweetid, rt_userid, True, True, tid[0]))
                        cnx.commit()
                        print("tweet table ok")
                        checked_ids.append(tid[0])
                    except Exception as e:
                        print("Error: {}".format(e.args[1]))
                        continue
                    cnx.commit()

                    try:
                        cur.execute("INSERT INTO user "
                                    "(userid, account_createdat, username, userdisplayname, userlocation,"
                                    " userstatusescount, usernumfollowers, usernumfriends, userfavcount, user_lang,"
                                    " user_listed_count, user_protected, user_description, user_geo_enabled,"
                                    " user_time_zone, user_url, user_utc_offset, user_verified,"
                                    " user_contributors_enabled, user_default_profile, user_default_profile_image,"
                                    " user_is_translator, user_notification, user_profile_background_color,"
                                    " user_profile_background_image_url, user_profile_background_image_url_https,"
                                    " user_profile_background_tile, user_profile_banner_url, user_profile_image_url,"
                                    " user_profile_image_url_https, user_profile_link_color,"
                                    " user_profile_sidebar_border_color, user_profile_sidebar_fill_color,"
                                    " user_profile_text_color, user_profile_use_background_image)"
                                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
                                    " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (userid, accountcreated, username, screenname, userlocation,
                                     statusescount, userfollowers, friends, userfav, userlang,
                                     user_listed_count, user_protected, user_description, user_geo_enabled,
                                     user_time_zone, user_url, user_utc_offset, user_verified,
                                     user_contributors_enabled, user_default_profile, user_default_profile_image,
                                     user_is_translator, user_notification, user_profile_background_color,
                                     user_profile_background_image_url, user_profile_background_image_url_https,
                                     user_profile_background_tile, user_profile_banner_url, user_profile_image_url,
                                     user_profile_image_url_https, user_profile_link_color,
                                     user_profile_sidebar_border_color, user_profile_sidebar_fill_color,
                                     user_profile_text_color, user_profile_use_background_image))
                        cnx.commit()
                        print("user table ok")
                    except Exception as e:
                        if e.args[0] == 1062:
                            print("user already known")
                        else:
                            print("Error: {}".format(e.args[1]))
                except Exception as e:
                    print("BBBBBBBig Error %d:%s" % (e.args[0], e.args[1]))

            else:
                print("hello")
                valid = True
                try:
                    print("No response")
                    cur.execute("INSERT INTO " + table_name + "_noresponse (tweetid) VALUES (%s)", (tid[0],))
                    cnx.commit()
                except Exception as e:
                    print("Error: {}".format(e.args[1]))
                try:
                    cur.execute("UPDATE " + table_name + " SET checked=%s, validated=%s WHERE tweetid=%s",
                                (True, True, tid[0]))
                    cnx.commit()
                    print(" record updated")
                except Exception as e:
                    print("Error: {}".format(e.args[1]))
                    continue
            if not valid:
                try:
                    cur.execute("UPDATE " + table_name + " SET checked=%s WHERE tweetid=%s", (False, tid[0]))
                    cnx.commit()
                except Exception as e:
                    print("Error: {}".format(e.args[1]))
        except:
            print('Not checked')
            if tid not in checked_ids:
                try:
                    cur.execute("UPDATE " + table_name + " SET checked=%s WHERE tweetid=%s", (False, tid[0]))
                    cnx.commit()
                except Exception as e:
                    print("Error: {}".format(e.args[1]))
    cur.close()
    cnx.close()

    return 10


if __name__ == '__main__':

    consumer_key = 'U8dH6h2iVgTLSECPvUXIV1FaN'
    consumer_secret = 'ucTdq3RhT5Z8qOoaNdMHjWftN7Mrw4XIcl7Hjfg7k8ogqlhoZ4'
    access_token = '76708810-8yujmq9H9Yn1onQm06Crew47QDP0uxJJITbtKTe7A'
    access_token_secret = 'JwWCCHipSsCvCATLAkJsvmXZFGcXtJxDrWencpOUzR1Bs'

    populate(consumer_key, consumer_secret, access_token, access_token_secret, 'fin_tech', 'DESC')
