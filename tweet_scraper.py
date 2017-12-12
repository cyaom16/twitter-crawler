# -*- coding: utf-8 -*-
"""
Created on Mon Apr 04 20:14:32 2016

@author: Timothy

Revised: Matt (Python 2.7)
"""

# import requests
# import re
# import datetime
# import urllib2
import os
import csv
import sys
import json
import time
import httplib
import mysql.connector as mdb
from bs4 import BeautifulSoup


def scrape(search_req='', search_name=''):

    print("Connecting to database...")
    cnx = mdb.connect(host='localhost',
                      user='root',
                      password='yaochen',
                      database='test_db_twitter')
    cur = cnx.cursor()

    table_name = search_name + '_tweetids'
    try:
        print("Creating table {}: ".format(table_name))
        query = "CREATE TABLE {} (tweetid BIGINT NOT NULL, PRIMARY KEY(tweetid)) ENGINE=InnoDB".format(table_name)
        cur.execute(query)
        cnx.commit()
    except mdb.Error as e:
        if e.args[0] == 1050:
            print("already exists.")
        else:
            print(e.args[1])

    conn = httplib.HTTPSConnection('twitter.com')
    # tw_id_list = []
    tid_set = set()
    request_next = ''
    tmp_log_csv = 'tmp_log_{}.csv'.format(search_name)

    if not os.path.isfile(tmp_log_csv):
        request_base = '/search?q={}&lang=en'.format(search_req)
        print(request_base)

        conn.request('GET', request_base)
        res = conn.getresponse()

        soup = BeautifulSoup(res.read(), 'lxml')
        li_tag = soup.find_all('li', class_='js-stream-item stream-item stream-item ')
        tid_subset = set(map(lambda li: li.get('data-item-id'), li_tag))
        tid_set.update(tid_subset)

        try:
            subquery = ', '.join(['(%s)'] * len(tid_subset))
            # cnx.raise_on_warnings = True
            query = "INSERT IGNORE INTO {0} (tweetid) VALUES {1}".format(table_name, subquery)
            cur.execute(query, tuple(tid_subset))
            cnx.commit()
        except mdb.Error as e:
            print("Error {}".format(e.args[1]))

        # tuples = 0
        # for li in li_tag:
        #     tid = li.get('data-item-id')
        #     tid_set.add(tid)
        #     #tw_id_list.append(tw_id)
        #     #print(tw_id)
        #
        #     try:
        #         query = "INSERT INTO " + table_name + " (tweetid) VALUES (%s)"
        #         cur.execute(query, (tid,))
        #         cnx.commit()
        #         tuples += 1
        #     except mdb.Error as e:
        #         if e.args[0] == 1062:
        #             print("ID already exists.")
        #         else:
        #             print(e.args[1])
        # print(time.time() - t0)
        # return
        print("# Tweets unique: ", len(tid_set))
        #print("Inserted {0} values in {1}".format(tuples, table_name))

        request_next = ('/i/search/timeline?vertical=default&q={0}&src=typd&include_available_features=1&'
                        'include_entities=1&max_position=TWEET-{1}-{2}-'
                        'BD1UO2FFu9QAAAAAAAAETAAAAAcAAAASAAAEAAAAACCAAAAAAAQgAAAAAAEAAAgAAAAAAiAAAAAgQE'
                        'AAAAAAAAAAAAAAAIAAEAQQAAAAACAAAAAAAAAAAAAAAAAAAAICAAQQAAAAACAAAAAAQAABAgAABCAAAAAAAAAAAg'
                        'AAAAAAAIAAAAAAAAEACAAAAAAAABQIAAAAAAQAAAAAAAAAAAAAAAAAAAAA&reset_error_state=false'
                        .format(search_req, min(tid_set), max(tid_set)))
        print("TWEET-{0}-{1}".format(min(tid_set), max(tid_set)))
    else:
        print("Resuming last search...")
        with open(tmp_log_csv, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                request_next = row['Request_Next']
                log_search_name = row['Search_Name']
                log_time = row['Time']
                if search_name != log_search_name:
                    print("The search name {} in log file does not match yours...".format(log_search_name))
                    print("Last log time: {}".format(log_time))
                    sys.exit(0)
        f.close()

    counter = 0
    pre_len = 0
    json_valid = True

    while counter < 3 and json_valid:
        # complete = 'https://twitter.com' + request_next
        # print(complete)
        json_valid = True
        conn.request('GET', request_next)
        res = conn.getresponse()
        try:
            parsed = json.loads(res.read())
        except Exception as e:
            json_valid = False
            print("Error:".format(e.args[0]))

        request_next = ('/i/search/timeline?vertical=default&q={0}&src=typd&include_available_features=1&'
                        'include_entities=1&max_position={1}&reset_error_state=false'
                        .format(search_req, parsed['min_position']))

        soup = BeautifulSoup(parsed['items_html'], 'lxml')

        li_tag = soup.find_all('li', class_='js-stream-item stream-item stream-item ')
        tid_subset = set(map(lambda li: li.get('data-item-id'), li_tag))
        tid_set.update(tid_subset)

        try:
            subquery = ', '.join(['(%s)'] * len(tid_subset))
            query = "INSERT IGNORE INTO {0} (tweetid) VALUES {1}".format(table_name, subquery)
            cur.execute(query, tuple(tid_subset))
            cnx.commit()
        except Exception as e:
            print("Error {0}".format(e.args[1]))

        # tuples = 0
        # for li in li_tag:
        #     tid = li.get('data-item-id')
        #     tid_set.add(tid)
        #     # tw_id_list.append(tw_id)
        #     # print(tw_id)
        #
        #     try:
        #         query = "INSERT INTO " + table_name + " (tweetid) VALUES (%s)"
        #         cur.execute(query, (tid,))
        #         cnx.commit()
        #         tuples += 1
        #     except mdb.Error as e:
        #         if e.args[0] == 1062:
        #             print("ID {} already exists.".format(tid))
        #         else:
        #             print(e.args[1])
        # tw_id_set = set(tw_id_list)
        # print("# Tweets: ", len(tw_id_list))

        tid_set_len = len(tid_set)
        print("# Tweets unique: ", tid_set_len)
        # print("Inserted {0} values in {1}".format(tuples, table_name))
        print(parsed['min_position'])

        if tid_set_len == pre_len:
            counter += 1
        pre_len = tid_set_len

        with open(tmp_log_csv, 'w') as f:
            field_names = ['Time', 'Search_Name', 'Request_Next']
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            writer.writerow({'Time': time.asctime(time.localtime()),
                             'Search_Name': search_name,
                             'Request_Next': request_next})

    cur.close()
    cnx.close()
    if counter >= 3:
        print("Done! Clean up temporary log file...")
        os.remove(tmp_log_csv)

    return tid_set


# def insert_table(list_tid, name):
#     TABLES = {}
#     table_name = name + '_tweetids'
#
#     TABLES[table_name] = ("CREATE TABLE " + table_name +
#                           " (tweetid BIGINT NOT NULL, PRIMARY KEY(tweetid)) "
#                           "ENGINE=InnoDB")
#
#     print ""
#     cnx = mdb.connect(host="localhost",
#                       port=3306,
#                       user="root",
#                       password="00990628",
#                       database="test_db_twitter")
#                       #unix_socket='/tmp/mysql_default3059569.sock')
#     cursor = cnx.cursor()
#
#     for name, ddl in TABLES.iteritems():
#         try:
#             print "Creating table {}: ".format(name),
#             cursor.execute(ddl)
#         except mdb.Error as err:
#             if err.args[0] == 1050:
#                 print "already exists."
#             else:
#                 print err.args[1]
#         else:
#             print "OK"
#
#         print ''
#     tuples = 0
#     for tid in list_tid:
#         query = "INSERT INTO " + table_name + " (tweetid) VALUES (%s)"
#         try:
#             cursor.execute(query, (int(tid),))
#             tuples += 1
#         except mdb.Error as e:
#             print "Error %d: %s" % (e.args[0], e.args[1])
#             continue
#     print len(list_tid)
#     cnx.commit()
#     print "Inserted %d values in %s" % (tuples, table_name)
#
#     cursor.close()
#     cnx.close()


if __name__ == "__main__":
    keyword = sys.argv[1]
    keyword_format = keyword.replace(' ', '%20')

    if len(sys.argv) == 2:
        year_from = ''
        month_from = ''
        day_from = ''
        year_to = ''
        month_to = ''
        day_to = ''
        token = False
    else:
        year_from = sys.argv[2]
        month_from = sys.argv[3]
        day_from = sys.argv[4]
        year_to = sys.argv[5]
        month_to = sys.argv[6]
        day_to = sys.argv[7]
        token = True

    # search_req = ''
    search_req = ('Fintech OR insurtech OR blockchain OR bitcoin OR blockchaintechnology OR cryptocurrency OR '
                  'cryptocurrencies OR "block chain" OR "crypto currency" OR epayments OR mpayments OR '
                  'mobilepayments OR digitalpayments OR "Fin Tech" OR "Fin Techs" OR Fintechs OR RegTechs OR '
                  'RegTech OR digitalcurrency OR wealthtech OR crowdfunding OR roboadvisors OR roboadvisor')

    # search_req += keyword_format
    search_req = search_req.replace(' ', '%20')
    if token:
        search_req += '%20since%3A'
        search_req += year_from + '-'
        search_req += month_from + '-'
        search_req += day_from + '%20until%3A'
        search_req += year_to + '-'
        search_req += month_to + '-'
        search_req += day_to

    clean_keyword = search_req.replace('\"', '').replace('\'', '')\
        .replace('%20', '_').replace('%3A', '_').replace('-', '_')
    name = keyword_format.replace('%20', '_') + '_' + '_' + year_to

    _ = scrape(search_req, name)
    #insert_table(list_tid, name)

