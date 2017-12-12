# -*- coding: utf-8 -*-
"""
    Created on Tue Apr 19 10:36:12 2016
    
    @author: Timothy
    """
import time
from populate_tweets_function import populate

#39
# == OAuth Authentication ==
#
# This mode of authentication is the new preferred way
# of authenticating with Twitter.

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")
consumer_key = 'U8dH6h2iVgTLSECPvUXIV1FaN'
consumer_secret = 'ucTdq3RhT5Z8qOoaNdMHjWftN7Mrw4XIcl7Hjfg7k8ogqlhoZ4'

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located
# under "Your access token")
access_token = '76708810-8yujmq9H9Yn1onQm06Crew47QDP0uxJJITbtKTe7A'
access_token_secret = 'JwWCCHipSsCvCATLAkJsvmXZFGcXtJxDrWencpOUzR1Bs'
flag = 10
while flag:
    try:
        flag = populate(consumer_key, consumer_secret, access_token, access_token_secret, 'fin_tech', 'DESC')
    except Exception as e:
        print("Error: {}".format(e.args[0]))
    time.sleep(flag)
