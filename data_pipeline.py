from apiclient.discovery import build
import pandas as pd
import psycopg2
from credentials import *
import re
import datetime

class youtubeWrapper:
    def __init__(self, youtube_key,db_host=None,db_port=None,database=None,db_user=None,db_password=None):
        '''
        input: youtube_key: youtube developer key
               db_host: host of database
               db_port: port of database
               database:name of data base
               db_user:user of database
               db_password: password of databse
        '''
        self.build = build('youtube', 'v3',developerKey=youtube_key)
        self.remaining_quota=10000
        try:
            self.conn=psycopg2.connect(host=db_host,port=db_port,database=database,user=db_user,password=db_password)
            self.cursor=self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def get_search_results(
            self,part='id,snippet', page_token=None, keyword=None, max_results=None,
            topic_id=None, published_after=None, published_before=None,
            type='video', order='date',relevanceLanguage='en'):
            '''getting search result from youtube api '''


            search_results = self.build.search().list(part=part,
                pageToken=page_token, q=keyword, type=type, order=order,
                relevanceLanguage=relevanceLanguage,maxResults=max_results,
                topicId=topic_id, publishedBefore=published_before,
                publishedAfter=published_after).execute()
            self.remaining_quota-=100
            nextPageToken = search_results['nextPageToken']
            return search_results,nextPageToken

    def create_table(self, query):
        '''create a table'''
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insert_video_search(self, table_name,search_results):
        try:
            for item in search_results['items']:
                title=item['snippet']['title']
                description=item['snippet']['description']
                channelId=item['snippet']['channelId']
                publishedAt=item['snippet']['publishedAt']
                videoId=item['id']['videoId']
                self.cursor.execute("INSERT INTO %s (video_id,title,description,\
                                    channel_id,publishedAt)VALUES(%%s,%%s,%%s,%%s,%%s)"
                                    %table_name, [videoId,title,description,channelId,publishedAt])
                self.conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def get_video_results(self,video_id,part):
        video_results=self.build.videos().list(part=part,id=video_id) .execute()
        if 'contentDetails' in part:
            self.remaining_quota-=7
        else:
            self.remaining_quota-=5
        return video_results

    def insert_video_detial(self,table_name,video_results):
        if video_results['items']!=[]:
            video_id=video_results['items'][0]['id']
            if 'contentDetails' in video_results['items'][0]:
                duration = video_results ['items'][0]['contentDetails']['duration']
            else:
                duration=None
            channelTitle = video_results ['items'][0]['snippet']['channelTitle']
            categoryId = video_results ['items'][0]['snippet']['categoryId']
            viewCount = video_results ['items'][0]['statistics']['viewCount']
            likeCount = video_results ['items'][0]['statistics'].get('likeCount', None)
            dislikeCount = video_results ['items'][0]['statistics'].get('dislikeCount', None)
            commentCount = video_results ['items'][0]['statistics'].get('commentCount',None)
            tags = video_results ['items'][0]['snippet'].get('tags', [])
            self.cursor.execute(" UPDATE %s \
                                  SET channel_title = %%s,\
                                      category_id = %%s, \
                                      view_count = %%s, \
                                      like_count = %%s, \
                                      dislike_count = %%s,\
                                      comment_count = %%s,\
                                      tags = %%s, \
                                      duration = %%s\
                                      WHERE video_id=%%s;" % table_name,
                                      [channelTitle,categoryId,viewCount,likeCount,\
                                      dislikeCount,commentCount,tags,duration,video_id])
            self.conn.commit()

