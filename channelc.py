#import Python library
import googleapiclient.discovery
import pandas as pd
import streamlit as st
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from streamlit_option_menu import option_menu
#youtube api service and api_key from console.developers.google.com
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyCqevk5cPt9q5mzfzjerCbmwi4-MlvahcE"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#def function extract youtube channel stats return as df
def get_channel_stats(youtube,channel_ids):
    chan_data = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_ids
        )
    response = request.execute()
    for ival in response['items']:
        cdata={'channelId':ival['id'],
               'channelName':ival['snippet']['title'],
               #'description':ival['snippet']['description'],
               'subscribers':ival['statistics']['subscriberCount'],
               'totalviews':ival['statistics']['viewCount'],
               'totalvideos':ival['statistics']['videoCount'],
               'playlistid':ival['contentDetails']['relatedPlaylists']['uploads']}
        chan_data.append(cdata)
    return(pd.DataFrame(chan_data))
#def function extract youtube video stats return as df
#get vids
def get_channel_vids(channel_ids):
    vid_ids = []
    reqt1 = youtube.channels().list(id=channel_ids,part="contentDetails").execute()
    playlist_id=reqt1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        reqt2 = youtube.playlistItems().list(playlistId=playlist_id,
                                             part="snippet",
                                             maxResults=50,
                                             pageToken=next_page_token).execute()
        for ival in range(len(reqt2['items'])):
            vid_ids.append(reqt2['items'][ival]['snippet']['resourceId']['videoId'])
            next_page_token = reqt2.get('nextPageToken')
        if next_page_token is None:
            break
    return vid_ids
#video stats
def get_video_stats(youtube,vid_ids,channel_ids): ###adding here
    vid_data = []
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=vid_ids
        )
    response = request.execute()
    for ival in response['items']:
        cdata={'videoId':ival['id'],
               'videoName':ival['snippet']['title'],
               'channelId':channel_ids,
               #'description':ival['snippet']['description'],
               'channelTitle':ival['snippet']['channelTitle'],
               #'tags':ival['snippet']['tags'],
               #'publishedAt':ival['snippet']['publishedAt'],
               'duration':ival['contentDetails']['duration'],
               #'suscribers':ival['statistics']['subscriberCount'],
               #'totalvideos':ival['statistics']['videoCount'],
               'totalviews':ival['statistics']['viewCount'],
               'likecount':ival['statistics']['likeCount'],
               ##'dislikecount':ival['statistics']['dislikeCount'],#dislikeCount
               'favcount':ival['statistics']['favoriteCount'],
               'commcount':ival['statistics']['commentCount']}
        vid_data.append(cdata)
    return(pd.DataFrame(vid_data))
#def function extract youtube comment stats return as df
#comment thread stats
def get_comment_tids(vid_ids):
    com_ids = []
    next_page_token = None
    while True:
        reqt2 = youtube.commentThreads().list(videoId=vid_ids,
                                             part="snippet",
                                             maxResults=50,
                                             pageToken=next_page_token).execute()
        for ival in range(len(reqt2['items'])):
            com_ids.append(reqt2['items'][ival]['id'])
            next_page_token = reqt2.get('nextPageToken')
        if next_page_token is None:
            break
    return com_ids
###
#comment stats
def get_comment_stats(youtube,comm_ids,vid_ids):
    com_data = []
    request = youtube.comments().list(
        part="snippet",
        id=comm_ids
        )
    response = request.execute()
    for ival in response['items']:
        cdata={'commId':ival['id'],
               'commText':ival['snippet']['textDisplay'],
               'authName':ival['snippet']['authorDisplayName'],
               'videoId':vid_ids}
               #'publishDt':ival['snippet']['publishedAt']}
        com_data.append(cdata)
    return(pd.DataFrame(com_data))

#def current page app()
def app():
    st.title("youtube data harvest")
    #establish db connection to mysql database
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        password='12345678',
        #database='p1ydhaw'
    )#cursor to db connection
    mycur=mydb.cursor()
    query='create database if not exists p1ydhaw'
    mycur.execute(query)
    query='use p1ydhaw'
    mycur.execute(query)
    mydb.commit()
    #mycur=mydb.cursor()
    ch0=""#variable assign channel ID
    ch1="UCjV6TXJ8YTIule0-IyrmY-Q"
    ch2="UCx671aLP-QXaDNg2XdkdnZg"
    ch3="UCydVjjfO74Vr7Scecn1sNAg"
    ch4="UCJVSs3f-sSMZcIV89Xs-R4A"
    ch5="UCJN3l7nQwqQIF2_RHfPxveg" ###"UCAuUUnT6oDeKwE6v1NGQxug"
    ch6="UC0SFwkGlQonlP69Lb9Gdb-w"
    ch7="UCUxc0iEpV8wZV4WLOui0RwQ"
    ch8="UCKZozRVHRYsYHGEyNKuhhdA"
    ch9="UCPxMZIFE856tbTfdkdjzTSQ"
    ch10="UCFsJ2qT0xzBtTvNrC7P-gEQ"
    ch11="All channels"
    ch = [ch1,ch2,ch3,ch4,ch5,ch6,ch7,ch8,ch9,ch10]
    #select box with channel ID list
    channID = st.selectbox('channelId',[ch0,ch11,ch1,ch2,ch3,ch4,ch5,ch6,ch7,ch8,ch9,ch10])
    if channID==ch0:
            return 0
    if channID == ch1:
        channel_ids=ch1        
        
    if channID == ch2:
        channel_ids=ch2
        
    if channID == ch3:
        channel_ids=ch3
    if channID == ch4:
        channel_ids=ch4
    if channID == ch5:
        channel_ids=ch5
    if channID == ch6:
        channel_ids=ch6
    if channID == ch7:
        channel_ids=ch7
    if channID == ch8:
        channel_ids=ch8
    if channID == ch9:
        channel_ids=ch9
    if channID == ch10:
        channel_ids=ch10
    if channID == ch11:
        channel_ids=ch11
#####################################################################################
        #drop if the db table exists
        query='DROP TABLE IF EXISTS test03'
        mycur.execute(query)
        query='DROP TABLE IF EXISTS test02'
        mycur.execute(query)
        query='DROP TABLE IF EXISTS test01'
        mycur.execute(query)
        mydb.commit()
        for n in ch:
            channel_ids=n
            #extract channel stats to dataframe
            df11=get_channel_stats(youtube,channel_ids)
            df11_copy = df11.copy() #copy
            df11_copy=df11_copy.astype({'subscribers':'int','totalviews':'int','totalvideos':'int'}) #'channelId':'str',
            
            conStr='mysql+pymysql://root:12345678@localhost/p1ydhaw'
            engine=create_engine(conStr)
            #warehouse df data to db table
            df11_copy.to_sql('test01',engine,if_exists='append',index=False)
            #extract video stats to dataframe
            df12=get_channel_vids(channel_ids)
            vid_ids = df12[1:5]
            for i in vid_ids:    
                df13=get_video_stats(youtube,i,channel_ids) ###adding here
                df13_copy = df13.copy() #copy            
                df13_copy=df13_copy.astype({'totalviews':'int','likecount':'int','favcount':'int','commcount':'int'}) #'dislikecount':'int',
                df13_copy.to_sql('test02',engine,if_exists='append',index=False)
            #extract comment stats to dataframe
            for i in vid_ids:
                df14=get_comment_tids(i)
                comm_ids=df14[1:5]
                for j in comm_ids:
                    df15=get_comment_stats(youtube,j,i)
                    df15_copy = df15.copy() #copy
                    #df15_copy=df15_copy.astype({'videoId':'text(55)'})
                    df15_copy.to_sql('test03',engine,if_exists='append',index=False)
        query='ALTER TABLE test01 MODIFY COLUMN channelId varchar(105)'
        mycur.execute(query)
        query='ALTER TABLE test01 ADD PRIMARY KEY (channelId)'
        mycur.execute(query)
        query='ALTER TABLE test02 MODIFY COLUMN channelId varchar(105)'
        mycur.execute(query)
        query='ALTER TABLE test02 MODIFY COLUMN videoId varchar(55)'
        mycur.execute(query)
        query='ALTER TABLE test02 ADD PRIMARY KEY (videoId)'
        mycur.execute(query)
        query='ALTER TABLE test02 ADD CONSTRAINT test02_chid FOREIGN KEY (channelId) REFERENCES test01(channelId) ON DELETE CASCADE'
        mycur.execute(query)
        query='ALTER TABLE test03 MODIFY COLUMN videoId varchar(55)'
        mycur.execute(query)
        query='ALTER TABLE test03 ADD CONSTRAINT test03_vid FOREIGN KEY (videoId) REFERENCES test02(videoId) ON DELETE CASCADE'
        mycur.execute(query)
        mycur.close()
        mydb.close()
        return 0



#####################################################################################
    #drop if the db table exists
    query='DROP TABLE IF EXISTS test03'
    mycur.execute(query)
    query='DROP TABLE IF EXISTS test02'
    mycur.execute(query)
    query='DROP TABLE IF EXISTS test01'
    mycur.execute(query)
    
#extract channel stats to dataframe
    df11=get_channel_stats(youtube,channel_ids)
    df11_copy = df11.copy() #copy
    df11_copy=df11_copy.astype({'subscribers':'int','totalviews':'int','totalvideos':'int'}) #'channelId':'str',
    
    conStr='mysql+pymysql://root:12345678@localhost/p1ydhaw'
    engine=create_engine(conStr)
    #warehouse df data to db table
    df11_copy.to_sql('test01',engine,if_exists='append',index=False)
    #df11_copy.to_sql('test01',engine,if_exists='replace',index=False)
    #modify datatype of table column and define primarykey
    query='ALTER TABLE test01 MODIFY COLUMN channelId varchar(105)'
    mycur.execute(query)
    query='ALTER TABLE test01 ADD PRIMARY KEY (channelId)'
    mycur.execute(query)
    #fetch data from db table to df and display on st dataframe
    query1='select * from test01'
    mycur.execute(query1)
    result1 = mycur.fetchall()
    df1 = pd.DataFrame(result1,columns=mycur.column_names)
    st.dataframe(df1)
#extract video stats to dataframe
    df12=get_channel_vids(channel_ids)
    vid_ids = df12[1:5]
    for i in vid_ids:    
        df13=get_video_stats(youtube,i,channel_ids) ###adding here
        df13_copy = df13.copy() #copy            
        df13_copy=df13_copy.astype({'totalviews':'int','likecount':'int','favcount':'int','commcount':'int'}) #'dislikecount':'int',
        df13_copy.to_sql('test02',engine,if_exists='append',index=False)
    ##modify datatype of table column and define primarykey
    query='ALTER TABLE test02 MODIFY COLUMN channelId varchar(105)'
    mycur.execute(query)
    query='ALTER TABLE test02 MODIFY COLUMN videoId varchar(55)'
    mycur.execute(query)
    query='ALTER TABLE test02 ADD PRIMARY KEY (videoId)'
    mycur.execute(query)
    query='ALTER TABLE test02 ADD CONSTRAINT test02_chid FOREIGN KEY (channelId) REFERENCES test01(channelId) ON DELETE CASCADE'
    mycur.execute(query)
    query2='select * from test02'
    mycur.execute(query2)
    result2 = mycur.fetchall()
    df2 = pd.DataFrame(result2,columns=mycur.column_names)
    st.dataframe(df2)
#extract comment stats to dataframe
    for i in vid_ids:
        df14=get_comment_tids(i)
        comm_ids=df14[1:5]
        for j in comm_ids:
            df15=get_comment_stats(youtube,j,i)
            df15_copy = df15.copy() #copy
            #df15_copy=df15_copy.astype({'videoId':'text(55)'})
            df15_copy.to_sql('test03',engine,if_exists='append',index=False)
    ###modify datatype of table column and define primarykey
    query='ALTER TABLE test03 MODIFY COLUMN videoId varchar(55)'
    mycur.execute(query)
    query='ALTER TABLE test03 ADD CONSTRAINT test03_vid FOREIGN KEY (videoId) REFERENCES test02(videoId) ON DELETE CASCADE'
    mycur.execute(query)
    query3='select * from test03'
    mycur.execute(query3)
    result3 = mycur.fetchall()
    df3 = pd.DataFrame(result3,columns=mycur.column_names)
    st.dataframe(df3)
    
    #close db cursor connection
    mycur.close()
    mydb.close()
    