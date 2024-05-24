#import streamlit as st
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


def app():
    
    st.title("youtube data harvest")
    #establish db connection to mysql database
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        password='12345678',
        database='p1ydhaw'
    )#cursor to db connection
    mycur=mydb.cursor()

#########################################################

#options, Question list
    qu0="" #variable assign Question
    qu1="names of all videos and channels"
    qu2="top 10 most viewed videos and channels"
    qu3="comments count on each video, and video name"
    qu4="comments on each video and corresponding video name"
    qu5="videos with highest likes and corresponding channel name"
    qu6="count of likes and dislikes for each video and name"
    qu7="total number of views for each channel and name"
    qu8="names of all the channels that have published videos"
    qu9="duration of all videos by channel"
    qu10="highest number of video comments by channel"
    #select box with channel ID list
    queID = st.selectbox('question choice',[qu0,qu1,qu2,qu3,qu4,qu5,qu6,qu7,qu8,qu9,qu10])
    #queID = st.sidebar.selectbox("select question",(qu0,qu1,qu2,qu3))
    if queID==qu0:
        return 0
    #havest warehouse selected channel data stats
    if queID == qu1:
        #channel stats to table1
        query1='select t1.channelId,t1.channelName,t2.videoId,t2.videoName from test01 as t1 inner join test02 as t2 on t1.channelId=t2.channelId'
                
    if queID == qu2:
        query1='select channelId,totalvideos from test01 order by totalvideos desc'
                
    if queID == qu3:
        query1='select channelId,videoId,totalviews from test02 order by totalviews desc'
        
    if queID == qu4:
        query1='select channelId,videoId,commcount,videoName from test02 order by commcount desc'
        
    if queID == qu5:
        query1='select t1.channelId,t1.channelName,t2.videoId,t2.likecount from test01 as t1 inner join test02 as t2 on t1.channelId=t2.channelId order by t2.likecount desc'
    
    if queID == qu6:
        query1='select videoId,likecount,favcount,videoName from test02'
        
    if queID == qu7:
        query1='select channelId,channelName,totalviews from test01 order by totalviews desc'
        
    if queID == qu8:
        query1='select channelName, totalvideos from test01'
        
    if queID == qu9:
        query1='select channelId, videoId, duration from test02'
    
    if queID == qu10:
        query1='select channelId, max(commcount) from test02 group by channelId'
    
#########################################################
    mycur.execute(query1)
    result1 = mycur.fetchall()
    df1 = pd.DataFrame(result1,columns=mycur.column_names)
    st.dataframe(df1) 

    #query3='select * from test03'
    #mycur.execute(query3)
    #result3 = mycur.fetchall()
    #df3 = pd.DataFrame(result3,columns=mycur.column_names)
    #st.dataframe(df3)
    
    #close db cursor connection
    mycur.close()
    mydb.close()