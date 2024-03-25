#1
from googleapiclient.discovery import build
import pymongo
import mysql.connector
from pymongo import MongoClient
import pandas
import json
import re
from datetime import datetime, timedelta
import streamlit

#api_key_connection

def api_connection():
    api_Id="AIzaSyBfa7Ip12upXMRWnsVWQtGdogMpM5dH_fY"
    api_name="youtube"
    api_version="v3"
    
    yt_connection=build(api_name,api_version,developerKey=api_Id)
    
    return yt_connection

youtube=api_connection()


#2 getting channel information from the youtube
def get_channel_info(channel_id):
    
    request=youtube.channels().list(
                                    part="snippet,ContentDetails,statistics",
                                    id=channel_id)
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
              Channel_Id=i['id'],
              Subscribers=i['statistics']['subscriberCount'],
              Views=i['statistics']['viewCount'],
              Video_count=i['statistics']['videoCount'],
              Channel_description=i['snippet']['description'],
              Playlist_id=i['contentDetails']['relatedPlaylists']['uploads']) 
    return data
        

#3
#get_video_id
def get_video_ids(channel_id):
    
    video_ids=[]
    response=youtube.channels().list(part="ContentDetails",id=channel_id).execute()
        #playlist_id to get the video_id's
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        #video_id list
    next_page_token=None

    while True:

        video_response=youtube.playlistItems().list(
                                                    part='snippet',
                                                    playlistId=playlist_id,
                                                    maxResults=50,
                                                    pageToken=next_page_token).execute()
        for i in video_response['items']:
            video_ids.append(i['snippet']['resourceId']['videoId'])
        next_page_token=video_response.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
    

#4
#getting all video information
def all_video_information(video_IDs):
    
    #video_ids_s=get_video_ids(channel_id)
    all_video_info=[]
    for video_id in video_IDs:
        video_request=youtube.videos().list(part="snippet,ContentDetails,statistics",
                                           id=video_id).execute()
        
        for item in video_request["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                      Channel_Id=item['snippet']['channelId'],
                      Video_Id=item["id"],
                      Title=item['snippet']['title'],
                      Tags=item['snippet'].get('tags'),
                      Thumbnails=item['snippet']['thumbnails']['default']['url'],
                      Description=item['snippet']['localized']['description'],
                      Published_Date=item['snippet']['publishedAt'],
                      Duration=item['contentDetails']['duration'],
                      Views=item['statistics'].get('viewCount'),
                      Likes=item['statistics'].get('likeCount'),
                      Commentcount=item['statistics'].get('commentCount'),
                      FavoriteCount=item['statistics']['favoriteCount'],
                      Definition=item['contentDetails']['definition'],
                      Caption=item['contentDetails']['caption']
                     )
            all_video_info.append(data)
    return all_video_info

#5
#get playlist details
def get_all_playlistdetails(channel_id):
    all_playlist=[]

    next_page_token=None
    playlist_response = youtube.playlists().list( part='contentDetails,snippet',
                                                 channelId=channel_id,
                                                 maxResults=50,pageToken=next_page_token).execute()
    while True:

        for item in playlist_response['items']:
            data=dict(Playlist_Id=item['id'],
                     Title=item['snippet']['title'],
                     Channel_Id=item['snippet']['channelId'],
                     Channel_Name=item['snippet']['channelTitle'],
                     Published_Date=item['snippet']['publishedAt'],
                     Video_count=item['contentDetails']['itemCount'])
            all_playlist.append(data)
            next_page_token=playlist_response.get('nextPageToken')

        if next_page_token is None:
            break
    return all_playlist


#6
#get comment data
def get_comment_data(video_IDs):
    all_comment_info = []
    
    for video_id in video_IDs:
        try:
            comment_response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100
            ).execute()

            for item in comment_response['items']:
                data = {
                    'Comment_Id': item['snippet']['topLevelComment']['id'],
                    'Video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published': item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                all_comment_info.append(data)
        except Exception as e:
            pass
            
    
    return all_comment_info


#7
#mongodb connection and database creation    mongodb://localhost:27017
client =pymongo.MongoClient('mongodb://localhost:27017/')
db = client['Youtube_channel_data']

#8
#uploading all extracted data to mongodb
def channel_details(channel_id):
    channel_data=get_channel_info(channel_id)
    v_id=get_video_ids(channel_id)
    video_info=all_video_information(v_id)
    comment_data=get_comment_data(v_id)
    playlist_data=get_all_playlistdetails(channel_id)
    
    collection1=db['Channel_Info']
    collection1.insert_one({"Channel_Information":channel_data,
                            "Video_Info":video_info,
                            "Comment_Info":comment_data,
                            "Playlist_Info":playlist_data
                           })
    return "uploaded done"


#10
#creating channel table in mysql and removing the existing table each time

def channel_table(selected_channel):
    mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='guvi2023',
    auth_plugin='mysql_native_password',
    database="youtube_data"
    )
    #mycursor = mydb.cursor()

    try:
        mycursor = mydb.cursor()
        channel_query='''create table if not exists channels(
                                                    Channel_Name varchar(100),
                                                    Channel_Id varchar(100) primary key,
                                                    Subscriber bigint,
                                                    Views bigint,
                                                    Videos int,
                                                    Description text,
                                                    Playlist_Id varchar(100))'''
        mycursor.execute(channel_query)
        mydb.commit()
    except:
        print("table is existing already")
    
    #fetching the selected channel name from the user from the mysql
    
    ch_name='''select Channel_Name from channels; '''
    mycursor.execute(ch_name)
    ch_list=mycursor.fetchall()
    mydb.commit()


    chsqlname_list = list(map(lambda x: x[0], ch_list))
    
    if selected_channel in chsqlname_list:
        display= f"{selected_channel} is Already exists in MysqlDB"        
        return display
    
    else:
        def channel_sql_info():

            csql_list=[]
            db = client['Youtube_channel_data']
            collection1=db['Channel_Info']
            for ch_data in collection1.find({"Channel_Information.Channel_Name":selected_channel},{"_id":0}):
                csql_list.append(ch_data["Channel_Information"])
            df=pandas.DataFrame(csql_list)
            return df

        channel_values=channel_sql_info()
        for index,row in channel_values.iterrows():
            channel_insert_query='''insert into channels(Channel_Name,
                                                        Channel_Id,
                                                        Subscriber,
                                                        Views,
                                                        Videos,
                                                        Description,
                                                        Playlist_Id)

                                                        values(%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['Channel_Name'],row['Channel_Id'],row['Subscribers'],row['Views'],row['Video_count'],row['Channel_description'],row['Playlist_id'])


            try:
                mycursor.execute(channel_insert_query,values)
                mydb.commit()
            except:
                print("channels values are existing already")

#this func is used to fetch the data of selected row's in the mongodb
def sql_updation(table_update,selected_channel):
    sql_list=[]
    db = client['Youtube_channel_data']
    collection1=db['Channel_Info']
    for necessary_data in collection1.find({"Channel_Information.Channel_Name":selected_channel},{"_id":0}):
        sql_list.append(necessary_data[table_update])
    df=pandas.DataFrame(sql_list[0])
    return df                

#11
#creating table in mysql and removing the existing table each time for playlist

def playlist_table(selected_channel):
    mydb = mysql.connector.connect(
        
        host='localhost',
        user='root',
        password='guvi2023',
        auth_plugin='mysql_native_password',
        database="youtube_data"
      )
    mycursor = mydb.cursor()
  
    try:
    
        #mycursor = mydb.cursor()
        playlist_query='''create table if not exists playlists(
                                                Playlist_Id varchar(100) primary key,
                                                Title varchar(100),
                                                Channel_Id varchar(100),
                                                Channel_Name varchar(100),
                                                Published timestamp,
                                                Videos int
                                                )'''
        
        mycursor.execute(playlist_query)
        mydb.commit()
    except:
        print("table is existing already")
        
    
    Playlist_Info="Playlist_Info"
    playlist_values=sql_updation(Playlist_Info,selected_channel)
    
   
    for index,row in playlist_values.iterrows():
        playlist_insert_query='''insert into playlists(
                                                    Playlist_Id,
                                                    Title,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    Published,
                                                    Videos
                                                    )

                                                    values(%s,%s,%s,%s,%s,%s)'''
    
        published_date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')


        values=(row['Playlist_Id'],row['Title'],row['Channel_Id'],row['Channel_Name'],published_date,row['Video_count'])
        
        try:
            mycursor.execute(playlist_insert_query,values)
            mydb.commit()
        except:
            print("playlistID are already exists")
        


#12
#creating table in mysql and removing the existing table if it there each time for comments

def comment_table(selected_channel):
    mydb = mysql.connector.connect(
        
        host='localhost',
        user='root',
        password='guvi2023',
        auth_plugin='mysql_native_password',
        database="youtube_data"
      )
    mycursor = mydb.cursor()
   
    try:

        comment_query='''create table if not exists comments(
                                                            Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comments text,
                                                            Comment_Person varchar(50),
                                                            Published TIMESTAMP
                                                            )'''
        mycursor.execute(comment_query)
        mydb.commit()
        
    except:
        print("Comment table already exists")

    Comment_Info="Comment_Info"
    comment_values=sql_updation(Comment_Info,selected_channel)
    
   
    for index,row in comment_values.iterrows():
        comment_insert_query='''insert into comments(Comment_Id,
                                                     Video_Id,
                                                     Comments,
                                                     Comment_Person,
                                                     Published
                                                        )

                                                        values(%s,%s,%s,%s,%s)'''
        

        published = datetime.strptime(row['Comment_Published'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        values=(row['Comment_Id'],row['Video_Id'],row['Comment_text'],row['Comment_Author'],published)
        mycursor.execute(comment_insert_query,values)
        mydb.commit()
        


#14
#creating table in mysql and removing the existing table if it there each time for videos

def video_table(selected_channel):
    
    mydb = mysql.connector.connect(
        
        host='localhost',
        user='root',
        password='guvi2023',
        auth_plugin='mysql_native_password',
        database="youtube_data"
      )
    mycursor = mydb.cursor()

    try:

        videos_query='''create table if not exists videos(Channel_Name varchar(100),
                                                         Channel_ID varchar(100),
                                                         Video_Id varchar(50) primary key,
                                                         Title varchar(100),
                                                         Tags text,
                                                         Thumbnails varchar(200),
                                                         Description text,
                                                         Published TIMESTAMP,
                                                         Duration TIME,
                                                         Views bigint,
                                                         Likes bigint,
                                                         Commentcount bigint,
                                                         FavoriteCount int,
                                                         Definition varchar(10),
                                                         Caption varchar(50)
                                                            )'''
        mycursor.execute(videos_query)
        mydb.commit()
        
    except:
        print("video table already exists")

    
    Video_Info="Video_Info"
    video_values=sql_updation(Video_Info,selected_channel)
    
    for index,row in video_values.iterrows():
        videos_insert_query='''insert into videos(Channel_Name,
                                                     Channel_Id,
                                                     Video_Id,
                                                     Title,
                                                     Tags,
                                                     Thumbnails,
                                                     Description,
                                                     Published,
                                                     Duration,
                                                     Views,
                                                     Likes,
                                                     Commentcount,
                                                     FavoriteCount,
                                                     Definition,
                                                     Caption
                                                        )

                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        

        published = datetime.strptime(row['Published_Date'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        Tags=json.dumps(row['Tags'])
        duration=row['Duration']    
        pattern = r'PT(?:(\d+)M)?(?:(\d+)S)?'
        match = re.match(pattern, duration)
        if match:
            minutes = int(match.group(1) or 0)
            seconds = int(match.group(2) or 0)
        total_seconds = minutes * 60 + seconds
        formatted_duration = '{:02d}:{:02d}:00'.format(total_seconds // 3600, (total_seconds % 3600) // 60)

        values=(row['Channel_Name'],row['Channel_Id'],row['Video_Id'],row['Title'],Tags,row['Thumbnails'],row['Description'],published,
               formatted_duration,row['Views'],row['Likes'],row['Commentcount'],row['FavoriteCount'],row['Definition'],row['Caption'])
        mycursor.execute(videos_insert_query,values)
        mydb.commit()
        
        
        
#15 all table functions
def tables(selected_channel):
    display=channel_table(selected_channel)
    #checking only the change_table to verify whether the channel was already in mysqlDB or not. (display variable)
    if display:
        return display
    else:
        
        playlist_table(selected_channel)
        comment_table(selected_channel)
        video_table(selected_channel)
        
    return "updated successfully"


#16 data for streamlit


def channel_streamlit():
    chsql_list=[]
    db = client['Youtube_channel_data']
    collection1=db['Channel_Info']
    for ch_data in collection1.find({},{"_id":0,"Channel_Information":1}):
        chsql_list.append(ch_data["Channel_Information"])
    df=streamlit.dataframe(chsql_list)
    return df
   


#16 UI for streamlit

with streamlit.sidebar:

    streamlit.title(":blue[Data Process]")

    streamlit.header(":red[Data Collection]")
    
    channel_id=streamlit.text_input("","Channel ID")
    #mongoDB button process
    if streamlit.button("MongoDB"):
        
        ch_ids=[]
        db = client['Youtube_channel_data']
        collection1=db['Channel_Info']
        for ch_data in collection1.find({},{"_id":0,"Channel_Information":1}):
            
            ch_ids.append(ch_data["Channel_Information"]['Channel_Id'])
        try:     
            if channel_id in ch_ids:
                streamlit.success("channels details already exist")
            else:
                st_channel_id_insert=channel_details(channel_id)
                streamlit.success(st_channel_id_insert)
        except KeyError:
            streamlit.success("ChannelID Field is Empty")

    streamlit.header(":red[Data Migrarion]")
    #data selection process
    channel_names=[]
    collection1=db['Channel_Info']
    for ch_data in collection1.find({},{"_id":0,"Channel_Information":1}):
        channel_names.append(ch_data["Channel_Information"]['Channel_Name'])
    selected_channel=streamlit.selectbox("",["Select the channel"]+channel_names)
    #mysql button process
    if streamlit.button("SQL"):
        
        try:
            # selecting the row from the streamlit using selected_channel function
            Table=tables(selected_channel)
            streamlit.success(Table)
                
        except IndexError:
            streamlit.success(f"Select the channel which are listed in the dropdown")
#title in streamlit
streamlit.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
       
streamlit.header(":red[Channels]")
channel_streamlit()

    
#sql connection in streamlit

mydb = mysql.connector.connect(
        
        host='localhost',
        user='root',
        password='guvi2023',
        auth_plugin='mysql_native_password',
        database="youtube_data"
      )
mycursor = mydb.cursor()

question=streamlit.selectbox("select the Question",("1.What are the names of all the videos and their corresponding channels?",                                                    
"2.Which channels have the most number of videos, and how many videos do they have?",
"3.What are the top 10 most viewed videos and their respective channels?",
"4.How many comments were made on each video, and what are their corresponding video names?",
"5.Which videos have the highest number of likes, and what are their corresponding channel names?",
"6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
"7.What is the total number of views for each channel, and what are their corresponding channel names?",
"8.What are the names of all the channels that have published videos in the year 2022?",
"9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
"10.Which videos have the highest number of comments, and what are their corresponding channel names?"),
   index=None,
   placeholder="Select Question...",)



if question== "1.What are the names of all the videos and their corresponding channels?":
   
    q1='''select Title,Channel_Name from videos'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Video-Title","Channel_Name"])
    streamlit.write(df)
    mydb.commit()
    
elif question== "2.Which channels have the most number of videos, and how many videos do they have?":
  
    q1='''select Channel_Name,Videos from channels ORDER BY Videos DESC'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Channel_Name","Videos"])
    streamlit.write(df)
    mydb.commit()

elif question== "3.What are the top 10 most viewed videos and their respective channels?":
   
    q1='''select Title,Views from videos ORDER BY Views DESC LIMIT 10'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Video-Title","Views"])
    streamlit.write(df)
    mydb.commit()
    
elif question== "4.How many comments were made on each video, and what are their corresponding video names?":
   
    q1='''select Title,Commentcount from videos ORDER BY Views DESC'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Video-Title","Commentcount"])
    streamlit.write(df)
    mydb.commit()
    
elif question== "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
   
    q1='''select Channel_Name,Likes from videos ORDER BY Likes DESC LIMIT 1'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Channel_Name","Likes"])
    streamlit.write(df)
    mydb.commit()
    
elif question== "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    
    q1='''select Channel_Name,Title,Likes from videos'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Channel_Name","Video_Title","Likes"])
    streamlit.write(df)
    mydb.commit()

elif question== "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    
    q1='''select Channel_Name,Views from channels'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Channel_Name","Views"])
    streamlit.write(df)
    mydb.commit()
    
elif question== "8.What are the names of all the channels that have published videos in the year 2022?":
    
    q1='''select Channel_Name,Published from videos where YEAR(Published)=2022;'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Channel_Name","Published Date"])
    streamlit.write(df)
    mydb.commit()
    
elif question== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    
    q1='''select Channel_Name,AVG(Duration) from videos GROUP BY Channel_Name; '''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Channel_Name","Duration"])
    streamlit.write(df)
    mydb.commit()
    
elif question== "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    
    q1='''select Channel_Name,Commentcount from videos ORDER BY Commentcount DESC LIMIT 1'''
    mycursor.execute(q1)
    quesfetch1=mycursor.fetchall()
    df=pandas.DataFrame(quesfetch1,columns=["Channel_Name","Commentcount"])
    streamlit.write(df)
    mydb.commit()

import streamlit as st

def common_footer():
    footer_html = """
    <div style="position: fixed; bottom: 0; padding: 1px; width: 100%; background-color:#86777c; text-align: center;">
        <p>Skills used MySQL, MongoDB, and Python</p>
    </div>
    """
    st.markdown(footer_html, unsafe_allow_html=True)
common_footer()

   


    
    


    
    



    
    
    
        
