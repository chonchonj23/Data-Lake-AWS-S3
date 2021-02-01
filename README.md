#### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which are in JSON format residing inside an AWS S3 bucket; data contain logs on user activity on the app (e.g. Log data), as well as a directory with JSON metadata of the songs in their app (e.g. Song data).

In this project, we would perform the role of a data engineer, and create the required data tables in Parquet format, and store them in another chosen S3 bucket.



#### Database Design

The database has a star scheme with one fact table and four dimension tables.

* Fact table: 
1. songplays - records in log data associated with song plays i.e. records with page NextSong
   songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


* Dimension Tables
1. users - users in the app
   columns in users: user_id, first_name, last_name, gender, level

2. songs - songs in music database
   columns in songs song_id, title, artist_id, year, duration

3. artists - artists in music database
   columns in artists: artist_id, name, location, latitude, longitude

4. time - timestamps of records in songplays broken down into specific units
   columns in time: start_time, hour, day, week, month, year, weekday


Due to relately simple data structure of song data (i.e. no or limited many-to-many relationships within data), star schema (instead of snowflake schema) has been chosen for this project.



#### ETL Pipeline Methodology

The challenges of this project are to:

- download two sources of JSON formatted data from S3 into Spark
- transofrm the data and create the 5 output tables described above in Spark
- store the 5 output tables in another chosen S3 bucket using Parquet format
 

To do that, we have to perform several steps:

1. Sign up an AWS account
2. Create an AWS user for this project (as it is not a good idea to use root user directly)
3. Create a publicly accessible S3 bucket
4. Assign a policy for allowing public access to the S3 bucket 
5. Download the log and song data from Sparkify's S3 bucket into Spark
6. Transform the log and song data within Spark, and create the 5 output tables described above (1 fact and 4 dimension tables)
7. Export the 5 output tables into the S3 bucket we created, and store the 5 tables in Parquet format



#### Python programs structure

etl.py is the only code for extracting, transforming and loading data for the project; there are several steps which are expressed as several user-defined functions in Python, which include: 

- process_song_data(): the function downloads Song data into Spark, and transform data into two output tables (e.g. songs and artists), and then store them into a chosen S3 bucket using Parquet format

- process_log_data(): the function downloads both Log and Song data into Spark, and transform data into three output tables (e.g. songplays, users, and time), and then store them into a chosen S3 bucket using Parquet format.

- main(): the two functions process_song_data() and process_log_data() are wrapped within the main() function; to execute the whole program, we only need to call main() in Python.



#### Software Requirements

Below softwares have been used in this project:

* Python version 3.6.3 
* Spark verion 2.4.3 
