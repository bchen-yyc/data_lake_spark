# Data Lake With Spark Project
### Background
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. They decide to implement a data lake solution with Spark.

### Schema
The data resides in S3. The ETL pipeline extracts the data from S3, transform them to a star schema and load them back into S3. A on-premise Spark cluster is ultilized to finish the whole ETL process. It can also be down with AWS EMR or Databricks Spark cluster. The star schema is optimized for queries on song play analysis. This includes the following tables.

**Fact Table**

1. songplays - records in log data associated with song plays i.e. records with page NextSong
   * num_songs, artist_id, artist_latitude, artist_longitude, artist_location, 
   * artist_name, song_id, title, duration, year

**Dimension Tables**

2. users - users in the app
   * ser_id, first_name, last_name, gender, level
3. songs - songs in music database
   * song_id, title, artist_id, year, duration
4. artists - artists in music database
   * artist_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
   * start_time, hour, day, week, month, year, weekday
### Databset
##### Song Dataset
The song dataset is place in S3 bucket `s3a://datalbc/input/song_data`. It is a subset of real data from the [Million Song Dataset]. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
##### Log Dataset
The log dataset is place in S3 bucket `s3a://datalbc/input/log_data`.This dataset consists of log files in JSON format generated by this [event simulator] based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.
![log data](https://video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)


### Files in repository and how to run them
- ***dl.cfg*** file contains your AWS credentials, fill in your own credentials.
- ***etl.py*** script retrieves the song and log data in the s3 bucket, transforms the data into fact and dimensional tables then loads the table data back into s3 as parquet files. Use`spark-submit etl.py` with other configurations.
- ***etl.ipynb*** file demonstrate the whole etl process in a clearer way. The job is done by the etl.py script eventually.
- ***create_emr.ipynb*** in case you want to provision a EMR cluster

[Million Song Dataset]:https://labrosa.ee.columbia.edu/millionsong/
[event simulator]:https://github.com/Interana/eventsim
