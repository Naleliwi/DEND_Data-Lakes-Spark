# Cloud data warehouse for Sparkify Data

## About 

Sparkify is a fictional music application that store songs and users' activity logs in separate **JSON** files. When the application started to grow, it becomes extremely difficult for the company to handle and benefit from these files. The suggested solution is to start investing in cloud solution. In this project Amazon Web services will be used. 

## ETL pipeline Logic
Load credentials
- **Loading credintials**
- **Read data from s3 bucket**
- **Transform data by careting five seprate tables**
- **Load data to a new s3 bucket**

### NOTE:

Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.


## User Manual:

To Run the codes do the following instructions in the same exact order

 1. Open the terminal or bash in windows
 2. Write *python etl.py* then wait until the processing is completed 
