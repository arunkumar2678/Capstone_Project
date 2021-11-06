# Project Title
### Data Engineering Capstone Project

#### Project Summary
--describe your project at a high level--

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


https://collegefootballdata.com/ 

### Scope of the Project
The objective of this project is to collect College Football data for the past decade for analysis for my data engineering project. Once the data is gathered, I will be using the Microsoft excel tool to clean up the data and copy them over as individual files in AWS S3 buckets. I will develop an ETL process using Airflow and Python to perform staging of source data, loading of Facts and Dimension tables, and perform Data Qulaity checks. I will run queries off the snowflake schema to do analysis. I will be focussing on the LSU Team on how they built a championhsip team in 2019 by comparing data over a decade.

#### Data Description
Describe the data sets you're using. Where did it come from? What type of information is included? 
I am using the data exported from ESPN and other online open sources available online. The data contains granular infromation starting from the conference details till the play by play details of each game and their results.

###### Data Sources 
- https://collegefootballdata.com/exporter
- https://drive.google.com/drive/folders/0B13YvT1olZIfZ0NKaEZDdGt3TDQ?resourcekey=0-sh9lds-ck95y3yeBpClk7g

#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data

- Fixed the date time stamp column in the files to have a timestamp in the format YYYY-MM-DD HH:MM:SS
- Cleaned up special characters that PostgresSQL does not support during insertion of data into the tables. For ex: San José State has a diacritical mark on the letter e in the word "Jose". PostgresSQL fails during insert of data into the tables. So I have replaced words like "José" to 'Jose' by removing the diacritical mark.

<img src="Staging Data Model.png"/>
