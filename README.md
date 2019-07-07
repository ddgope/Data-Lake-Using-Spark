# Data Engineering Nanodegree
## Project: Data Lake using Spark and AWS
## Table of Contents
* **Definition**
    * **Project Overview** :
    Sparkify is a music app, they wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
    Currently, they don't have an easy way to query their data, which resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app..
    
    * **Problem Statement** : 
       Sparkify Analytics team is particularly interested in understanding what songs users are listening to.
   
* **DataLake Design**
    **DataLake Design** : 
    There are many ways I can design the DataLake. For this project I ahve followed deisgn apttern.
    ![Sparkify DataLake Model](/images/AWSDataLake.png)
    
* **DataLake Flow**   
    * **Data Ingestion** : 
        Before running ETL pipeline, I need to make sure that the songs and log files both are valid files. Following data quality I have validated
        1. Data validation.
        1. Constraint validation: ensuring the constraints for tables are properly defined.
        1. Data completeness: checking all expected data has been loaded. For e.g. verify any required column is having either Null or Empty value.
        1. Data correctness: making sure the data has been accurately recorded. For e.g. verify if any duplicates record exists or not.
        1. Validating dates e.g. ts timestamp column should be integer and should be able to convert into datetime.
        1. Data cleanliness: removing unnecessary columns.
        1. Either song or log file content is empty, then reject the files and send message that invalid file.
        1. Missing Observations
        
    * **Data Process** :
    Once you run the pipeline, it will collect, process, and store into tables. Once it is processed, you can get timely insights and react quickly to new information. Below is two step process
        1. Read From S3: Reading the files using python
        1. Processing using Spark : Procesisng using pySpark

    * **Data Persistance** :
    Once data is processed, I have saved backed into AWS S3 stroage
        1. Write into S3: Once data is process, it is saved into S3
           
* **How to Run** : Open the terminal, type as below
    1. dl.cfg
        1. Open the dl.cfg and provide the AWS access keys and secret        
    1. python etl.py
    1. analysis.ipynb - run you all analysis
    
* **Final Result / Analysis** : Now Sparkify Analytics team can run multiple queries using data_analysis.ipynb notebook or Users can connect any tool like Amazon QuickSight, Power BI,tableau to S3. They can do what if analysis or they can slice/dice the data as per their reqirement. 
    1. Currently how many users are listening songs ?
    1. How the users are distributes across the geography ?
    1. Which are the songs they are playing ?
    
* **Software Requirements** : This project uses the following software and Python libraries:
        1. Python 3.0
        1. Spark 2.4.0
        1. Amazon S3
        
    You will also need to have software installed to run and execute a Jupyter Notebook.
    If you do not have Python installed yet, it is highly recommended that you install the Anaconda distribution of Python, which already has the above packages and more included.    

* **Acknowledgement** : Must give credit to Udacity for the project. You can't use this for you Udacity capstone project. Otherwise, feel free to use the code here as you would like!

* **Bonus** : While designing this project, I have followed below Data Lake deisgn best practices. 