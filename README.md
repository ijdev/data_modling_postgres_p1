## INTRO
A startup called Sparkify wants to analyze the data they've been collecting on songs
and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to. 
Currently, they don't have an easy way to query their data, 
which resides in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.

## THE GOAL
###  Star schema
To help solve the problem I created the necessary tables in the conceptual shape of a star schema.
The start schema is a great and yet simple to design to organize the database and mak it 
really easy to do OLAP operations. resulting in simple quires and fast aggregations.
### ETL Pipeline
1. Extracting: Data is in the data folder in a json format.
2. Transforming: After loading the data using pandas dataframe we now do the necessary wrangling and transforming
3. Loading: load the data to the database.

## RESULT
#### Example query:
 #### 1. Aggregate play time and group by city.
       
        SELECT location, SUM(start_time) 
        FROM songplays 
        GROUP BY location;
        
#### 2. All free-member users who listing to music on a weekday from 8-9 AM.   

        SELECT users.user_id, users.first_name, users.last_name, users.gender, time.weekday, time.hour
        FROM songplays JOIN users ON users.user_id = songplays.user_id
        JOIN time ON time.start_time = songplays.start_time
        WHERE time.weekday <> 'Sunday' AND 
        time.weekday <> 'Saturday' AND 
        users.level = 'free' AND 
        (time.hour = 8 OR time.hour = 9)
        
        

## HOW TO RUN 
1. run create_tables.py to create the necessary tables and insert statements.
2. run etl.py to process all data at once. to process one file go to etl.ipynb
3. run test.ipynb to see if all insertion are working properly 

##### NOTE: You will not be able to run test.ipynb, etl.ipynb, or etl.py until you have run create_tables.py at least once to create the sparkifydb database, which these other files connect to.