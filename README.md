# Data Collection and ETL

## Constructing the API string
I used the documentation found on the [American Community Survey 5-Year Data (2009-2019)](https://api.census.gov/data/2019/acs/acs5/examples.html) to construct my API string.
I seperated the items required for constructing the API string into different variables so they can be changed in the future to get census data from different census API's for different years.

To ensure that the script is modular I also created a function called **var_select** where the geography and level of analysis attributes can be entered as arguments


## The Data
I decided to look at block group level data in the State of Missouri (since I lived there for about 7 years), and focused on population figures related to the elderly population. The elderly population are active participants in local races, and I thought this information would be valuable to politial candidates to decide which blocks to target in door-to-door campaigning.

## Transforming Data
I used a Pandas dataframe to structure the data, and created a column concatenating the state, tract and block group level numbers to be used as a primary key column in the database table

## Loading the data
I used **psycopg2** a PostgreSQL database adapter to establish the database connection, and created a cursor instance using `cur = conn.cursor()`

I created a CREATE query to create the nessesary table, and an INSERT query to enter the data in to the database table. I then entered each of the rows from the pandas dataframe using `cur.executemany` method.

## Files in the Repo
+ datapull.py - The python script 
+ Documentation.txt-A list of resources that were helpful for working on this project

