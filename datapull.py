# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 11:12:11 2021

@author: lakna
"""
# Required Packages 
import requests
import pandas as pd
import psycopg2 




HOST = 'https://api.census.gov/data'
year = '2019'
dataset = 'acs/acs5'
base_url = "/".join([HOST,year,dataset])




predicates = {}

# Variables to include
get_vars = ['NAME','B01001_001E','B01001_019E','B01001_020E','B01001_043E','B01001_044E']
predicates['get'] = ','.join(get_vars)

#B01001_001E - total_pop
#B01001_019E - male_62_64
#B01001_020E - male_65_66
#B01001_043E - female_62_64
#B01001_044E - female_65_66


# Function to insert predicates needed for the api string
def var_select(level_of_analysis,geography,api_key):
    predicates['for'] = level_of_analysis
    #geography 
    predicates["in"] = geography
    # API key 
    predicates['key'] = api_key

var_select('block%20group:*','state:29%20county:*','0bbb3b25a5222043c09e77752ac1b37dd0f568e4')


url = base_url+"?get="+predicates["get"]+'&for='+predicates["for"]+"&in="+predicates["in"]+"&key="+predicates["key"]               

r = requests.get(url)

#-------------------------Loading the Data into a Pandas Dataframe----------------------------------
colnames = ['name','total_pop',' male_62_64','male_65_65','female_62_64','female_65_66','state','county','tract','block_group']
df = pd.DataFrame(columns=colnames,data=r.json()[1:])

df['county_tract_blockgroup']=df['county'].astype(str)+'-'+df['tract']+'-'+df['block_group']
df=df.drop(['county', 'tract','block_group','state'], axis=1) # Removing columns that were cancatnated

df=df[['county_tract_blockgroup','name','total_pop',' male_62_64','male_65_65','female_62_64','female_65_66']]

rows = df.values.tolist() # Creating the dataframe values to a list of lists





#------------------------Creating a Connection the Postgres Database-----------------------------------------------
DB_HOST = 'acs-db.mlpolicylab.dssg.io'
DB_NAME = 'acs_data_loading'
DB_USER = 'mlpp_student'
DB_PASS = 'CARE-horse-most'
DB_PORT = 5432

# connecting to the database
try:
    conn = psycopg2.connect(dbname=DB_NAME, 
                            user=DB_USER, 
                            password=DB_PASS, 
                            host=DB_HOST)
except Exception as e:
    print(e)

cur = conn.cursor()


#-------------------------------Create database table---------------------------------------------------------------
create_table = """CREATE TABLE Olderpopulation(
      UNIQUE_BLOCK VARCHAR PRIMARY KEY,
      BLOCK_DESCRIP VARCHAR,
      TOTAL_POP NUMERIC,
      M_62_64 NUMERIC,
      M_65_66 NUMERIC,
      F_62_64 NUMERIC,
      F_65_66 NUMERIC
);

"""

cur.execute(create_table)


#----------------------------Insert Query-----------------------------------------------------------------
postgres_insert_query = """ INSERT INTO Olderpopulation (
    UNIQUE_BLOCK,
    BLOCK_DESCRIP,
    TOTAL_POP,
    M_62_64,
    M_65_66,
    F_62_64,
    F_65_66) VALUES (%s,%s,%s,%s,%s,%s,%s)     
"""

# Obtaining the rows to insert from the pandas dataframe
record_to_insert=rows

cur.executemany(postgres_insert_query, record_to_insert)

count = cur.rowcount

print(count) # looking at how many rows were 

conn.commit() # Commiting these changes


# ------------------Testing some queries----------------------------------
query="SELECT * FROM Olderpopulation WHERE M_62_64 >10;" # obtaining all the records where the male pop between 62 and 64 is > 10
cur.execute(query)
rows = cur.fetchall() # possibly many rows

for row in rows:
    print(row)


cur.close() # closing the cursor
conn.close() # closing the database connection



#-----------

