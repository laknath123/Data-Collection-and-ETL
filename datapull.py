# -*- coding: utf-8 -*-
"""
Created on Thu Sep  2 11:12:11 2021

@author: lakna
"""
import requests
import pandas as pd
import psycopg2 

HOST = 'https://api.census.gov/data'
year = '2019'
dataset = 'acs/acs5'
base_url = "/".join([HOST,year,dataset])

predicates = {}

# Variables to include
get_vars = ['NAME','B01001_001E','B01001_019E','B01001_005E','B01001_045EA','B01001_034E','B01001_035E']
predicates['get'] = ','.join(get_vars)

#B01001_001E - total_pop
#B01001_019E - male_60_64
#B01001_005E - male_10_14
#B01001_045EA - female_67_69
#B01001_034E - female_22_24
#B01001_035E - female_25_29


# Level of Analysis
predicates['for'] = 'block%20group:*'
#geography 
predicates["in"] = 'state:29%20county:*' #29 is the missouri code- change this to the state code you want
# API key 
predicates['key'] = '0bbb3b25a5222043c09e77752ac1b37dd0f568e4'

url = base_url+"?get="+predicates["get"]+'&for='+predicates["for"]+"&in="+predicates["in"]+"&key="+predicates["key"]               

r = requests.get(url)

#-------------------------Loading the Data into a Pandas Dataframe----------------------------------
colnames = ['name','total_pop',' male_60_64','male_10_14','female_67_69','female_22_24','female_25_29','state','county','tract','block_group']
df = pd.DataFrame(columns=colnames,data=r.json()[1:])


#------------------------Loading data into the database-----------------------------------------------


DB_HOST = 'acs-db.mlpolicylab.dssg.io'
DB_NAME = 'acs_data_loading'
DB_USER = 'mlpp_student'
DB_PASS = 'CARE-horse-most'
DB_PORT = 5432

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

conn.close()
