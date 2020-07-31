import json
import psycopg2
import random
import datetime
import random


import pandas as pd
from psycopg2 import sql

#first_names:
# # https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-data-by-state-and-district-of-#topic=developers_navigation


STATE_CODES= ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
STATE_NAMES= ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
STATE_DICT= dict(zip(STATE_CODES,STATE_NAMES))
STATE_COUNT= 1000 # 51k total
MONTH_NUMS = [1,2,3,4,5,6,7,8,9,10,11,12]
DAYS_MONTH = [31,28,31,30,31,30,31,31,30,31,30,31]
# last_names:
# https://www.census.gov/topics/population/genealogy/data/2010_surnames.html
LAST_NAMES_FILE='data/Names_2010Census.csv'
DAYS_PER_MONTH= dict(zip(MONTH_NUMS,DAYS_MONTH))

def connect_to_db():
    # todo: once connected change to config file instead of str : https://www.postgresqltutorial.com/postgresql-python/connect/
    try:
        conn = psycopg2.connect(host="localhost", database="*", user="*", 
                                password="*", port=5432 )
        #print(conn)
        return conn
    except psycopg2.OperationalError as ex:
        print("Connection failed: {0}".format(ex))


def build_query_insert_person(person):
    q = sql.SQL("INSERT INTO ppl(name,lname,dob,state) VALUES({name},{lname},{dob},{state});").format(
        name=sql.Literal(person['name'].upper()),
        lname=sql.Literal(person['lname'].upper()),
        dob=sql.Literal(person['dob']),
        state=sql.Literal(person['state'].upper()))
    return q


def do_query(conn,query_type,p):
    cursor = conn.cursor()
    if query_type == 'insert_person':
        query = build_query_insert_person(p)
    if query:
        #print(f'Executing  \n{query}')
        cursor.execute('BEGIN')
        cursor.execute(query)
        cursor.execute('COMMIT')
    cursor.close()


#def make_all_data():
del STATE_DICT['PR']
con = connect_to_db()
df=pd.read_csv(LAST_NAMES_FILE)
all_names=[]
lnames=list(df['name'].values)  #162,254 values

if con and len(lnames)>0:
    for sc in list(STATE_DICT.keys()):
        fname= f'data/namesbystate/{sc}.txt'
        print(fname)
        with open(fname, 'r') as f:
            lines= f.readlines()
            random.shuffle(lines)
            print(f'example line: {lines[0]}')
            for line in lines[:STATE_COUNT]:
                year = line.split(',')[2]
                month = random.choice(MONTH_NUMS)
                day= random.choice(range(1,DAYS_PER_MONTH[month]))
                person= {
                    'name': line.split(',')[3],
                    'lname': random.choice(lnames),
                    'dob': f'{year}-{month}-{day}',
                    'state':sc
                    }
                do_query(con,'insert_person',person)
    con.close()


