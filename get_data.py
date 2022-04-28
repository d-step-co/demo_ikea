# Import all necessaryed libraries
import json
import psycopg2
import requests
import datetime as dt


# Credentials for database connections
connect = psycopg2.connect(
    dbname = 'demo_ikea',
    user = 'postgres',
    password = 'password',
    host = 'localhost',
    port = '5433'
)


# Gettinglast data timestamp for incremental updating data
cursor = connect.cursor()
try:
    cursor.execute("""SELECT GREATEST(MAX(newsfeed_date), EXTRACT(EPOCH FROM (current_date - 1)::timestamp))::bigint 
                      FROM demo_ikea.demo_ikea.newsfeed
                   """
                   )
finally:
    connect.commit()
    response = cursor.fetchall()
    start_time_unix = json.dumps(response[0][0])
    cursor.close()


# Setting of variables for configuration of API request
api_version = 5.131
token = '55a0d56d4abb7c4e22eb92621f4507fc703017285770e9cc699f0705db893f8e96ef5f147fd3dbebb49e4'
keywords = ['ikea', 'икея', 'икеа']
message_count = 200
start_time = start_time_unix


# Configuration of API request
url = f"https://api.vk.com/method/newsfeed.search?" \
      f"v={api_version}" \
      f"&access_token={token}" \
      f"&q={'|'.join(keywords)}" \
      f"&count={message_count}" \
      f"&start_time={start_time}"


# Getting data from API
r = requests.request("GET", url)
r = r.json()


# Inserting data into database
for i in range(0, len(r['response']['items'])):
    cursor = connect.cursor()
    try:
        cursor.execute(f"""INSERT INTO demo_ikea.demo_ikea.newsfeed (newsfeed_date, newsfeed_item, insert_timestamp) 
                           VALUES (
                                    {json.dumps(r['response']['items'][i]['date'])}
                                 , '{json.dumps(r['response']['items'][i]).replace("'", "")}'
                                 , '{dt.datetime.now()}'
                                    )
                        """
                       )
    finally:
        connect.commit()
        cursor.close()
connect.close()
