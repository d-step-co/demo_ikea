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
    cursor.execute("""SELECT 
                            CASE WHEN MAX(newsfeed_date) IS NULL 
                                 THEN EXTRACT(EPOCH FROM (current_date - 1)::timestamp) 
                                 ELSE MAX(newsfeed_date) 
                            END::bigint 
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
token = '27d76222769d4a052e12982bdba1f09b33cffe7ef6171648931f1c843857c07617de8b0ba69bb3bc3c97e'
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
