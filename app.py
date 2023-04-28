from flask import Flask, request
import os 
import psycopg2 
from dotenv import load_dotenv
from datetime import datetime, timezone 

# SQL Queries 

# Different rooms of the house (rooms table)
CREATE_ROOMS_TABLE = (
    "CREATE TABLE IF NOT EXISTS rooms (id SERIAL PRIMARY KEY, name TEXT);"
)
# Insert rooms  
INSERT_ROOM_RETURN_ID = "INSERT INTO rooms (name) VALUES (%s) RETURNING id;"

# For the temperatures (temp. table)
CREATE_TEMPS_TABLE = (
    """CREATE TABLE IF NOT EXISTS temperatures (room_id INTEGER, temperature REAL, date TIMESTAMP, FOREIGN KEY(room_id) REFERENCES rooms(id) ON DELETE CASCADE);"""
)
# Insert temperatures 
INSERT_TEMP = (
    "INSERT INTO temperatures (room_id, temperature, date) VALUES (%s, %s, %s);"
)

# No. of days of data 
GLOBAL_NUMBER_OF_DAYS = (
    """SELECT COUNT(DISTINCT DATE(date)) AS days FROM temperatures;"""
)
# Avg. temperature 
GLOBAL_AVG = """SELECT AVG(temperature) as average FROM temperatures;"""



# before reading env 
load_dotenv()

app = Flask(__name__)

# connecting db using connection string from env 
url=os.getenv("DATABASE_URL")
connection = psycopg2.connect(url)


@app.get("/")
def home(): 
    return "Hello, world" 


@app.post("/api/room")
def create_room():
    data = request.get_json() # request will turn this request data to a dictionary 
    name = data["name"]
    with connection: # connection to DB
        with connection.cursor() as cursor: # cursor allows to insert data to the database or iterate rows that the DB returns (if we make query to select data)
            cursor.execute(CREATE_ROOMS_TABLE) # executing the query to create rooms table 
            cursor.execute(INSERT_ROOM_RETURN_ID, (name, )) 
            # since there is an id field we need to pass it a tuple so that it executes the query based on that passed id (%s - dynamic data field)
            # to pass the values we basically need to pass 'em through some tuple. Here since we need only one value, we pass like - (name, )
            room_id = cursor.fetchone()[0]; 
            # fetches one row from the cursor now based on the schema str you can access values (see query to see the structure of schema)
    return {"id": room_id, "message": f"Room {name} created."}, 201 # returning the response for first endpoint 

# we will handle the temperature with three diff data points (room name, rooom id, and temp) optionally we can also require the date (by defaulr it is now)
@app.post("/api/temperature")
def add_temp():
    data = request.get_json()
    room_id = data["room"]
    temperature = data["temperature"]
    
    try:
        date = datetime.strptime(data["date"], "%m-%d-%Y %H:%M:%S")
    except KeyError:
        date = datetime.now(timezone.utc)    
        
    # creating the connection 
    with connection:
        with connection.cursor() as cursor: 
            cursor.execute(CREATE_TEMPS_TABLE)
            cursor.execute(INSERT_TEMP, (room_id, temperature, date))
            
    return {"message" : "Temperature added."}, 201 

@app.get("/api/average")
def get_global_avg():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(GLOBAL_AVG)
            average = cursor.fetchone()[0]
            print("average room ", average)
            cursor.execute(GLOBAL_NUMBER_OF_DAYS)
            days = cursor.fetchone()[0]
            print("no. of days ", cursor.fetchall())
    return {"average": round(average, 2), "days": days}
    