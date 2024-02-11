import csv
import mysql.connector
from mysql.connector import Error
import random
import math

def generate_random_coordinates(lat, lon, distance_km):
    earth_radius_km = 9371
    max_angle = distance_km / earth_radius_km

    angle = random.uniform(0, 2 * math.pi)
    dist = random.uniform(0, max_angle)

    new_lat = lat + dist * math.cos(angle)
    new_lon = lon + dist * math.sin(angle)
    return new_lat, new_lon

host = "localhost"
port = "3306"
database = "metro"
user = "root"
password = "123456"

try:
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password
    )
    
    if connection.is_connected():
        db_info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_info)
        cursor = connection.cursor()

        cursor.execute(f"DROP DATABASE IF EXISTS {database}")
        print(f"Database {database} dropped if it existed.")

        cursor.execute(f"CREATE DATABASE {database}")
        print(f"Database {database} created.")

        cursor.execute(f"USE {database}")

        create_stations_table_query = """
        CREATE TABLE IF NOT EXISTS metro_stations (
            station_id VARCHAR(10),
            station_name VARCHAR(255),
            latitude DOUBLE,
            longitude DOUBLE,
            PRIMARY KEY (station_id)
        );
        """
        cursor.execute(create_stations_table_query)

        create_passages_table_query = """
        CREATE TABLE IF NOT EXISTS train_passages (
            line_id VARCHAR(20),  
            station_id VARCHAR(10),
            direction VARCHAR(255),
            passage_time TIME,
            passage_timeid VARCHAR(50),  
            FOREIGN KEY (station_id) REFERENCES metro_stations(station_id),
            PRIMARY KEY (passage_timeid)  
        );
        """
        cursor.execute(create_passages_table_query)
        print("Tables are created.")

        with open('metro_station_data.csv', 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  
            for row in csv_reader:
                station_id = row[1]
                station_name = row[2]
                direction = row[3]
                timetable = row[4].split(', ')

                latitude, longitude = generate_random_coordinates(48.8760918, 2.3230224, 10)
                insert_station_query = """
                INSERT INTO metro_stations (station_id, station_name, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE station_name=VALUES(station_name), latitude=VALUES(latitude), longitude=VALUES(longitude);
                """
                cursor.execute(insert_station_query, (station_id, station_name, latitude, longitude))

                for time_entry in timetable:
                    parts = time_entry.split('-')
                    line_id = parts[0]
                    passage_time = parts[2]
                    passage_timeid = f"{line_id}_{station_id}_{passage_time.replace(':', '')}"

                    insert_passage_query = """
                    INSERT INTO train_passages (line_id, station_id, direction, passage_time, passage_timeid)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_passage_query, (line_id, station_id, direction, passage_time, passage_timeid))

        connection.commit()
        print("Data inserted successfully.")

except Error as e:
    print("Error while connecting to MySQL", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
