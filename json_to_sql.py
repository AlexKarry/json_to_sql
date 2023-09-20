import sqlite3
import json

db_filename = '/Users/alexkarry/Desktop/FOLDERS/Programming/Python/Advanced_Python_NYU/python_data_apy/session_02_working_files/session_2.db'

conn = sqlite3.connect(db_filename)
cursor = conn.cursor()

drop_table_query = 'DROP TABLE IF EXISTS weather_newyork'
cursor.execute(drop_table_query)

create_table_query = "CREATE TABLE weather_newyork (date TEXT, mean_temp INT, precip FLOAT, events TEXT)"
cursor.execute(create_table_query)

insert_query = 'INSERT INTO weather_newyork (date, mean_temp, precip, events) VALUES (?, ?, ?, ?)'

fh = open('/Users/alexkarry/Desktop/FOLDERS/Programming/Python/Advanced_Python_NYU/python_data_apy/session_02_working_files/weather_newyork_dod.json')
load_file = json.load(fh)

for date in load_file:
    inner_dict = load_file[date]
    mean_temp = int(inner_dict['mean_temp'])
    precip  = inner_dict['precip']
    events = inner_dict['events']
    if precip == 'T':
        precip = None
    else:
        precip = float(inner_dict['precip'])

    data_string = f"Date: {date}, Mean Temp: {mean_temp}, Precip: {precip}, Events: {events}"
    print(data_string)

    cursor.execute(insert_query, (date, mean_temp, precip, events))

cursor.execute('SELECT COUNT(*) FROM weather_newyork')
row_count = cursor.fetchone()[0]
print("Number of row in weather_newyork table:", row_count)

conn.commit()
conn.close()
