import requests
import json
import csv
import psycopg2
import pandas as pd

cities = ['London','Manchester']
cities_countries =[{"city":"London","country":"uk"},{"city":"Manchester","country":"uk"},{"city":"Boston","country":"usa"}]
#cities_countries = [{"city":"London","country":"uk"}]

# To get temp_condition,temp by passing city, country
def weather_data(cities_countries):
    response_data = []

    for city_country in cities_countries:
        city_country_dict = dict(city_country)
        city = city_country_dict['city']
        country = city_country_dict['country']
        r = requests.get('http://api.openweathermap.org/data/2.5/weather?q='+city+','+country+'&APPID='')
        response = json.loads(r.content)
        weather = response['weather']
        temp = response['main']
        dt = response['dt']
        dict_weather = weather[0]

        from datetime import datetime
        ts = int(dt)


        weather_date = str(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d'))
        weather_time = str(datetime.utcfromtimestamp(ts).strftime('%H:%M:%S'))

        connection = psycopg2.connect(user="postgres",
                                      password="root",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="weatherinfo")
        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print (connection.get_dsn_parameters(), "\n")

        sql = """INSERT INTO weatherdata(city,country,weather_type,temperature,weather_date,weather_time)
                     VALUES(%s,%s,%s,%s,%s,%s);"""



        "INSERT INTO weatherdata(city,country,weather_type,temperature,weather_date,weather_time)VALUES("+city+","+





        cursor.execute(sql, (city, country, dict_weather['main'],temp['temp'] , weather_date, weather_time))
        connection.commit()
        cursor.close()
        connection.close()





        result_dict = {"city":city,"country":country,"weather_type":dict_weather['main'],"temp":temp['temp'],"weather_date":weather_date,"weather_time":weather_time}
        response_data.append(result_dict)
    return response_data

data = weather_data(cities_countries)

df = pd.DataFrame(data)

#print(pd.DataFrame.from_dict(data))
#print(pd.DataFrame.from_records(data))

# Resulting saving to csv
def export_to_csv(dataframe):
    with open("out.csv", 'a') as f:
        df.to_csv(f, index=False,header=True)
    return "Saved to out.csv"



#export_to_csv(df)


# Inserting into postgres db
# "city":city,"country":country,"weather_type":dict_weather['main'],"temp":temp['temp'],"weather_date":weather_date,"weather_time":
def insert_to_postgres(city,country,weather_type,temperature,weather_date,weather_time):
    connection = psycopg2.connect(user="postgres",
                          password="root",
                          host="127.0.0.1",
                          port="5432",
                          database="weatherinfo")
    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print (connection.get_dsn_parameters(), "\n")

    sql = """INSERT INTO weatherdata(city,country,weather_type,temperature,weather_date,weather_time)
             VALUES(%s,%s,%s,%d,%s,%s);"""

    cursor.execute(sql,(city,country,dict_weather['main'],temperature,weather_date,weather_time))
    connection.commit()
    cursor.close()
    connection.close()
    return "inserted data"

#insert_to_postgres()






    # insert to table




