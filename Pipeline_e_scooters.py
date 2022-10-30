#!/usr/bin/env python
# coding: utf-8

# In[89]:


import pandas as pd
import requests
from bs4 import BeautifulSoup
import regex as re
import ast


# In[90]:


WIKIPEDIA_CITY_URL = "https://en.wikipedia.org/wiki/List_of_metropolitan_areas_in_Europe"
API_KEY = "675eaf22f26901bccacf8e9cb50f3976"
AWS_HOST="e-scooter-db.ctvbrrdkjyzj.eu-central-1.rds.amazonaws.com"
LOCAL_HOST = "127.0.0.1"
AWS_DB_USER="admin"
LOCAL_DB_USER = "root"
DB_PASSWORD="Rajatccna1990"
DB_PORT=3306
X_RAPID_API_KEY = "13825192f1msh925ed007a59ab15p12cbabjsn9d5b658ff56b"
X_RAPID_API_HOST = "aerodatabox.p.rapidapi.com"


# # Web Scraping

# In[91]:


def get_city_urls():
    url = WIKIPEDIA_CITY_URL
    response = requests.get(url)
    response.status_code
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", class_="wikitable sortable")
    table_body = table.find("tbody")
    table_rows = table_body.find_all("tr")
    result = []
    for row in table_rows[1:]:
        cell = row.find("td")
        city = cell.get_text().strip()
        link = cell.find("a", href = True)
        href = link["href"]
        href = "https://en.wikipedia.org/"+href
        result.append({"city": city, "url": href})
    return pd.DataFrame(result)


def clean_city_names(city_name):
    if "metropolitan" in city_name or "(" in city_name or "Metropolitan" in city_name:
        city_name = city_name.split(" ")[0]
    elif "/" in city_name:
        city_name = city_name.split("/")[0]
    elif "Greater" in city_name:
        city_name = city_name.split(" ")[1]
    elif "[" in city_name:
        city_name = city_name.split("[")[0]
    elif "-" in city_name:
        city_name = city_name.split("-")[0]
    else:
        city_name = city_name
    return city_name.strip()


def get_population(rows):
    for index, row in enumerate(rows[1:]):
        row = row.find("th")
        if row and "Population" in row.get_text():
            return rows[index+2].get_text()
    
def get_coordinates(rows):
    for index, row in enumerate(rows[1:]):
        row = row.find("td")
        if row and "Coordinates" in row.get_text():
            return row.get_text()
        

def get_coordinates_population(df):
    population = []
    location = []
    city = []
    urls = []
    for index, row in df.iterrows():
        try:
            city_url = row["url"]
            city_name = row["city"]
            response = requests.get(city_url)
            response.status_code
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find("table", class_="infobox ib-settlement vcard")
            table_body = table.find("tbody")
            rows = table_body.find_all("tr")
            
            city.append(city_name)
            urls.append(city_url)
            population.append(get_population(rows))
            location.append(get_coordinates(rows)) 
            
        except:
            
            continue
    return pd.DataFrame({"city": city, "population": population , "coordinates": location, "url": urls})


def get_clean_population(item):
    if "(" in item:
        item = item.split("(")[0]
    elif "[" in item:
        item = item.split("[")[0]
    elif "Rank" in item or "rank" in item:
        item = None
    return item

    
def get_cleaner_population(item):
    if item:
        num_in_item = re.findall('[0-9]+', item)
        if num_in_item:
            return "".join(num_in_item)
        
def get_clean_coordinates(item):
    if item:
        item = item.split(":")[1].strip()
        item = item.split("/")[1]
        return item

    
def get_scraped_data():
    city_data = get_city_urls()
    city_data["city"]= city_data["city"].apply(clean_city_names)
    city_data = get_coordinates_population(df= city_data)
    city_data["population"]= city_data["population"].apply(get_clean_population).apply(get_cleaner_population)
    city_data["coordinates"]= city_data["coordinates"].apply(get_clean_coordinates)
    return city_data


# # API Data

# In[92]:


def get_weather(df):
    
    API_key = API_KEY
    temp_min = []
    temp_max = []
    humidity = []
    temp = []
    feels_like = []
    dt = []
    location = []
    city_names = []
    for city in df["city"]:
        try:
            #print(".....................................")
            url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_key}&units=metric"
            response = requests.get(url)
            resp = response.json()
            resp1 = resp["list"]
            location.append(city)
            for item in resp1:
                temp.append(item["main"]["temp"])
                temp_min.append(item["main"]["temp_min"])
                temp_max.append(item["main"]["temp_max"])
                humidity.append(item["main"]["humidity"])
                feels_like.append(item["main"]["feels_like"])
                dt.append(item["dt_txt"])
                city_names.append(city)
        except Exception as e:
            print("error", city)
            
    return pd.DataFrame({"city": city_names, "timestamp": dt, 
                                 "min_temp": temp_min, 
                                 "max_temp": temp_max, 
                                 "temp": temp, 
                                 "feels_like": feels_like, 
                                 "humidity": humidity})


# # Flights Rapid API data

# In[93]:


def get_city_airports(item):
    import requests
    if item:
        
        lat = item.strip().split("°")[0]
        lat = lat.replace(u'\ufeff', '')
        
        lon = item.strip().split("°")[1].split(" ")[1]
        lon = lon.replace(u'\ufeff', '')
        
        lat = float(lat.strip())
        lon = float(lon.strip())

        url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{lat}/{lon}/km/25/16"

        querystring = {"withFlightInfoOnly":"true"}

        headers = {
            "X-RapidAPI-Key": X_RAPID_API_KEY,
            "X-RapidAPI-Host": X_RAPID_API_HOST
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        
        return response.text

def get_airport_icao(item):
    icao = None
    if item:
        import ast
        item = ast.literal_eval(item)
        item = item["items"]
        if item:
            item = item[0]
            icao = item["icao"]
    return icao

def get_airport_iata(item):
    iata = None
    if item:
        import ast
        item = ast.literal_eval(item)
        item = item["items"]
        if item:
            item = item[0]
            iata = item["iata"]
    return iata
  

def get_flights(item):
    import requests
    icao = item

    url = f"https://aerodatabox.p.rapidapi.com/airports/icao/{icao}/stats/routes/daily"

    headers = {
        "X-RapidAPI-Key": X_RAPID_API_KEY,
        "X-RapidAPI-Host": X_RAPID_API_HOST
    }

    response = requests.request("GET", url, headers=headers)
    return response.text

    
def get_num_flights(item):
    num_of_flights = 0.0
    if item:
        item = ast.literal_eval(item)
        item = item["routes"]
        if item:
            for route in item:
                num_of_flights += route["averageDailyFlights"]
    return num_of_flights

def get_flights_data(df):
    df["airports"] = df["coordinates"].apply(get_city_airports)
    df["airport_icao"] = df["airports"].apply(get_airport_icao)
    df["airport_iata"] = df["airports"].apply(get_airport_iata)
    df["flights"] = df["airport_icao"].apply(get_flights)
    df["num_of_flights"] = df["flights"].apply(get_num_flights)
    df = df.drop(["airports", "flights"], axis = 1)
    return df


# # Connecting to DB

# In[61]:


def get_connection(run_locally):
    
    if run_locally:
        schema = "e_scooters"   
        host = LOCAL_HOST        
        user = LOCAL_DB_USER
        password = DB_PASSWORD
        port = DB_PORT
        con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    else:
        schema = "e_scooters"
        host = AWS_HOST
        user = AWS_DB_USER
        password = DB_PASSWORD
        port = DB_PORT
        con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return con
        


# # Saving Data

# In[57]:


def save_data(data, table_name, con):
    data.to_sql(table_name,if_exists='append',con=con,index=False)


# # Pipeline

# In[87]:


def lambda_handler(run_locally):
    print("Start")
    con = get_connection(run_locally)
    data1 = get_scraped_data()
    data2 = get_weather(data1)
    data3 = get_flights_data(data1.head(10))
    save_data(data1, "city_data", con)
    save_data(data2, "city_weather", con)
    save_data(data3, "flights", con)
    print("Done")
    


# In[ ]:




