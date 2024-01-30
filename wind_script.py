import json
import time
import datetime
import pandas as pd
import math

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

def fetch_weather_data():
    MAX_ATTEMPTS = 6
    SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

    def download_data(uri):
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            try:
                data = urlopen(uri, timeout=300).read().decode("utf-8")
                if data is not None and not data.startswith("ERROR"):
                    return data
            except Exception as exp:
                print("download_data(%s) failed with %s" % (uri, exp))
                time.sleep(5)
            attempt += 1

        print("Exhausted attempts to download, returning empty data")
        return ""
#Here we are cleaning the data and only leaving what we need. 
    def clean_data(station_code):
        filename = f"{station_code}.txt"
        station_data = pd.read_fwf(filename, header=None)
        station_data = station_data.iloc[5:]
        if station_data.empty:
            station_data_clean = pd.DataFrame(columns=['date', 'sped', 'drct'])
        else:
            with open(f"{station_code}_station_data.txt", 'w') as f:
                dfAsString = station_data.to_string(header=False, index=False)
                f.write(dfAsString)

            station_data_clean = pd.read_csv(f"{station_code}_station_data.txt")

        station_data_clean.to_csv(f"{station_code}_station_data.csv", index=None)

    def calculate_mean_and_mode(station_code):
        filename = f"{station_code}_station_data.csv"
        data = pd.read_csv(filename)

        data['sped'] = pd.to_numeric(data['sped'], errors='coerce')
        data['drct'] = pd.to_numeric(data['drct'], errors='coerce')

        mean_wind_speed = data['sped'].mean()
        
#equations used to fin wind direction, where taken from an article titled 
# #"Technical note: Averaging wind speeds and directions" by Stuart K Grange 
# #link https://www.researchgate.net/publication/262766424_Technical_note_Averaging_wind_speeds_and_directions
        u_values = -mean_wind_speed * data['sped'].apply(lambda x: math.sin(2 * math.pi * x / 360))
        v_values = -mean_wind_speed * data['sped'].apply(lambda x: math.cos(2 * math.pi * x / 360))

        wind_direction = (180 / math.pi) * pd.Series([math.atan2(u, v) for u, v in zip(u_values, v_values)])
        wind_direction = wind_direction.apply(lambda x: x + 360 if x < 0 else x)

        mode_wind_direction = wind_direction.mode().max()

        date = data.iloc[0, 1]
        lon = data.iloc[0, 2]
        lat = data.iloc[0, 3]

        return date, lon, lat, mean_wind_speed, mode_wind_direction
     #setting the time range that we want our data to be collected. So its 
    #always going to be 2 days before today N-2

    endts = datetime.datetime.utcnow()
    startts = endts - datetime.timedelta(days=2)
#This is where we specify what we are pulling from the api, in this case its,
#drct(direction), sped, lon, lat, date and time
    service = SERVICE + "data=drct&data=sped&tz=Etc/UTC&format=comma&latlon=yes&"

    service += startts.strftime("year1=%Y&month1=%m&day1=%d&")
    service += endts.strftime("year2=%Y&month2=%m&day2=%d&")

    stations = ["GNT", "GUP", "FMN", "RQE", "INW", "PGA", "GCN", "CMR", "BDG", "4SL", "AEG", "CEZ", "E80", "ONM"]
    
    for station in stations:
        uri = "%s&station=%s" % (service, station)
        print("Downloading data for station: %s" % (station,))
        data = download_data(uri)
        outfn = "%s.txt" % (station,)
        out = open(outfn, "w")
        out.write(data)
        out.close()

        clean_data(station)
# The stations are put into a list and then we used a for loop to loop over each station, and organizing the data

    stations = ["GNT", "GUP", "FMN", "RQE", "INW", "PGA", "GCN", "CMR", "BDG", "AEG", "CEZ", "ONM"]

    results = {}
    for station_code in stations:
        date, lon, lat, mean_wind_speed, mode_wind_direction = calculate_mean_and_mode(station_code)
        results[station_code] = {
            'date': date,
            'lon': lon,
            'lat': lat,
            'mean_wind_speed': mean_wind_speed,
            'mode_wind_direction': mode_wind_direction,
        }
#The data is being transposed from a dictionary data structure and being written in a new 
#csv file called combined_data.csv
    combined_data = pd.DataFrame.from_dict(results).transpose()
    combined_data.to_csv("combined_data.csv", index=True)

if __name__ == "__main__":
    fetch_weather_data()
            