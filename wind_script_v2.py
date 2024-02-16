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

    def clean_data(station_code, date):
        filename = f"{station_code}_{date}.txt"
        station_data = pd.read_fwf(filename, header=None)
        station_data = station_data.iloc[5:]
        if not station_data.empty:
            with open(f"{station_code}_{date}_station_data.txt", 'w') as f:
                dfAsString = station_data.to_string(header=False, index=False)
                f.write(dfAsString)
            station_data_clean = pd.read_csv(f"{station_code}_{date}_station_data.txt")
            station_data_clean.to_csv(f"{station_code}_{date}_station_data.csv", index=None)

    def calculate_mean_and_mode(station_code, date):
        filename = f"{station_code}_{date}_station_data.csv"
        data = pd.read_csv(filename)
        data.replace('M', pd.NA, inplace=True)

        if data.empty:
            return None, None, None, None, None, None, None, None, None, None, None

        data['sped'] = pd.to_numeric(data['sped'], errors='coerce')
        data['drct'] = pd.to_numeric(data['drct'], errors='coerce')
        data['tmpf'] = pd.to_numeric(data['tmpf'], errors='coerce')
        data['relh'] = pd.to_numeric(data['relh'], errors='coerce')
        data['mslp'] = pd.to_numeric(data['mslp'], errors='coerce')
        data['p01m'] = pd.to_numeric(data['p01m'], errors='coerce')
        data['vsby'] = pd.to_numeric(data['vsby'], errors='coerce')

        mean_wind_speed = data['sped'].mean()
        u_values = -mean_wind_speed * data['sped'].apply(lambda x: math.sin(2 * math.pi * x / 360))
        v_values = -mean_wind_speed * data['sped'].apply(lambda x: math.cos(2 * math.pi * x / 360))

        wind_direction = (180 / math.pi) * pd.Series([math.atan2(u, v) for u, v in zip(u_values, v_values)])
        wind_direction = wind_direction.apply(lambda x: x + 360 if x < 0 else x)

        mode_wind_direction = wind_direction.mode().max()

        # Calculate mode cloud cover
        mode_cloud_cover1 = data[['skyc1']].mode().max()
        mode_cloud_cover2 = data[['skyc2']].mode().max()
        mode_cloud_cover3 = data[['skyc3']].mode().max()

        # Calculate average values
        average_temp = data['tmpf'].mean(skipna=True)
        average_relh = data['relh'].mean(skipna=True)
        average_pressure = data['mslp'].mean(skipna=True)
        average_precipitation = data['p01m'].mean(skipna=True)
        average_visibility = data['vsby'].mean(skipna=True)

        return date, mean_wind_speed, mode_wind_direction, mode_cloud_cover1, mode_cloud_cover2, mode_cloud_cover3, average_temp, average_relh, average_pressure, average_precipitation, average_visibility

    # Define stations_dates dictionary and loop inside the fetch_weather_data function
    stations_dates = {
        "3A6": "2021-10-01",
        "5T6": "2023-08-01",
        "9A1": "2021-08-17",
        "ABQ": "2023-08-06",
        "AEG": "2023-06-21",
        "BDN": "2023-08-02",
        "CBM": "2023-05-11",
        "CPR": "2023-08-21",
        "CTJ": "2023-08-13",
        "EDC": "2023-08-16",
        "FBL": "2023-05-22",
        "FFZ": "2019-10-25",
        "GEU": "2023-08-12",
        "ILM": "2021-02-14",
        "LRU": "2023-07-22",
        "LUL": "2020-08-06",
        "MCJ": "2023-08-11",
        "MKC": "2023-05-19",
        "NPA": "2023-08-15",
        "NZJ": "2020-10-26",
        "OAK": "2023-08-09",
        "PGV": "2023-08-06",
        "PHX": "2021-06-07",
        "RBG": "2020-08-20",
        "RDU": "2022-11-26",
        "RID": "2023-04-11",
        "SPA": "2023-07-15",
        "TUS": "2021-04-30"
    }

    for station, date_str in stations_dates.items():
        fetch_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        service = SERVICE + "data=tmpf&data=relh&data=drct&data=sped&data=mslp&data=p01m&data=vsby&data=skyc1&data=skyc2&data=skyc3&data=skyl1&data=skyl2&data=skyl3&tz=Etc/UTC&format=comma&latlon=yes&"
        service += fetch_date.strftime("year1=%Y&month1=%m&day1=%d&")
        service += fetch_date.strftime("year2=%Y&month2=%m&day2=%d&")

        uri = f"{service}&station={station}"
        print(f"Downloading data for station: {station} on date: {date_str}")
        data = download_data(uri)
        outfn = f"{station}_{fetch_date.strftime('%Y%m%d')}.txt"
        out = open(outfn, "w")
        out.write(data)
        out.close()

        clean_data(station, fetch_date.strftime('%Y%m%d'))

    results = {}
    for station_code, date_str in stations_dates.items():
        date, mean_wind_speed, mode_wind_direction, mode_cloud_cover1, mode_cloud_cover2, mode_cloud_cover3, average_temp, average_relh, average_pressure, average_precipitation, average_visibility = calculate_mean_and_mode(station_code, datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime('%Y%m%d'))

        # Check if any of the returned values are None
        # if None in (date, mean_wind_speed, mode_wind_direction, mode_cloud_cover1, mode_cloud_cover2, mode_cloud_cover3, average_temp, average_relh, average_pressure, average_precipitation, average_visibility):
        #     print(f"Skipping {station_code}, incomplete data")
        #     continue

        results[station_code] = {
            'date': date,
            'mean_wind_speed': mean_wind_speed,
            'mode_wind_direction': mode_wind_direction,
            'mode_cloud_cover1': mode_cloud_cover1,
            'mode_cloud_cover2': mode_cloud_cover2,
            'mode_cloud_cover3': mode_cloud_cover3,
            'average_temp': average_temp,
            'average_relh': average_relh,
            'average_pressure': average_pressure,
            'average_precipitation': average_precipitation,
            'average_visibility': average_visibility
        }

    combined_data = pd.DataFrame.from_dict(results).transpose()
    combined_data.to_csv(f"weather_data.csv", index=True)


    fetch_weather_data()