from meteostat import Stations, Daily
from datetime import datetime

now = datetime.now()
date_filter = datetime(now.year, now.month, now.day)


def get_station_id(weather_stations):
    """ 
    Getting Station Ids for all required weather stations. return type (List) 
    """

    stations_id_list = []                                       # contains the list of station id's
    for ws in weather_stations:
        try:
            stations = Stations()
            stations = stations.region(ws[0])
            stations = stations.fetch()
            stations.reset_index(inplace=True)

            # Filtering station to only given weather station and extracting only id and name of station
            stations = stations[stations['name'].isin(ws[1])]
            stations = stations[['id', 'name']]

            # Converting df to List
            stations_id_list.extend(stations.values.tolist())
        except Exception as error:
            print(error)
            return 0
 
    return stations_id_list


def get_data(batch_type, station_id):
    """
    Getting data from meteostat library
    """
    
    # get daily data for historical or daily
    data = Daily(station_id, date_filter, date_filter) if batch_type == 'daily' else Daily(station_id)
    data = data.fetch()
    data.reset_index(inplace=True)

    return data