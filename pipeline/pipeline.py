import meteo_lib as ml
import db.database as db
import pipeline.queries as qu
from os import remove


weather_stations = [
    [ 
        'DE',
        [
            'Berlin / Tegel'
        ]
    ]
]


weather_attributes = [
    'time',
    'tavg'
]

def run(batch_type):
    """
    main function to trigger the whole pipeline
    """

    # getting station id's to get the data for each station
    print('\nGetting Station IDs...\n')
    stations_id_list = ml.get_station_id(weather_stations)

    # Getting connection
    conn = db.connect()

    # Creating tables if not created
    print("Table Creating if not exists...\n")
    status = db.execute_query(conn, qu.create_table_query)
    if status == 0:
        print("Error in Table Creation...\n")
        return 0
    status.close()


    # Calling either daily or historical function depends upon batch_type
    status = batch_type_dict[batch_type](batch_type, stations_id_list, conn)
    if status != 0:
        print('Script completed successfully...\n')

    db.disconnect(conn)


def daily_run(batch_type, stations_id_list, conn):
    """ Daily Run function"""

    print('Daily Job Triggered...\n')

    print('Truncating data...\n')
    status = db.execute_query(
        conn,
        qu.daily_table_deletion.format(ml.date_filter, ml.now.year, ml.now.month)
    )
    if status == 0:
        print("Error in Truncating data...\n")
        return 0
    status.close()

    status = load_data(stations_id_list, batch_type, conn)
    if status == 0:
        return 0

    status = db.execute_query(conn, qu.insertion_from_staging)
    if status == 0:
        print("Insertion in Staging failed...\n")
        return 0
    status.close()

    print('Data inserted from staging table...\n')

    # executing query to update monthly agg table
    status = db.execute_query(
        conn, 
        qu.daily_monthly_agg_query.format(ml.now.year, ml.now.month)
    )
    if status == 0:
        print("Error duing monthly agg query...\n")
        return 0
    status.close()
        

def historical_run(batch_type, stations_id_list, conn):
    """ Historical Run function"""

    print('Historical Job Triggered...\n')

    print('Truncating data...\n')
    status = db.execute_query(conn, qu.history_table_deletion)
    if status == 0:
        print("Error in Truncating data...\n")
        return 0
    status.close()

    status = load_data(stations_id_list, batch_type, conn)
    if status == 0:
        return 0

    status = db.execute_query(conn, qu.insertion_from_staging)
    if status == 0:
        print("Insertion in Staging failed...\n")
        return 0
    status.close()
    
    print('Data inserted from staging table...\n')

    # executing query to update monthly agg table
    status = db.execute_query(
        conn, 
        qu.historical_monthly_agg_query
    )
    if status == 0:
        print("Error duing monthly agg query...\n")
        return 0
    status.close()



def data_transformation(data, station_name):
    # only selecting required attributes from data
    data = data[data.columns.intersection(weather_attributes)]

    # adding weather station name, year, month and date column into data
    data.insert(loc=0, column='station', value=station_name)
    data.insert(loc=2, column='year', value=data['time'].dt.year)
    data.insert(loc=3, column='month', value=data['time'].dt.month)
    data.insert(loc=4, column='day', value=data['time'].dt.day)

    return data


def load_data(stations_id_list, batch_type, conn):
    tmp_file = "data.csv"
    print("Creating data file...\n")
    for sil in stations_id_list:
        # Getting historical data from meteostat python library
        data = ml.get_data(batch_type, sil[0])

        # calling function to get filtered data columns
        data = data_transformation(data, sil[1])

        data.to_csv(tmp_file, index=False, header=False, mode='a')

    f = open(tmp_file, 'r')
    status = db.execute_query(
        conn, 
        qu.change_schema.format('comatch')
    )
    status.close()
    status = db.load_data(conn, f, qu.weather_staging_table)
    if status == 0:
        print("Error in Loading data file...\n")
        f.close()
        return 0
    
    f.close()
    remove(tmp_file)
    print("Data Loaded successfully...\n")


batch_type_dict = {
    'daily'     : daily_run,
    'historical': historical_run
}