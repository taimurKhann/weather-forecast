import unittest
import db.database as db
import meteo_lib as ml
import main as m

weather_stations = [
    [ 
        'DE',
        [
            'UFS TW Ems',
            'Berlin / Tegel'
        ]
    ]
]

unkonwn_weather_stations = [
    [ 
        'E',
        [
            'Test'
        ]
    ]
]

class Testing(unittest.TestCase):

    def test_connection(self):
        conn = db.connect()
        self.assertNotEqual(conn, 1)
        curr = db.execute_query(conn, "Select version()")
        val = curr.fetchall()
        self.assertTrue(False if not val else True)
        curr.close()
        db.disconnect(conn)


    def test_station_id(self):
        station_id_list = ml.get_station_id(weather_stations)

        self.assertEqual(station_id_list[0][0], '10004')
        self.assertEqual(station_id_list[1][0], '10382')

    def test_unkonwn_station_id(self):
        station_id_list = ml.get_station_id(unkonwn_weather_stations)

        self.assertTrue(True if not station_id_list else False)


if __name__ == '__main__':
    unittest.main()