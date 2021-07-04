import db.database as db
import pipeline.queries as qu
from pprint import pprint

conn = db.connect()

cur = db.execute_query(conn, qu.tegel_monthly_tavg_report)
if cur == 0:
    print('Error in executing report...\n')

cur_value = cur.fetchall()
if not cur_value:
    print('No record found...\n')
else:
    pprint(cur_value)

db.disconnect(conn)