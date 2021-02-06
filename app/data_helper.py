import geopandas as gpd
import json
import movingpandas as mpd
import pandas as pd
import shapely
import sqlalchemy as db
import time

from datetime import datetime, timedelta

MESSAGE_TYPES = [1, 2, 3, 18, 19, 27]
NROWS = 200000

db_name = 'database'
db_user = 'username'
db_pass = 'secret'
db_host = 'db'
db_port = '5432'

# Connecto to the database
db_string = 'postgres://{}:{}@{}:{}/{}'.format(
    db_user, db_pass, db_host, db_port, db_name
)
engine = db.create_engine(db_string)
conn = engine.connect()
metadata = db.MetaData()
messages_tb = db.Table('messages', metadata, autoload=True, autoload_with=engine)


def insert_rows(rows):
    """
    Raw insertion of rows into the 'messages' table.
    """
    data = [
        {
            "userid": row['Message']['UserID'],
            "messageid": row['Message']['MessageID'],
            "utctimestamp": "'%s'"
            % datetime.utcfromtimestamp(row['UTCTimeStamp']).strftime(
                '%Y-%m-%d %H:%M:%S'
            ),
            "latitude": row['Message']['Latitude'],
            "longitude": row['Message']['Longitude'],
            "speed": row['Message']['Sog'],
        }
        for row in rows
    ]
    insert_query = messages_tb.insert().values(data)
    engine.execute(insert_query)


def get_vessels():
    """
    Returns a list of unique vessel IDs (UserID col from messages table)
    """
    ids = []
    results = conn.execute("SELECT DISTINCT UserID FROM messages;")
    for r in results:
        ids.append(r[0])
    return ids


def get_vessel_data(vessel_id):
    """
    Get all DB messages related to given vessel ID and return a tuple
    wit the dataframe and the stops collection
    """
    query = db.select([messages_tb]).where(messages_tb.columns.userid == vessel_id)
    df = pd.read_sql(query, conn)
    # assemble geodf
    return gpd.GeoDataFrame(
        df.drop(['latitude', 'longitude'], axis=1),
        crs='epsg:4326',
        geometry=df.apply(
            lambda row: shapely.geometry.Point((row.longitude, row.latitude)), axis=1
        ),
    ).set_index('utctimestamp')


def get_stop_points(gdf):
    # calculate stops with min duraiton of 1h and 1knot based diameter (1852 m)
    traj = mpd.TrajectoryCollection(gdf, 'userid')
    stops = mpd.TrajectoryStopDetector(traj).get_stop_segments(
        min_duration=timedelta(seconds=3600), max_diameter=1852
    )
    return stops.get_start_locations()


def populate_db_from_json(file_path):
    """
    Populate DB from given JSON file (only if table messages is empty)
    """
    total_count = 0
    with open(file_path, 'rb') as file:
        rows = []
        for line in file:
            json_obj = json.loads(line)
            if json_obj['Message']['MessageID'] in MESSAGE_TYPES:
                rows.append(json_obj)
            if len(rows) >= NROWS:
                t0 = time.time()
                insert_rows(rows)
                total_count += NROWS
                print(
                    '%d new rows in %s seconds. Total # of rows: %d'
                    % (NROWS, str(time.time() - t0), total_count)
                )
                rows = []
        if len(rows) > 0:
            insert_rows(rows)


def delete_dupes():
    # remove dupes (same userid and timestamp)
    t0 = time.time()
    conn.execute(
        """
        DELETE FROM messages a USING (
        SELECT MIN(ctid) as ctid, userid, utctimestamp
        FROM messages
        GROUP BY userid,utctimestamp HAVING COUNT(*) > 1) b
        WHERE a.userid = b.userid AND a.utctimestamp = b.utctimestamp
        AND a.ctid <> b.ctid;
        """
    )
    print('     Dupes deleted in %s seconds' % str(time.time() - t0))
    count = conn.execute("SELECT COUNT(*) FROM messages;")
    print('     Total number of rows in messages table: %s' % count.first()[0])
