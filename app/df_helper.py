import geopandas as gpd
import movingpandas as mpd
import pandas as pd
import shapely

from datetime import datetime, timedelta


def get_stop_points(gdf):
    # calculate stops with min duration of 1h and 1knot based diameter (1852 m)
    traj = mpd.TrajectoryCollection(gdf, 'userid')
    stops = mpd.TrajectoryStopDetector(traj).get_stop_segments(
        min_duration=timedelta(seconds=3600), max_diameter=1852
    )
    return stops.get_start_locations()


def get_geodf(df):
    # transofrm df to geodf
    return gpd.GeoDataFrame(
        df.drop(['latitude', 'longitude'], axis=1),
        crs='epsg:4326',
        geometry=df.apply(
            lambda row: shapely.geometry.Point((row.longitude, row.latitude)), axis=1
        ),
    ).set_index('utctimestamp')
