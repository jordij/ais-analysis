import os
import pandas as pd
import sys
from multiprocessing import Pool

from db_helper import (
    delete_dupes,
    populate_db_from_json,
    get_vessels,
    get_vessel_data,
)
from df_helper import get_stop_points, get_geodf


def main(args):
    """
    Main method of the app. Reads input file, populated DB,
    performs tracked analysis per each identified vessel.
    Results are written in specified file as args[1]
    """
    print('⏳ AIS Tracking Analysis started.. time for a ☕')

    current_directory = os.path.dirname(__file__)
    try:
        input_file = args[0]
        file_path = os.path.join(current_directory, '..', input_file)
    except Exception as e:
        print('Need a valid input file')
        return

    if not args[1].endswith('.json'):
        print('Need a valid output file')
        return

    if not os.path.exists(file_path):
        print('Need a valid input file')
        return

    # Populate DB
    print('🚢 Loading vessel data..')
    populate_db_from_json(file_path)

    print('🔨 Deleting duplicates (same userid/vessel and timestamp)..')
    delete_dupes()

    # Fetch vessel data from db and process it
    vessels = get_vessels()  # fetch list of vessel IDs
    nvessels = len(vessels)
    print('⚒️  Processing data from %d vessels..' % nvessels)
    # 10 parallel processes to process all vessel DFs in chunks of 50
    pool = Pool(processes=10)
    batch = 50

    all_stop_points = []
    for i in range(0, nvessels, batch):
        batch_vessels = vessels[i : i + batch]
        vessel_dfs = [get_vessel_data(vessel_id) for vessel_id in batch_vessels]
        vessel_gdfs = pool.map(get_geodf, vessel_dfs)
        stop_points = pool.map(get_stop_points, vessel_gdfs)
        batch_stop_points = pd.concat(stop_points)
        all_stop_points.append(batch_stop_points)
        print(
            '     %d Stop points calculated from %d vessels..'
            % (len(batch_stop_points.index), len(batch_vessels))
        )
    # Write results to given output file
    concat_points = pd.concat(all_stop_points)
    concat_points.to_file(args[1], driver="GeoJSON")
    print('✅ Nice work, %d vessels processed' % nvessels)
    print('🎉 %d stop points stored in %s' % (len(concat_points.index), args[1]))
    pool.close()


if __name__ == '__main__':
    main(sys.argv[1:])
