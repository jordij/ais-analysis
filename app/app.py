import os
import sys


from data_helper import (
    delete_dupes,
    get_stop_points,
    populate_db_from_json,
    get_vessels,
    get_vessel_data,
)


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

    # Get stop points for each vessel
    print(
        '⚒️  Processing vessel data.. might take a while depending on your input data..'
    )
    vessels = get_vessels()  # list of vessel IDs
    all_stop_points = None
    for vessel_id in vessels:
        vessel_df = get_vessel_data(vessel_id)
        stop_pts = get_stop_points(vessel_df)
        if all_stop_points is None:
            all_stop_points = stop_pts
        else:
            all_stop_points = all_stop_points.append(stop_pts)

    # Write results to given output file
    if all_stop_points is None:
        print('🚨 Oops something went wrong, no vessel data??')
    else:
        all_stop_points.to_file(args[1], driver="GeoJSON")
        print('✅ Nice work, %d vessels processed' % len(vessels))


if __name__ == '__main__':
    main(sys.argv[1:])
