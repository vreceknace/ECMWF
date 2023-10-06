"""
This script is meant to run on francesca server. It is used to preprocess ECMWF data and upload it to the database.
"""

import os
import sys
from Code_repo.ECMWF_grib_reader import EcmwfGrib

# Define path of ECMWF data
ecmwf_path = input('Enter path to ECMWF data: ')
ecmwf_path = os.path.join('r', ecmwf_path)

loop = input('Loop through files? (y/n): ')

if loop == 'y':
    loop = True
else:
    loop = False

# Load ecmwf data to dataframe
ecmwf_df_2022 = EcmwfGrib(ecmwf_path, loop=loop)
print('Successfully loaded ECMWF data to dataframe')

# Save dataframe to pickle
save = input('Do you want to save dataframe to pickle? (y/n):')

if save == 'y':
    save_path = input('Do you want to save dataframe to a specific path? (y/n):')
    if save_path == 'y':
        save_path = input('Enter path to save dataframe to: ')
        save_path = os.path.join(save_path, 'ecmwf_df_2022.pkl')
    elif save_path == 'n':
        save_path = os.path.join(os.getcwd(), 'ecmwf_df_2022.pkl')

    ecmwf_df_2022.to_pickle(save_path)
    print('Successfully saved dataframe to: ', save_path)

# Upload data to database
upload = input('Do you want to upload data to database? (y/n):')

# Save db credentials to dictionary
print('Enter db parameters:')
db_params = {
    "dbname": input('dbname: '),
    "user": input('user: '),
    "password": input('password: '),
    "host": input('host: '),
    "port": input('port: ')
}

if_exists = input('If data already exists in database, do you want to replace it or append it? (r/a):')

# Check if if_exists is valid
if if_exists == 'r':
    if_exists = 'replace'
elif if_exists == 'a':
    if_exists = 'append'

if upload == 'y':
    ecmwf_df_2022.append_to_db(db_params, if_exists=if_exists, index=True)
    print('Successfully uploaded data to database')

# Test upload to database
test_upload = input('Do you want to test upload to database? (y/n):')

if test_upload == 'y':
    ecmwf_df_2022.test_upload(db_params)




