"""
Script for reading ECMWF grib files
"""
import os
import numpy as np
import cfgrib
import pandas as pd


class EcmwfGrib:
    """
    This class reads ECMWF grib files in specified directory and time range and returns ECMWF parameters
     for given locations
    """

    def __init__(self, file_path, loop=False):
        self.file_path = file_path

        if loop:
            cfgrib_list = []
            for i, f in enumerate(os.listdir(file_path)):
                print('Reading file: ', i + 1, '/', len(os.listdir(file_path)))
                path = os.path.join(file_path, f)
                cfgrib_temp = cfgrib.open_dataset(path, indexpath='')
                cfgrib_temp.close()
                cfgrib_temp = cfgrib_temp.to_dataframe()
                cfgrib_list.append(cfgrib_temp)

            cfgrib_df = pd.concat(cfgrib_list)

        else:
            self.ds = cfgrib.open_dataset(file_path, indexpath='')
            self.ds.close()
            cfgrib_df = self.ds.to_dataframe()

        self.cfgrib_df = cfgrib_df.sort_values(by=['latitude', 'longitude', 'valid_time', 'step'])

        # Round multiindex (lat and lon) to match with coordinates of ECMWF points
        res_lat = self.cfgrib_df.index.get_level_values('latitude').to_series().apply(lambda x: round(x, 1))
        res_lon = self.cfgrib_df.index.get_level_values('longitude').to_series().apply(lambda x: round(x, 1))
        self.cfgrib_df['latitude'] = np.array(res_lat)
        self.cfgrib_df['longitude'] = np.array(res_lon)
        self.cfgrib_df.reset_index(drop=True, inplace=True)  # reset index to access columns
        self.cfgrib_df.drop(columns=['number'], axis=1,
                            inplace=True)  # drop number column (byproduct of resetting multiindex)

        # Recalculate step column (postgres does not support timedelta)
        self.cfgrib_df['step'] = self.cfgrib_df['step'].apply(lambda x: x.total_seconds() / 3600)  # convert to hours

    def get_forecast(self, locations=None):
        """
        locations: dictionary of locations (lat, lon, elev)
        """

        if locations is None:
            return self.cfgrib_df

        else:
            # Round locations to 1 decimal (to match with ECMWF coordinates (grid resolution is 0.1 deg)) and split to
            # lat/lon
            loc_lat = {k: np.round(v[0], 1) for k, v in locations.items()}
            loc_lon = {k: np.round(v[1], 1) for k, v in locations.items()}

            # Return rows where lat and lon are equal to location coordinates
            self.cfgrib_df = self.cfgrib_df.loc[
                (self.cfgrib_df['latitude'].isin(loc_lat.values())) &
                (self.cfgrib_df['longitude'].isin(loc_lon.values()))]

            return self.cfgrib_df

    def append_to_db(self, db_params, if_exists='append', index=False):
        """
        Append ECMWF data to database
        """
        import psycopg2
        from sqlalchemy import create_engine

        # # Create SQLAlchemy engine (onnect to database)
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:'
                                        f'{db_params["port"]}/{db_params["dbname"]}')

        conn = psycopg2.connect(**db_params)

        # Append to database
        self.cfgrib_df.to_sql(db_params["dbname"], engine, if_exists=if_exists, index=index)

        # Close connection
        conn.commit()
        conn.close()

    def test_upload(self, db_params):
        """
        Test upload to database
        """
        import psycopg2

        try:
            connection = psycopg2.connect(**db_params)
            print("Connected to the database {}".format(db_params["dbname"]))

        except psycopg2.Error as e:
            print("Error connecting to the database {}:".format(db_params["dbname"]), e)

    def to_pickle(self, file_path):
        """
        Save dataframe to pickle
        """
        self.cfgrib_df.to_pickle(file_path)