#!/usr/bin/python3

import time
import numpy as np
import pandas as pd
import geopandas as gpd

disk_dir = '/media/jeremy/Seagate Backup Plus Drive/wind_capstone'

# This script will loop through one of the NREL datasets,
# adding state, and extracting the latitude+longitude
# of the centroid for each cell. 
# This dataset eats an ENORMOUS amount of RAM trying to calculate
# these attributes if vectorized instead of looped, unfortunately.

print("Starting at ",time.strftime('%X %x %Z'))

# We can determine state by joining to this dataset of US boundaries
borders = gpd.read_file("./tl_2017_us_state/tl_2017_us_state.shp")
lower48_borders = borders.loc[(borders['REGION'] < '5') & (~borders['STUSPS'].isin(['AK','HI']))].to_crs({'init': 'epsg:4326'})

del borders

for qualifier in ['no']: #['w','no']:
    print("Opening {} NREL file ".format(qualifier),time.strftime('%X %x %Z'))
    if (qualifier == 'w'):
        nrel_wind_data = gpd.read_file('./ref-wind-with-exclusions/ref_wind.shp')
    elif (qualifier == 'no'):
        nrel_wind_data = gpd.read_file('./ref-wind-no-excl/ref_wind_no_excl.shp')
    else:
        raise ValueError("{} is not a valid value for qualifier".format(qualifier))
    sjoin_op = 'intersects'
    
    nrel_wind_data.drop('gid',axis='columns',inplace=True)
    
    nrel_out_df = gpd.GeoDataFrame()
    
    # The number of records we will process at one time:
    nrel_chunk_size = 1000
    nrel_length = nrel_wind_data.shape[0]
    nrel_first_idx = 0
    nrel_last_idx = nrel_chunk_size
    print("nrel_length: {}".format(nrel_length))
    not_done = True
    while not_done: 
        print("nrel_wind_data[{}:{}]".format(nrel_first_idx,nrel_last_idx))
        print("Adding state and extracting centroid...")
        nrel_chunk_df = gpd.sjoin(nrel_wind_data[nrel_first_idx:nrel_last_idx],
                                  lower48_borders[['STUSPS','geometry']],
                                  op=sjoin_op,
                                  how='left').to_crs({'init': 'epsg:4326'})
    
        nrel_chunk_df.drop('index_right',axis='columns',inplace=True)
        nrel_chunk_df['centroid_lat'] = nrel_chunk_df['geometry'].centroid.y
        nrel_chunk_df['centroid_long'] = nrel_chunk_df['geometry'].centroid.x
    
        nrel_out_df = gpd.GeoDataFrame(pd.concat([nrel_out_df,nrel_chunk_df],
                                                 ignore_index=True),
                                       crs=nrel_chunk_df.crs)
    
        nrel_last_idx += nrel_chunk_size
        nrel_first_idx += nrel_chunk_size
        if nrel_last_idx > nrel_length:
            nrel_last_idx = nrel_length 
        if nrel_first_idx > nrel_length:
            not_done = False # In other words, we are not not_done. :)
    
    nrel_out_df.rename({'STUSPS': 'state'},axis='columns',inplace=True)
    nrel_out_df['state'].fillna('Offshore',inplace=True)
#    if not rerun:
#        nrel_out_df.to_file('{}/nrel_{}_exclusions_all.geojson'.format(disk_dir,qualifier), driver='GeoJSON')
#    else:
#        nrel_out_df = gpd.read_file('{}/nrel_{}_exclusions_all.geojson'.format(disk_dir,qualifier), driver='GeoJSON')
    for state in nrel_out_df['state'].unique():
# I should make this more rerunnable. Best I can do in a pinch:
# Skip Offshore, which is garbage.
        if state in ['Offshore']: 
            continue
# Whittle down the augmented dataset to what is within the borders of the state.
# I thought sjoin did this, but it doesn't--.overlay() does.
        print("Running an overlay for {}".format(state),time.strftime('%X %x %Z'))
        nrel_out_df_state = gpd.overlay(nrel_out_df,
                                  lower48_borders.loc[lower48_borders['STUSPS'] == state,['geometry']],
                                  how='intersection')

# Skip offshore records. I'm not going to use them anyway.
# Don't attempt to write empty output files, either.
        if (state != 'Offshore') & (nrel_out_df_state.shape[0] > 0):
            print("Writing {} GeoJSON file for {} ".format(qualifier,state),time.strftime('%X %x %Z'))
            nrel_out_df_state.loc[nrel_out_df_state['state'] == state].to_file("{}/nrel_{}_exclusions_by_state/nrel_{}_exclusions_{}.geojson".format(disk_dir,qualifier,qualifier,state), driver='GeoJSON')

print("Completed at ",time.strftime('%X %x %Z'))
