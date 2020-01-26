#!/usr/bin/python3

import os
import fnmatch
import time
import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.geometry import Point
from shapely.ops import nearest_points
from shapely.ops import unary_union

debug=False

# This script will loop through the NREL dataset without exclusions, adding 
# the remaining features we will need for the supervised learning model.
# This dataset eats an ENORMOUS amount of RAM trying to calculate
# these attributes if vectorized instead of looped, unfortunately.

#disk_dir='/media/jeremy/Seagate Backup Plus Drive/wind_capstone'
disk_dir='/home/jeremy/Google_Drive/wind_capstone' 
#local_dir='/home/jeremy/Google_Drive/wind_capstone' #Documents/Jupyter/Final Capstone'
local_dir='/media/jeremy/Seagate Backup Plus Drive/wind_capstone'

# Some summary statistics will be generated for each state, and compiled in a single dataframe.
# This way, we won't have to open 48 files later to obtain these stats to compare.
national_df = pd.DataFrame(['count','unique','top','freq','mean','std','min','25%','50%','75%','max'],columns={'index'})
national_df.set_index('index',inplace=True)

print("Starting at ",time.strftime('%X %x %Z'))

# Read in the NREL no exclusion files that were generated for each state.
all_nrel = gpd.GeoDataFrame()
for filename in os.listdir('{}/nrel_no_exclusions_by_state/'.format(local_dir)):
    missing_power = False
    missing_turbines = False
# Skip files that don't have a two-letter abbreviation for state.
# Unfortunately, this includes a lot of great offshore locations...
# but we don't have any power line data, calculating it would be a pain,
# and only a turbine or two may be out there anyway.
    if not (fnmatch.fnmatch(filename, 'nrel_no_exclusions_??.geojson')):
        print("Found and skipping a stray file: ",filename)
        continue
    us_state = filename.split('_')[3].split('.')[0]
    if us_state in ['KS','AL','NH','MA']: # Test states, already completed
        print("Skipping {}; continue".format(us_state))
        continue

    print("Reading NREL file for ",us_state,time.strftime('%X %x %Z'))
    nrel_wind_data = gpd.read_file('{}/nrel_no_exclusions_by_state/{}'.format(local_dir,filename),driver='GeoJSON')

# Open the corresponding power line state file (NOTE: WY does not have one--handle that!)
    print("Reading power line file for",us_state,time.strftime('%X %x %Z'))
    missing_power = False
    try:
        power_lines = gpd.read_file('{}/power_by_state/power_{}.geojson'.format(disk_dir,us_state),driver='GeoJSON')
    except:
        print("Couldn't open that file! We'll have to fillna some values for this state later.")
        missing_power = True
    lines_union = None
    if not missing_power:
        print("Creating unary union of power lines")
        lines_union = power_lines['geometry'].unary_union

# Open the corresponding turbine file too (NOTE: 9 or so states don't have one--handle that!)
    print("Reading turbine file for ",us_state,time.strftime('%X %x %Z'))
    missing_turbines = False
    try:
        turbines = gpd.read_file('{}/turbines_by_state/turbines_{}.geojson'.format(disk_dir,us_state),driver='GeoJSON')
    except:
        print("Couldn't open that file! We'll have to fillna some values for this state later.")
        missing_turbines = True

# Turn the centroid lat/long into a point. 
    nrel_wind_data['centroid'] = [Point(xy) for xy in zip(nrel_wind_data['centroid_long'], nrel_wind_data['centroid_lat'])]

# Create some features to add to the NREL dataset!
# 1. We have the centroid for each cell. Join to the power line dataset and:
#   a. Calculate the distance from it to the nearest power line.
    nearest_df = gpd.GeoDataFrame(nrel_wind_data['id'].copy(deep=True))
    if not missing_power:
        nearest_point = nrel_wind_data['centroid'].apply(lambda x: nearest_points(x, lines_union))
        nearest_df['centroid'] = gpd.GeoSeries(nearest_point.apply(lambda x: x[0]))
        nearest_df['geometry'] = gpd.GeoSeries(nearest_point.apply(lambda x: x[1]))
    
        new_dataframe = pd.DataFrame()
        for idx,this_row in nearest_df.iterrows():
            if idx%1000 == 0:
                print("INDEX {}".format(idx))
            if debug:
                print("Location of the centroid: ",this_row['centroid'])
                print("Location of the closest point to this centroid on the transmission line: ",this_row['geometry'])
# .distance() is a method of Shapely objects, not of Geopandas series or dataframes
            pt_to_pt_distance = this_row['centroid'].distance(this_row['geometry'])
            if debug:
                print("Distance between these two points: ",pt_to_pt_distance)
# What's the distance from our line(s?) to the point in this_row, which should be ON the line?
            pt_on_line_distances = power_lines['geometry'].distance(this_row['geometry']).values
            if debug:
                print("Distances between the lines, and the point (allegedly) ON the line: ", pt_on_line_distances)
            min_pt_to_line_distance = pt_on_line_distances.min()
# np.where returns a one-dimensional array in this case, which contains a list...
            idx_pt_to_line_distance = np.where(pt_on_line_distances == min_pt_to_line_distance)[0][0]
            if debug:
                print("Minimum distance: {}; index of that value: {}".format(min_pt_to_line_distance,idx_pt_to_line_distance))
    
# Moving on...let's get the attributes for the transmission line, since we have its index.
# We'll end up with a dataframe that has the same number of rows as the turbine dataframe--easy to concat.
            line_attributes = power_lines.iloc[idx_pt_to_line_distance:idx_pt_to_line_distance+1]
# Add the distance between the centroid and the nearest transmission line
            line_attributes.loc[:,'dist_to_nearest_line'] = pt_to_pt_distance
    
            new_dataframe = pd.concat([new_dataframe,line_attributes],axis='rows')
    
#   b. Get some attributes for the closest power line
        existing_wind_data_updated = pd.concat([nrel_wind_data.reset_index(),
                                           new_dataframe[['OBJECTID','TYPE','VOLTAGE','VOLT_CLASS','SHAPE__Len','dist_to_nearest_line']].reset_index()],
                                           axis=1)
        existing_wind_data_updated.drop(['index'],axis='columns',inplace=True)
        del new_dataframe
    
#   c. Calculate the number of power lines in a 1 degree radius (~69mi/111km)
        existing_wind_data_buffer = gpd.GeoDataFrame(nrel_wind_data[['id','centroid']].copy(deep=True))
        existing_wind_data_buffer['geometry'] = existing_wind_data_buffer['centroid'].apply(lambda x: x.buffer(1))
        existing_wind_data_buffer.crs = {'init': 'epsg:4326'}
    
# Count the number of lines that have a relationship to each NREL cell's buffer area
        candidate_lines = gpd.sjoin(power_lines, existing_wind_data_buffer, how='right')
        candidate_line_cts = candidate_lines.groupby('id',as_index=False).count()[['id','ID']]
        candidate_line_cts = candidate_line_cts.rename(columns={'ID': 'line_count'})
# Add those line counts to the NREL dataset
        existing_wind_data_updated = existing_wind_data_updated.merge(right=candidate_line_cts,on='id',how='left')
        print("Count frequency for power lines w/in NREL cells:",existing_wind_data_updated['line_count'].value_counts())
    else:
        existing_wind_data_updated = nrel_wind_data.copy(deep=True).reset_index(drop=True)

# Add these columns as NaNs to mimic what would have been added if we had a power line dataset
# Nearest line:
# 'OBJECTID','TYPE','VOLTAGE','VOLT_CLASS','SHAPE__Len','dist_to_nearest_line'
# 
# Lines within 1 degree of the centroid:
# 'line_count'
        existing_wind_data_updated = existing_wind_data_updated.assign(OBJECTID=np.nan,TYPE=np.nan,
                                            VOLTAGE=np.nan,VOLT_CLASS=np.nan,SHAPE__Len=np.nan,
                                            dist_to_nearest_line=np.nan,line_count=np.nan)

    if not missing_turbines:
# 2. Using the turbine dataset, calculate the number of turbines in this cell.
        turbines_here = gpd.sjoin(nrel_wind_data,turbines.to_crs({'init': 'epsg:4326'}),how='left')
# Eliminate rows with no turbines in them from the count.
        turbines_here = turbines_here.loc[~turbines_here['index_right'].isnull()]
        turbines_here_ct = turbines_here['id'].value_counts()
        turbines_here_ct.rename('turbine_count',inplace=True)
    
        existing_wind_data_updated = existing_wind_data_updated.merge(right=turbines_here_ct,
                                                                  left_on='id',right_index=True,
                                                                  how='left')
        existing_wind_data_updated.loc[:,'turbine_count'] = existing_wind_data_updated['turbine_count'].fillna(0)
    
# Each turbine has an associated wind_class from the NREL dataset with exclusions
# applied. For each cell, find the lowest wind_class among the cell's turbines.
        turbines_here = turbines_here.loc[~turbines_here['index_right'].isnull()]\
                                 .sort_values(by=['id','wind_class_right'])
        turbines_ct_w_dups = turbines_here[['id','wind_class_right']].sort_values(by=['id','wind_class_right'])
        turbines_ct_w_dups = turbines_ct_w_dups.loc[~turbines_ct_w_dups['id'].duplicated(keep='first')]
# Join to wind data on id and save the wind_class_right column
        existing_wind_data_updated = existing_wind_data_updated.merge(right=turbines_ct_w_dups[['id','wind_class_right']],
                                                                on='id',
                                                                how='left')
        existing_wind_data_updated.rename(columns={'wind_class_right': 'wind_class_turbine'},inplace=True)
        existing_wind_data_updated['wind_class_turbine'] = existing_wind_data_updated['wind_class_turbine'].fillna(0)
    else:
# Add these columns as NaNs to mimic what would have been added if we had a turbines dataset
# Number of turbines in this cell:
# 'turbine_count'
#
# Lowest wind class associated with a turbine in this cell:
# 'wind_class_turbine'
        existing_wind_data_updated = existing_wind_data_updated.assign(turbine_count=np.nan,wind_class_turbine=np.nan)

# We're done with these dataframes at this point.
# Free up memory, then open the NREL dataset w/exclusions and process it.
    print("Deleting processed dataframes for {} ".format(us_state),time.strftime('%X %x %Z'))
    if not missing_power:
        del power_lines,lines_union,candidate_lines,candidate_line_cts
    if not missing_turbines:
        del turbines,turbines_here,turbines_ct_w_dups
    del nrel_wind_data

    print("Reading NREL dataset w/exclusions for {} ".format(us_state),time.strftime('%X %x %Z'))
    nrel_w_exclusions = gpd.read_file('{}/nrel_w_exclusions_by_state/nrel_w_exclusions_{}.geojson'.format(local_dir,us_state),driver='GeoJSON')

# Create a unary union for each wind_class in the dataset.
# This will consume memory, but hopefully it cuts down the amount of work sjoin will have to do later..?
    nrel_w_exclusions_class = gpd.GeoDataFrame(columns=['wind_class','geometry'])
    for idx,wind_class in enumerate(nrel_w_exclusions['wind_class'].unique()):
        wind_class_row = gpd.GeoDataFrame({'wind_class': [wind_class],
                          'geometry': [nrel_w_exclusions.loc[nrel_w_exclusions['wind_class'] == wind_class].unary_union]})
        print(wind_class_row)
        nrel_w_exclusions_class = gpd.GeoDataFrame(pd.concat([nrel_w_exclusions_class,wind_class_row],
                                                        ignore_index=True,
                                                        sort=True),
                                                        crs={'init':'epsg:4326'})#wind_class_row.crs)
    print("Done calculating unary unions {}".format(us_state),time.strftime('%X %x %Z'))

    nrel_augmented_out = gpd.GeoDataFrame()

# The number of records we will process at one time.
# Process small datasets all at once.
    nrel_chunk_max = 1000
    nrel_length = existing_wind_data_updated.shape[0]
    if nrel_length < nrel_chunk_max:
        nrel_chunk_size = nrel_length + 1
    else:
# Break big datasets into digestible chunks.
# 1000 at a time pushed my laptop to the brink in a unit test,
# so let's go for 500.
        nrel_chunk_size = 500
        print("Dataset for {} is > {} rows ({}); breaking into chunks of {} records"\
             .format(us_state,nrel_chunk_max,nrel_length,nrel_chunk_size))
    nrel_first_idx = 0
    nrel_last_idx = nrel_first_idx + nrel_chunk_size
    not_done = True
# The polygons of the two NREL datasets don't seem to overlap neatly,
# but having any idea about how the two sets relate will be super useful.
# Joining polygons also seems to be resource intensive, but let's break down
# the datasets into chunks that my poor laptop can handle.
    while not_done:
        print("First index of this chunk:",nrel_first_idx,time.strftime('%X %x %Z'))
        nrel_chunk_df = gpd.sjoin(existing_wind_data_updated.loc[nrel_first_idx:nrel_last_idx],
                                  nrel_w_exclusions_class,
                                  how='left')
    
        nrel_chunk_df.drop('index_right',axis='columns',inplace=True)
        if debug:
            print("Columns of nrel_augmented_out:",nrel_augmented_out.columns)
            print("Columns of nrel_chunk_df:",nrel_chunk_df)
            print(nrel_chunk_df)
    
        nrel_augmented_out = gpd.GeoDataFrame(pd.concat([nrel_augmented_out,nrel_chunk_df],
                                              ignore_index=True,sort=True),
                                              crs=nrel_chunk_df.crs)
    
        nrel_last_idx += nrel_chunk_size
        nrel_first_idx += nrel_chunk_size
        if nrel_last_idx > nrel_length:
            nrel_last_idx = nrel_length
        if nrel_first_idx > nrel_length:
            not_done = False # In other words, we are not not_done. :)

    print("Writing augmented nrel_wind_data GeoJSON file for {} ".format(us_state),time.strftime('%X %x %Z'))
    nrel_augmented_out.rename(columns={'wind_class_left': 'wind_class',
                                       'wind_class_right': 'wind_class_excl',}
                                       ,inplace=True)

    nrel_augmented_out.loc[:,'wind_class_excl'] = nrel_augmented_out['wind_class_excl'].fillna(0)

# The NREL cells can match to multiple rows from the NREL set w/exclusions.
# Keep the row with the lowest wind_class_excl.
    nrel_augmented_out_dedup = nrel_augmented_out.sort_values(by=['id','wind_class_excl'])
    nrel_dupes = nrel_augmented_out_dedup.duplicated(subset=['OBJECTID', 'SHAPE__Len', 'TYPE', 'VOLTAGE', 'VOLT_CLASS',
                                                   'centroid_lat', 'centroid_long', 'dist_to_nearest_line',
                                                   'id', 'line_count', 'state', 'turbine_count', 'wind_class',
                                                   'wind_class_turbine'],keep='first')
    nrel_augmented_out_dedup = nrel_augmented_out.loc[~nrel_dupes]

    nrel_augmented_out_dedup.drop('centroid',axis='columns')\
                      .to_file("{}/nrel_augmented_by_state/nrel_augmented_{}.geojson".format(local_dir,us_state),
                               driver='GeoJSON')

# Capture these additional stats to add to national_df:
# 1. Power line density
    national_df['line_count_{}'.format(us_state)] = existing_wind_data_updated['line_count'].describe(include='all')
# 2. Distribution of dist_to_nearest_line
    national_df['dist_to_nearest_line_{}'.format(us_state)] = existing_wind_data_updated['dist_to_nearest_line'].describe(include='all')
    if debug:
        print(existing_wind_data_updated['line_count'].describe(include='all'))
        print(existing_wind_data_updated['dist_to_nearest_line'].describe(include='all'))

    print("Writing augmented national stats file",time.strftime('%X %x %Z'))
    national_df.reset_index(level=0).to_csv('{}/national_level_statistics_{}.csv'.format(local_dir,us_state),index=False)

print("Completed at ",time.strftime('%X %x %Z'))
