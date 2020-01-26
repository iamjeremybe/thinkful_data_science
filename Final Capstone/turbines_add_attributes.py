#!/usr/bin/python3

import time
import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.ops import nearest_points
from shapely.ops import unary_union

capstone_path = ('/home/jeremy/Documents/Jupyter/Final Capstone')

print("Starting at ",time.strftime('%X %x %Z'))

# # For this example, I have three datasets to work with:
# ## 1. "lower48_borders"--this contains US state/national borders for the contiguous 48 states.
print("Opening US borders file ",time.strftime('%X %x %Z'))
lower48_borders = gpd.read_file('{}/lower48_borders.geojson'.format(capstone_path),driver='GeoJSON')

# ## 2. "existing_turbines"--these points are existing wind turbines.
print("Opening existing turbines file ",time.strftime('%X %x %Z'))
existing_turbines_US = gpd.read_file('{}/existing_turbines.geojson'.format(capstone_path),driver='GeoJSON')
existing_turbines_US.rename(columns={'index':'t_index'},inplace=True)

# ## 3. "power_lines"--high-voltage transmission lines.
print("Opening transmission lines file ",time.strftime('%X %x %Z'))
power_lines = gpd.read_file("{}/Electric_Power_Transmission_Lines/Electric_Power_Transmission_Lines.shp".format(capstone_path))
power_lines = power_lines.loc[(power_lines['VOLTAGE'] > 0) &
                             (~power_lines['VOLT_CLASS'].isin(['UNDER 100','100-161','NOT AVAILABLE'])) &
                              (power_lines['STATUS'].isin(['IN SERVICE','UNDER CONST']))]

print("Joining transmission lines to borders to obtain state ",time.strftime('%X %x %Z'))
power_out_df = gpd.GeoDataFrame()

# The number of records we will process at one time:
power_chunk_size = 1000
power_length = power_lines.shape[0]
power_first_idx = 0
power_last_idx = power_chunk_size
print("power_length: {}".format(power_length))
not_done = True
# Add state to the power line dataset, in chunks so we don't blow out memory
while not_done:
    print("power_lines[{}:{}]".format(power_first_idx,power_last_idx))
    print("Adding state...")
    power_chunk_df = gpd.sjoin(power_lines[power_first_idx:power_last_idx],
                              lower48_borders[['STUSPS','geometry']],
                              how='left').to_crs({'init': 'epsg:4326'})

    power_chunk_df.drop('index_right',axis='columns',inplace=True)

    power_out_df = gpd.GeoDataFrame(pd.concat([power_out_df,power_chunk_df],
                                             ignore_index=True),
                                   crs=power_chunk_df.crs)

    power_last_idx += power_chunk_size
    power_first_idx += power_chunk_size
    if power_last_idx > power_length:
        power_last_idx = power_length
    if power_first_idx > power_length:
        not_done = False # In other words, we are not not_done. :)

power_out_df.rename({'STUSPS': 'state'},axis='columns',inplace=True)
power_out_df = power_out_df.loc[~power_out_df['state'].isin(['HI','AK'])]
power_out_df.to_file('{}/power_out_df.geojson'.format(capstone_path),driver='GeoJSON')

# Use our augmented power line dataset to create some features for the turbine dataset.
using_neighbors = False
for state in power_out_df['state'].unique():
    print("Writing power line GeoJSON file for {} ".format(state),time.strftime('%X %x %Z'))
    if power_out_df.loc[power_out_df['state'] == state].shape[0] == 0:
        print("No power line data for {} ".format(state),time.strftime('%X %x %Z'))
    else:
        power_out_df.loc[power_out_df['state'] == state].to_file("{}/power_by_state/power_{}.geojson".format(capstone_path,state), driver='GeoJSON')

for us_state in np.sort(lower48_borders['STUSPS'].unique()): #lower48_borders['STUSPS'].unique():
    print("Processing turbines for {} ".format(us_state),time.strftime('%X %x %Z'))
    existing_turbines = existing_turbines_US.loc[existing_turbines_US['t_state'] == us_state].copy(deep=True)
# If there are no turbines for a given state, skip ahead to the next state.
    if (existing_turbines.shape[0] == 0):
        print("No turbines found for {}. Skipping to the next state ".format(us_state),time.strftime('%X %x %Z'))
        continue
    print("Processing transmission lines for {} ".format(us_state),time.strftime('%X %x %Z'))
    power_lines = power_out_df.loc[power_out_df['state'] == us_state].copy(deep=True)
# If there are no power lines for a given state, find its neighboring states and use their power lines instead.
    if power_lines.shape[0] == 0:
        using_neighbors = True
        state_neighbors = lower48_borders.loc[lower48_borders['STUSPS'] == us_state,'neighbors'].tolist()[0].split(',')
        power_lines = power_out_df.loc[power_out_df['state'].isin(state_neighbors)]
        print("No power lines for {}. Using the power lines of its neighbors: {}".format(us_state,state_neighbors))
    
# What I'd like to do is find the nearest transmission line for each turbine,
# some of its attributes, and maybe an extra, related feature or two.
# Let's see if we can speed things up by creating a unary_union of all of the
# transmission lines--then, we'll only have that one (albeit large) object to
# compare to each point.
    
    print("Generating unary union for transmission lines ",time.strftime('%X %x %Z'))
    lines_union = power_lines['geometry'].unary_union
    
    nearest_df = gpd.GeoDataFrame(existing_turbines['t_index'].copy(deep=True))
# This calculation should be simpler if we're only comparing points to a single object...?
    nearest_df['geometry'] = existing_turbines['geometry'].apply(lambda x: nearest_points(x, lines_union))
    nearest_df['t_geometry'] = gpd.GeoSeries(nearest_df['geometry'].apply(lambda x: x[0]))
    nearest_df['geometry'] = gpd.GeoSeries(nearest_df['geometry'].apply(lambda x: x[1]))
    
    new_dataframe = pd.DataFrame()
    for idx,this_row in nearest_df.iterrows():
        print("INDEX {}:".format(idx))
        print("Location of the turbine: ",this_row['t_geometry'])
        print("Location of the closest point to this turbine on the transmission line: ",this_row['geometry'])
# .distance() is a method of Shapely objects, not of Geopandas series or dataframes
        pt_to_pt_distance = this_row['t_geometry'].distance(this_row['geometry'])
        print("Distance between these two points: ",pt_to_pt_distance)
# What's the distance from our line(s?) to the point in this_row, which should be ON the line?
        pt_on_line_distances = power_lines['geometry'].distance(this_row['geometry']).values
#        print("Distances between the lines, and the point (allegedly) ON the line: ", pt_on_line_distances)
        min_pt_to_line_distance = pt_on_line_distances.min()
# np.where returns a one-dimensional array in this case, which contains a list...
        idx_pt_to_line_distance = np.where(pt_on_line_distances == min_pt_to_line_distance)[0][0]
        print("Minimum distance: {}; index of that value: {}".format(min_pt_to_line_distance,idx_pt_to_line_distance))

# Moving on...let's get the attributes for the transmission line, since we have its index.
# We'll end up with a dataframe that has the same number of rows as the turbine dataframe--easy to concat.
        line_attributes = power_lines.iloc[idx_pt_to_line_distance:idx_pt_to_line_distance+1]
# Add the distance between the turbine and the nearest transmission line
        line_attributes['dist_to_nearest_line'] = pt_to_pt_distance
        new_dataframe = pd.concat([new_dataframe,line_attributes],axis='rows')

    existing_turbines_updated = pd.concat([existing_turbines.reset_index(),
                                           new_dataframe[['OBJECTID','TYPE','VOLTAGE','VOLT_CLASS','SHAPE__Len','dist_to_nearest_line']].reset_index()],
                                          axis=1)
    existing_turbines_updated.drop(['index'],axis='columns',inplace=True)

# Create a feature: the number of transmission lines 
# within 1 degree (~69 miles/111 km) of the turbine.
# Start by creating a buffer around each point...
    existing_turbines_buffer = gpd.GeoDataFrame(existing_turbines['t_index'])
    existing_turbines_buffer['geometry'] = existing_turbines['geometry'].buffer(1)
    existing_turbines_buffer.crs = {'init': 'epsg:4326'}

# Count the number of lines that have a relationship to each turbine's buffer area
    candidate_lines = gpd.sjoin(power_lines, existing_turbines_buffer, how='right')
    candidate_line_cts = candidate_lines.groupby('t_index',as_index=False).count()[['t_index','ID']]
    candidate_line_cts = candidate_line_cts.rename(columns={'ID': 'line_count'})

# Add those line counts to the turbine dataset
    existing_turbines_updated = existing_turbines_updated.merge(right=candidate_line_cts,
                                                                left_on='t_index',
                                                                right_on='t_index',
                                                                how='left')
    print("Count frequency for power lines w/in turbine buffers:",existing_turbines_updated['line_count'].value_counts())

# Next up: open the NREL dataset w/exclusions. We can try to join to it to add wind_class to each turbine. 
    nrel_wexcl = gpd.read_file('{}/nrel_w_exclusions_by_state/nrel_w_exclusions_{}.geojson'.format(capstone_path,us_state),driver='GeoJSON')

    existing_turbines_add_nrel = gpd.sjoin(existing_turbines_updated,
                                           nrel_wexcl[['wind_class','geometry']].to_crs({'init': 'epsg:4269'}),
                                           how='left')
    existing_turbines_add_nrel.drop('index_right',axis='columns',inplace=True)
    existing_turbines_add_nrel.loc[:,'wind_class'] = existing_turbines_add_nrel['wind_class'].fillna(2)

    print("Writing turbine GeoJSON file for {} ".format(us_state),time.strftime('%X %x %Z'))
    existing_turbines_add_nrel.to_file("{}/turbines_by_state/turbines_{}.geojson".format(capstone_path,us_state), driver='GeoJSON')

print("Completed at ",time.strftime('%X %x %Z'))
