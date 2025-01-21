# surface-pavedness-to-hot
This is a small repository to be able to match Street View Imagery based on DL and Mapillary with HOTOSM data on HDX and update it

## get_hot.py
This script downloads all related coountry road data from HOTOSM on HDX via the HDX API.

the data itself will be saved a hard directory on SDS ending with '/hotosm_files' whereas the metadata to '/hotosm_resources'
At the moment the filepaths are hardcoded as it was aimed to be done only one originally

## merge_hotosm.py
the script merges the downloaded hotosm files with the road related metadata from DL and Mapillary based on the osm id

Finally, it saves the individual files as GPKG and GeoJSON respectively and creates three csv files to compare which countries are lacking in which dataset (HOTOSM or Road Surface Quality)

## create_stats.py
This script creates statistics based on the merged country files so that they can be uploaded to HOTOSM individually. Stats include 'total_road_length, 'total_unpaved_length', 'total_paved_length', 'total_missing_length' indicating the roads without any surface label as well as 'provided_length' meaning the length of roads with surface labels. All values are in kilometers

## compare_hot_our_data.py 
After having pushed the country files to HOTOSM, it became apparent that some countries were missing. This script performs operations to find those countries and compare the Road Surface Quality Dataset with HOTOSM

## other_countries.py
The missing countries on the side of HOTOSM were Tajikistan (TJ) and Kosovo (XK). This script creates the files for those countries following the HOTOSM standards and then expands them with road surface data






