import re, os, logging, zipfile
import geopandas as gpd
import pandas as pd
from merge_hotosm import extract_first_wildcard
from concurrent.futures import ProcessPoolExecutor, as_completed

#PATH_TO_FILES = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files/updated_hotosm_files'
PATH_TO_FILES = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files/new_countries'
PATTERN = r'^heigit_(.*?)_roadsurface_lines*'

WORKERS = os.cpu_count()
WORKERS = 8

def zip_file(filepath):
    try:
        with zipfile.ZipFile(f'{filepath}.zip', 'w') as zipf:
            zipf.write(filepath, os.path.basename(filepath))
        #os.remove(filepath)
    except Exception as err:
        logging.warning(err)

def create_stats(country, filepath):
    result = {'country':country,
              'total_road_length':None,
              'total_unpaved_length':None,
              'total_paved_length':None,
              'total_missing_length':None,
              'provided_length':None,
               }
    try:
        gdf = gpd.read_file(filepath)
        result['total_road_length'] = gdf['osm_length'].sum()/1000
        result['total_unpaved_length'] = gdf.loc[gdf['combined_surface_DL_priority'] == 'unpaved', 'osm_length'].sum()/1000
        result['total_paved_length'] = gdf.loc[gdf['combined_surface_DL_priority'] == 'paved', 'osm_length'].sum()/1000
        result['total_missing_length'] = gdf.loc[gdf['combined_surface_DL_priority'].isna(), 'osm_length'].sum()/1000
        #result['provided_length'] = gdf.loc[gdf['osm_surface'].isna(), 'predicted_length'].sum()/1000 
        result['provided_length'] = gdf.loc[gdf['surface'].isna(), 'predicted_length'].sum()/1000 
        zip_file(filepath)
    except Exception as err:
        logging.warning(err)
    return result

if __name__ == '__main__':
    unique_countries =  set()
    files = {}

    for f in os.listdir(PATH_TO_FILES):
        country = extract_first_wildcard(f, PATTERN)
        if country is not None and country not in unique_countries:
            files[country.lower()] = f'{PATH_TO_FILES}/{f}'
            unique_countries.add(country)

    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = []
        country_data = []
        for country, filepath in files.items():
            futures.append(executor.submit(create_stats, country, filepath))

        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    country_data.append(result)
            except Exception as err:
                logging.warning(err)
    
    country_data = pd.DataFrame(country_data)
    #country_data.to_csv(f'{PATH_TO_FILES}/country_stats.csv', index=False)
    country_data.to_csv(f'{PATH_TO_FILES}/new_country_stats.csv', index=False)




