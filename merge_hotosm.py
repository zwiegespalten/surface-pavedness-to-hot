import os, re, logging, zipfile, random
import duckdb
import pandas as pd
import geopandas as gpd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from shapely import from_wkt, to_wkt

PATH_TO_HOTOSM = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files/hotosm_files'
PATH_TO_HOTOSM = 'C:/Users/admin/Desktop'

PATH_TO_OUR_DATA = '/mnt/sds-hd/sd17f001/eren/mapillary_final/predictions_osm'
PATH_TO_OUR_DATA = 'C:/Users/admin/Desktop'

PATH_TO_UPDATED_HOTOSM = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files/updated_hotosm_files'
PATH_TO_UPDATED_HOTOSM = 'C:/Users/admin/Desktop'

PATTERN = r'^hotosm_(.*?)_roads_lines_*'
WORKERS = 4
MEM = 234

SUB_MEMORY = round(MEM / WORKERS)
SUB_THREADS = (os.cpu_count() / WORKERS)

COUNTRIES = set()
NOT_IN_OUR_COUNTRIES = set()
NOT_IN_HOTS_COUNTRIES = set()

def extract_first_wildcard(test_string, pattern):
    match = re.search(pattern, test_string)
    if match:
        first_part = match.group(1)
        return first_part
    else:
        return None

def strip_char(string):
    return string.split('/')[-1]

def create_and_zip(result,filepath, driver):
    try:
        result.to_file(filepath, driver=driver, index=False)
        with zipfile.ZipFile(f'{filepath}.zip', 'w') as zipf:
            zipf.write(filepath, os.path.basename(filepath))
        os.remove(filepath)
    except Exception as err:
        logging.warning(err)

def find_rows(country, directory, memory, threads):
    result = None
    query = f"""
    SELECT
        continent,
        country as country_iso_a2,
        country_iso_a3, 
        urban, urban_area, 
        strip_char(osm_id) as osm_id, 
        osm_tags_highway, osm_tags_surface, osm_surface_class, osm_surface,
        pred_class, pred_label,
        combined_surface_osm_priority, combined_surface_DL_priority,
        changeset_timestamp as osm_changeset_timestamp, 
        mean_timestamp as DL_mean_timestamp,
        length as osm_length, 
        predicted_length,
        n_of_predictions_used
    FROM read_parquet('{directory}/predictions_updated_*.parquet')
    WHERE list_contains(country_iso_a3, '{country}')
    """
    try:
        conn = duckdb.connect(':memory:')
        conn.execute(f"SET memory_limit='{memory}GB';")
        conn.execute(f"SET threads TO {threads};")
        conn.create_function("strip_char", strip_char, ['varchar'], 'int') 
        result = conn.execute(query).df()
    except Exception as err:
        logging.warning(err)
    finally:
        conn.close()
    return result
def merge_files(hotosm_filepath, our_filepath, memory, threads):
    result = None

    country = extract_first_wildcard(os.path.basename(hotosm_filepath), PATTERN)
    if country is not None:
        country = country.upper()

    hotosm_data = gpd.read_file(hotosm_filepath)    

    #This part ensures that columns like 'name:en', 'name:fr' are passed correctly to DUCKDB
    #This will be reset after the SQL query
    cols_containing_two_colons = [col for col in hotosm_data.columns if ':' in col]
    hotosm_data_columns = {
        col: (col.replace(':', '_') if ':' in col else col) 
        for col in hotosm_data.columns
    }
    hotosm_data.rename(hotosm_data_columns, axis=1, inplace=True)

    crs = hotosm_data.crs
    our_data = find_rows(country, our_filepath, memory, threads)

    #This sets the wanted order of columns
    final_columns = ['continent', 'country_iso_a2', 'country_iso_a3', 'urban', 'urban_area', 'osm_id', 'osm_type', 
                     'highway', 'surface', 'smoothness', 'osm_surface_class', 
                     'pred_class', 'pred_label', 'combined_surface_osm_priority', 'combined_surface_DL_priority', 
                     'osm_changeset_timestamp', 'DL_mean_timestamp', 'osm_length',
                     'predicted_length', 'n_of_predictions_used']
    rest_columns = set(['highway', 'surface', 'smoothness', 'osm_id', 'osm_type', 'geometry'])
    final_columns.extend([col for col in  hotosm_data.columns if col not in rest_columns])
    cols_to_be_parsed = (',').join([f'a.{col}' for col in hotosm_data.columns if col not in rest_columns])

    if our_data is not None:
        # DUCKDB does not recognize 'GEOMETRY' type so it has to be casted first
        hotosm_data['geometry'] = hotosm_data['geometry'].apply(lambda geom: to_wkt(geom))

        query = f"""
        WITH        
        hotosm AS(
            SELECT *
            FROM hotosm_data
        ),
        data AS(
            SELECT *
            FROM our_data
        )
        SELECT
            b.continent, b.country_iso_a2,
            b.country_iso_a3,
            b.urban, b.urban_area, b.osm_id, a.osm_type,
            CASE 
                WHEN b.osm_tags_highway IS NOT NULL THEN b.osm_tags_highway
                ELSE a.highway
            END as highway,
            CASE 
                WHEN b.osm_tags_surface IS NOT NULL THEN b.osm_tags_surface
                ELSE a.surface
            END AS surface,
            a.smoothness,
            CASE
                WHEN b.osm_surface_class IS NOT NULL THEN b.osm_surface_class
                WHEN list_contains(['paved', 'asphalt', 'chipseal', 'concrete', 'concrete:lanes', 'concrete:plates', 'paving_stones', 'sett', 'unhewn_cobblestone', 'cobblestone', 'bricks', 'metal', 'wood'], a.surface) THEN 'paved'
                WHEN list_contains(['unpaved', 'compacted', 'fine_gravel', 'gravel', 'shells', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'grass_paver', 'metal_grid', 'mud', 'sand', 'woodchips', 'snow', 'ice', 'salt'], a.surface) THEN 'unpaved'
                ELSE NULL
            END AS osm_surface_class,
            b.pred_class, b.pred_label,
            CASE
                WHEN b.combined_surface_osm_priority IS NOT NULL THEN b.combined_surface_osm_priority
                WHEN b.osm_surface_class IS NOT NULL THEN b.osm_surface_class
                WHEN list_contains(['paved', 'asphalt', 'chipseal', 'concrete', 'concrete:lanes', 'concrete:plates', 'paving_stones', 'sett', 'unhewn_cobblestone', 'cobblestone', 'bricks', 'metal', 'wood'], a.surface) THEN 'paved'
                WHEN list_contains(['unpaved', 'compacted', 'fine_gravel', 'gravel', 'shells', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'grass_paver', 'metal_grid', 'mud', 'sand', 'woodchips', 'snow', 'ice', 'salt'], a.surface) THEN 'unpaved'
                ELSE b.pred_class          
            END AS combined_surface_osm_priority,
            CASE
                WHEN b.pred_class IS NOT NULL THEN b.pred_class
                WHEN b.osm_surface_class IS NOT NULL THEN b.osm_surface_class
                WHEN list_contains(['paved', 'asphalt', 'chipseal', 'concrete', 'concrete:lanes', 'concrete:plates', 'paving_stones', 'sett', 'unhewn_cobblestone', 'cobblestone', 'bricks', 'metal', 'wood'], a.surface) THEN 'paved'
                WHEN list_contains(['unpaved', 'compacted', 'fine_gravel', 'gravel', 'shells', 'rock', 'pebblestone', 'ground', 'dirt', 'earth', 'grass', 'grass_paver', 'metal_grid', 'mud', 'sand', 'woodchips', 'snow', 'ice', 'salt'], a.surface) THEN 'unpaved'
                ELSE NULL  
            END AS combined_surface_DL_priority,
            b.osm_changeset_timestamp, 
            b.DL_mean_timestamp,
            b.osm_length, 
            b.predicted_length,
            b.n_of_predictions_used,
            {cols_to_be_parsed},
            a.geometry
        FROM hotosm a 
        LEFT JOIN our_data b
            ON CAST(a.osm_id AS INT) = CAST(b.osm_id AS INT)
        """
        try:
            conn = duckdb.connect(':memory:')
            conn.execute(f"SET memory_limit='{memory}GB';")
            conn.execute(f"SET threads TO {threads};")
            conn.execute(f"LOAD SPATIAL;")
            result = conn.execute(query).df()

            # geometry should be parsed back to 'GEOMETRY' type
            result['geometry'] = result['geometry'].apply(lambda geom: from_wkt(geom))
            COUNTRIES.add(country)
        except Exception as err:
            logging.warning(err)
        finally:
            conn.close()

    else:
        NOT_IN_OUR_COUNTRIES.add(country)
        for col in final_columns:
            if col not in hotosm_data.columns:
                hotosm_data[col] = np.nan
        
        result = hotosm_data[final_columns]

    del hotosm_data

    barename = os.path.basename(hotosm_filepath).split('.')[0]
    result = gpd.GeoDataFrame(result, geometry='geometry', crs=crs)

    final_columns = {
        col: (col.replace('_', ':') if col.replace('_', ':') in cols_containing_two_colons else col) 
        for col in final_columns
    }
    result.rename(final_columns, axis=1, inplace=True)
    create_and_zip(result,f'{PATH_TO_UPDATED_HOTOSM}/heigit_{country.lower()}_roadsurface_lines.gpkg', 'GPKG')
    create_and_zip(result,f'{PATH_TO_UPDATED_HOTOSM}/heigit_{country.lower()}_roadsurface_lines.geojson', 'GeoJSON')

if __name__ == '__main__': 
    if not os.path.exists(PATH_TO_UPDATED_HOTOSM):
        os.makedirs(PATH_TO_UPDATED_HOTOSM, exist_ok=True)

    hot_osm_files = [f'{PATH_TO_HOTOSM}/{f}' for f in os.listdir(PATH_TO_HOTOSM) if f.startswith('hotosm_') and (f.endswith('.geojson') or f.endswith('.shp') or f.endswith('.gpkg') or f.endswith('.kml'))]
    random.shuffle(hot_osm_files)

    #with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        
     #   futures = [executor.submit(merge_files, hotosm_filepath, PATH_TO_OUR_DATA, SUB_MEMORY, SUB_THREADS) for hotosm_filepath in hot_osm_files]

     #   for future in as_completed(futures):
     #       if future is not None:
     #           try:
     #               future.result()
     #           except Exception as err:
     #               logging.warning(err)

    for hotosm_filepath in hot_osm_files:
        try:
            merge_files(hotosm_filepath, PATH_TO_OUR_DATA, MEM, os.cpu_count())
        except Exception as err:
            logging.warning(err)

    ############################################################################################################################################################################
    NOT_IN_HOTS_COUNTRIES = duckdb.sql(f"""
    SELECT DISTINCT country_iso_a3
    FROM read_parquet('{PATH_TO_OUR_DATA}/predictions_updated_*.parquet')
    """
    ).df()

    NOT_IN_HOTS_COUNTRIES = NOT_IN_HOTS_COUNTRIES[~NOT_IN_HOTS_COUNTRIES['country_iso_a3'].isin(COUNTRIES)]
    COUNTRIES = pd.DataFrame(list(COUNTRIES), columns=['Numbers'])
    NOT_IN_OUR_COUNTRIES = pd.DataFrame(list(NOT_IN_OUR_COUNTRIES), columns=['Numbers'])

    NOT_IN_HOTS_COUNTRIES.to_csv(f'{PATH_TO_UPDATED_HOTOSM}/countries_not_in_hots.csv', index=False)
    NOT_IN_OUR_COUNTRIES.to_csv(f'{PATH_TO_UPDATED_HOTOSM}/countries_not_in_our_data.csv', index=False)
    COUNTRIES.to_csv(f'{PATH_TO_UPDATED_HOTOSM}/countries_in_both_data.csv', index=False)
















    


    
