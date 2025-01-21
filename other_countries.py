import duckdb 
import pandas as pd
import geopandas as gpd
from shapely import from_wkt
import os 
import logging
import shutil

ORIGINAL_OSM_DIR = '/mnt/sds-hd/sd17f001/ohsome/ohsome-parquet/103831'
PREDICTIONS_DIR = '/mnt/sds-hd/sd17f001/eren/mapillary_final/predictions_osm'
COUNTRIES =  {'TJ':'TJK', 'XK':'XKK'}
COLS_FROM_ORG = ['osm_id', 'osm_type', 'name' , 'smoothness', 
                 'width', 'lanes', 'oneway', 'bridge', 'layer', 'source'
                 ]
OUTPUT_DIR = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files'
TEMP_DIR = os.path.join(OUTPUT_DIR, 'temp')

if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR, exist_ok=True)

def find_rows(ids, COLS_FROM_ORG, TEMP_DIR, osm_filepath, index):
    temp_file = os.path.join(TEMP_DIR, f'temp_{index}.db')
    result = None

    try:
        conn = duckdb.connect(temp_file)
        query = f"""
            SELECT 
                osm_id, osm_type, 
                list_extract(map_extract(tags, 'name'),1) as name, 
                list_extract(map_extract(tags, 'smoothness'),1) as smoothness, 
                list_extract(map_extract(tags, 'width'),1) as width, 
                list_extract(map_extract(tags, 'lanes'),1) as lanes, 
                list_extract(map_extract(tags, 'oneway'),1) as oneway, 
                list_extract(map_extract(tags, 'bridge'),1) as bridge, 
                list_extract(map_extract(tags, 'layer'),1) as layer, 
                list_extract(map_extract(tags, 'source'),1) as source
            FROM read_parquet('{osm_filepath}/*.parquet')
            WHERE osm_id in (SELECT osm_id from ids)
        """
        logging.warning(query)
        result = conn.sql(query).df()
        logging.warning(result)
    except Exception as err:
        logging.warning(err)
    finally:
        try:
            conn.close()
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception as err:
            logging.warning(err)

    if result is not None and not result.empty:
        return result 

    return pd.DataFrame(columns=COLS_FROM_ORG)    

def process_country(country, PREDICTIONS_DIR, TEMP_DIR, index):
    temp_file = os.path.join(TEMP_DIR, f'temp_country_{index}.db')
    predictions = None

    def strip_char(string):
        return string.split('/')[-1]

    query = f"""
    SELECT
        continent,
        country as country_iso_a2,
        country_iso_a3, 
        urban, urban_area, 
        osm_id as osm_id_original, 
        strip_char(osm_id) as osm_id, 
        osm_tags_highway, osm_tags_surface, osm_surface_class, osm_surface,
        pred_class, pred_label,
        combined_surface_osm_priority, combined_surface_DL_priority,
        changeset_timestamp as osm_changeset_timestamp, 
        mean_timestamp as DL_mean_timestamp,
        length as osm_length, 
        predicted_length,
        n_of_predictions_used,
        geometry
    FROM read_parquet('{PREDICTIONS_DIR}/predictions_updated_*.parquet')
    WHERE country = '{country}'
    """
    
    try:
        conn = duckdb.connect(temp_file)
        conn.create_function("strip_char", strip_char, ['varchar'], 'int') 
        predictions = conn.sql(query).df()
    except Exception as err:
        logging.warning(err)
    finally:
        try:
            conn.close()
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception as err:
            logging.warning(err)

    if predictions is not None and not predictions.empty:
        return predictions
    return 

def merge(COUNTRIES, COLS_FROM_ORG, PREDICTIONS_DIR, ORIGINAL_OSM_DIR, TEMP_DIR, OUTPUT_DIR):

    output_dir = os.path.join(OUTPUT_DIR, 'new_countries')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    for index, country in enumerate(COUNTRIES.keys()):
        predictions = process_country(country, PREDICTIONS_DIR, TEMP_DIR, index)

        if predictions is not None and not predictions.empty:
            ids = predictions['osm_id'].to_frame()
            ids['osm_id'] = ids['osm_id'].apply(lambda x: 'way/' + str(x))
            osm_data = find_rows(ids, COLS_FROM_ORG, TEMP_DIR, ORIGINAL_OSM_DIR, index)
            logging.warning(osm_data)
            if osm_data is not None and not osm_data.empty:

                query = f"""
                WITH        
                original_data AS(
                    SELECT *
                    FROM osm_data
                ),
                data AS(
                    SELECT *
                    FROM predictions
                )
                SELECT
                    b.continent, b.country_iso_a2,
                    b.country_iso_a3,
                    b.urban, b.urban_area, b.osm_id, a.osm_type,
                    b.osm_tags_highway as highway,
                    b.osm_tags_surface AS surface,
                    a.smoothness,
                    b.osm_surface_class,
                    b.pred_class, b.pred_label,
                    b.combined_surface_osm_priority,
                    b.combined_surface_DL_priority,
                    b.osm_changeset_timestamp, 
                    b.DL_mean_timestamp,
                    b.osm_length, 
                    b.predicted_length,
                    b.n_of_predictions_used,
                    a.name, a.width, a.lanes, a.oneway, a.bridge, a.layer, a.source,
                    --ST_AsText(ST_GeomFromWKB(b.geometry)) as geometry
                    b.geometry
                FROM data b
                LEFT JOIN original_data a
                    ON b.osm_id_original = a.osm_id
                """      
                
                temp_file = os.path.join(TEMP_DIR, f'temp_country_{index}.db')
                
                try:       
                    conn = duckdb.connect(temp_file)
                    conn.sql('LOAD SPATIAL;')
                    result = conn.sql(query).df()
                    result['geometry'] = result['geometry'].apply(lambda geom: from_wkt(geom))
                    result = gpd.GeoDataFrame(result, geometry='geometry', crs="EPSG:4326")

                    form = f'heigit_{COUNTRIES[country].lower()}_roadsurface_lines'
                    
                    for filepath, driver in zip([os.path.join(output_dir, f'{form}.geojson'), os.path.join(output_dir, f'{form}.gpkg')], ['GeoJSON', 'GPKG']):
                        result.to_file(filepath, driver=driver, index=False)

                except Exception as err:
                    logging.warning(err)
                finally:
                    try:
                        conn.close()
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception as err:
                        logging.warning(err)

    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)

merge(COUNTRIES, COLS_FROM_ORG, PREDICTIONS_DIR, ORIGINAL_OSM_DIR, TEMP_DIR, OUTPUT_DIR)



                
                   





        



    

    






