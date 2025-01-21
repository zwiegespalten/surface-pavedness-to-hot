import re
import os
import logging
import json
import zipfile
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from hdx.utilities.easy_logging import setup_logging
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

WORKERS = os.cpu_count()*4
#WORKERS = os.cpu_count() 
PATTERN = r'^hotosm_(.*?)_roads_lines_*'
DIRECTORY = os.getcwd()
DIRECTORY = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files'
DIRECTORY_TO_DOWNLOAD = f'{DIRECTORY}/hotosm_files'
METADATA_DIR = f'{DIRECTORY}/hotosm_resources'
API_TOKEN = {'hdx_key':
 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJReWRKSG9pYm14QnZxcmQwSnl4T1F3MG9lWlIwajhxNjBsQUZ4VlRzM3ZjIiwiaWF0IjoxNzMwMzc3MTQ0LCJleHAiOjE3NjE0ODExNDR9.5vxy99m6RA9AkP7q0rlxO21tHgp5TvqsnjhWAlKscFA'}

if not os.path.exists(DIRECTORY_TO_DOWNLOAD):
    os.makedirs(DIRECTORY_TO_DOWNLOAD,exist_ok=True)

if not os.path.exists(METADATA_DIR):
    os.makedirs(METADATA_DIR,exist_ok=True)

def rename_file(current_file_name, new_file_name):
    try:
        os.rename(current_file_name, new_file_name)
    except FileNotFoundError:
        logging.warning(f"Error: The file '{current_file_name}' does not exist.")
    except FileExistsError:
        logging.warning(f"Error: A file named '{new_file_name}' already exists.")
    except Exception as e:
        logging.warning(f"An error occurred: {e}")

def unzip_file(zip_file_name, extract_to_directory):
    os.makedirs(extract_to_directory, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
            zip_ref.extractall(extract_to_directory)
    except FileNotFoundError:
        logging.warning(f"Error: The file '{zip_file_name}' does not exist.")
    except zipfile.BadZipFile:
        logging.warning(f"Error: The file '{zip_file_name}' is not a zip file.")
    except Exception as e:
        logging.warning(f"An error occurred: {e}")

def extract_first_wildcard(test_string, pattern):
    match = re.search(PATTERN, test_string)
    if match:
        first_part = match.group(1)
        return first_part
    else:
        return None

def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert to ISO 8601 string
    elif isinstance(obj, dict):
        return {key: serialize(value) for key, value in obj.items()}  # Recursively serialize dicts
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]  # Recursively serialize lists
    return obj  # Return the object as is if it's not a datetime

def write_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file,default=serialize, indent=4)

def download_resource(datasets, data):
    i = data['dataset']
    j = data['resources']
    resources = datasets[i].get_resources()[j]

    url, path = resources.download(DIRECTORY_TO_DOWNLOAD)
    print(f"Resource URL {url} downloaded to {path}")

    if os.path.exists(path):
        new_filepath = path.split('.')[0] + '.zip'
        rename_file(path, new_filepath)
        unzip_file(new_filepath, DIRECTORY_TO_DOWNLOAD)
        
setup_logging()
Configuration.create(hdx_site="prod", user_agent="HeiGIT", hdx_read_only=True)
# Configuration.create(hdx_site="stage", user_agent="MyOrg_MyProject")

country_data = {}  
datasets = Dataset.search_in_hdx("hotosm roads")
dataset_info = {}

for i, elm in enumerate(datasets):
    timestamp_dt = datetime.min 

    for j, res in enumerate(elm.get_resources()):

        timestamp = res.get('created')
        url = res.get('download_url')
        name = res.get('name')
        
        country_iso3 = extract_first_wildcard(name, PATTERN)
        country_info = {}

        if timestamp is not None:
            temp_timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')

            if temp_timestamp_dt >= timestamp_dt:
                if url is not None and name is not None and country_iso3 is not None:
                    timestamp_dt = temp_timestamp_dt
                    country_info['url'] = url
                    country_info['timestamp'] = timestamp_dt
                    country_info['dataset'] = i
                    country_info['resources'] = j
                    country_info['name'] = name

                    country_data[country_iso3] = country_info
                    dataset_info[country_iso3] = [res]

write_to_file(country_data, f'{METADATA_DIR}/country_data.json')
#write_to_file(dataset_info, f'{METADATA_DIR}/resources.json')

start_time = time.time()

with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = {executor.submit(download_resource, datasets, data): country for country, data in country_data.items()}

    for future in as_completed(futures):
        if future is not None:
            try:
                future.result() 
            except Exception as e:
                print(f'Error downloading data for {futures[future]}: {e}')

for file in os.listdir(DIRECTORY_TO_DOWNLOAD):
    if file.endswith('.txt') or file.endswith('.zip'):
        os.remove(os.path.join(DIRECTORY_TO_DOWNLOAD, file))

end_time = time.time()
n_countries = len(country_data)

# Output download time statistics
if n_countries > 0:
    print(f'It took {(end_time - start_time):.2f} seconds to download {n_countries} countries.')
    print(f'Average time for a country: {(end_time - start_time) / n_countries:.2f} seconds')
else:
    print('No countries were processed.')







