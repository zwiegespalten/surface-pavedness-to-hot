import pandas as pd
import os
import numpy as np
import pycountry

country_stats_filepath = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files/country_stats.csv'
predictions_filepath = '/mnt/sds-hd/sd17f001/eren/mapillary_final/predictions_osm/merged_predicted_osm.parquet'
output_filepath = '/mnt/sds-hd/sd17f001/eren/mapillary_final/hotosm_files/comparison'
data_filepath = '/mnt/sds-hd/sd17f001/eren/mapillary_final/final_partitioned_filtered'

def alpha2_to_alpha3(alpha2_code):
    try:
        country = pycountry.countries.get(alpha_2=alpha2_code)
        return country.alpha_3 if country else None
    except KeyError:
        return None

print(alpha2_to_alpha3('TJ'))

country_stats = pd.read_csv(country_stats_filepath)
predictions = pd.read_parquet(predictions_filepath, columns=['country'])

continents = [os.path.join(data_filepath, f) for f in os.listdir(data_filepath) if os.path.isdir(os.path.join(data_filepath, f))]
countries_alpha_2 = [
    f.split('=')[-1] 
    for k in continents 
    for f in os.listdir(k) 
    if os.path.isdir(os.path.join(k, f))
]
countries_alpha_3 = []
countries_wo_alpha_3 = []
for x in countries_alpha_2:
    alpha3 = alpha2_to_alpha3(x)
    if alpha3:
        countries_alpha_3.append(alpha3)
    else:
        countries_wo_alpha_3.append(x)

countries_wo_alpha_3 = [x.lower() for x in countries_wo_alpha_3]

countries_predictions = []
#for elm in predictions['country_iso_a3'].to_list():
    #countries_predictions.extend(elm)

countries_predictions = countries_alpha_3
countries_predictions = [x.lower() for x in countries_predictions]
countries_predictions = set(countries_predictions)

countries_wo_info = country_stats[country_stats['total_road_length'] == 0.0]
countries_not_in_hotosm = country_stats[~country_stats['country'].apply(lambda x: x.lower()).isin(countries_predictions)]['country'].unique()
countries_in_hotosm_wo_info = countries_wo_info[countries_wo_info['country'].apply(lambda x: x.lower()).isin(countries_predictions)]['country'].unique()
countries_wo_info = country_stats[country_stats['total_road_length'] == 0.0]['country'].unique()

if not os.path.exists(output_filepath):
    os.makedirs(output_filepath, exist_ok=True)
for file, filepath in zip(
    [countries_not_in_hotosm, countries_in_hotosm_wo_info, countries_wo_info, countries_wo_alpha_3], 
    [os.path.join(output_filepath, 'countries_not_in_hotosm.csv'),
     os.path.join(output_filepath, 'countries_in_hotosm_wo_info.csv'),
     os.path.join(output_filepath, 'countries_wo_info.csv'),
     os.path.join(output_filepath, 'countries_without_alpha3.csv')]):
    
    pd.DataFrame({'country':file}).to_csv(filepath, index=False)

