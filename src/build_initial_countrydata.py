from datetime import datetime
import os
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
from configparser import ConfigParser
import numpy as np
import seaborn as sns
from modelgenerator.Download_files import *
from modelgenerator.Project_GIS import *
from settlement_build import *
from modelgenerator.elec_start import *
from modelgenerator.Build_csv_files import *
from modelgenerator.Pathfinder_processing_step import *
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser()
config.read('config/config_input.ini')

country = 'Kenya'
if country == 'Kenya':
    crs = config['geospatialdata']['Kenyacrs']

    files = pd.read_csv(config['inputfiles']['Kenyagisfiles'], index_col=0)
    GIS_URL_Kenya = config['geospatialdata']['KenyaURL']
    KenyaUnzip = config['geospatialdata']['KenyaUnzip']
    Kenyaprojectedfolder = config['geospatialdata']['Kenyaprojectedfolder']
    Kenyapop_shp =config['geospatialdata']['Kenyapopshp']
    Kenyaelec_shp = config['inputfiles']['Kenyaelec_shp']
    Kenyascenarios_folder = config['inputfiles']['Kenyascenarios_folder']
    projpath = config['dijkstra_Kenya']['proj_path']
    elec_actual = float(config['electrification_Kenya']['elec_actual'])
    pop_cutoff = int(config['electrification_Kenya']['pop_cutoff'])
    dist_mv = int(config['electrification_Kenya']['dist_mv'])
    dist_lv = int(config['electrification_Kenya']['dist_lv'])
    dist_to_trans = int(config['electrification_Kenya']['dist_to_trans'])
    dist_to_sub = int(config['electrification_Kenya']['dist_to_sub'])
    dist_minig = int(config['electrification_Kenya']['dist_minig'])
    min_night_lights = float(config['electrification_Kenya']['min_night_lights'])
    max_grid_dist = int(config['electrification_Kenya']['max_grid_dist'])
    max_road_dist = int(config['electrification_Kenya']['max_road_dist'])
    pop_cutoff2 = int(config['electrification_Kenya']['pop_cutoff2'])
    urban_elec_ratio =  float(config['electrification_Kenya']['urban_elec_ratio'])
    rural_elec_ratio = float(config['electrification_Kenya']['rural_elec_ratio'])
    pop_actual = int(config['electrification_Kenya']['pop_actual'])
    urban =float(config['electrification_Kenya']['urban'])
    urban_cutoff =  int(config['electrification_Kenya']['urban_cutoff'])
    start_year =  int(config['electrification_Kenya']['start_year'])
    settlement = gpd.read_file(config['electrification_Kenya']['settlement'])                           
else:
    crs = config['geospatialdata']['Benincrs']

    files = pd.read_csv(config['inputfiles']['Beningisfiles'], index_col=0)
    GIS_URL_Kenya = config['geospatialdata']['BeninURL']
    KenyaUnzip = config['geospatialdata']['BeninUnzip']
    Kenyaprojectedfolder = config['geospatialdata']['Beninprojectedfolder']
    Kenyapop_shp =config['geospatialdata']['Beninpopshp']
    Kenyaelec_shp = config['inputfiles']['Beninelec_shp']
    Kenyascenarios_folder = config['inputfiles']['Beninscenarios_folder']
    projpath = config['dijkstra_Benin']['proj_path']
    elec_actual = float(config['electrification_Benin']['elec_actual'])
    pop_cutoff = int(config['electrification_Benin']['pop_cutoff'])
    dist_mv = int(config['electrification_Benin']['dist_mv'])
    dist_lv = int(config['electrification_Benin']['dist_lv'])
    min_night_lights = float(config['electrification_Benin']['min_night_lights'])
    max_grid_dist = int(config['electrification_Benin']['max_grid_dist'])
    max_road_dist = int(config['electrification_Benin']['max_road_dist'])
    pop_cutoff2 = int(config['electrification_Benin']['pop_cutoff2'])
    urban_elec_ratio =  float(config['electrification_Benin']['urban_elec_ratio'])
    rural_elec_ratio = float(config['electrification_Benin']['rural_elec_ratio'])
    pop_actual = int(config['electrification_Benin']['pop_actual'])
    urban =float(config['electrification_Benin']['urban'])
    urban_cutoff =  int(config['electrification_Benin']['urban_cutoff'])
    start_year =  int(config['electrification_Benin']['start_year'])
    settlement = gpd.read_file(config['electrification_Benin']['settlement'])

# 1. The files in "input_data/GIS_data" are downloaded and placed in a "temp" folder.
date = datetime. now(). strftime("%Y_%m_%d-%I:%M:%S_%p")
print(date)
print("1. The files in input_data/GIS_data are downloaded and placed in a temp folder.")
URL_viirs = 'https://eogdata.mines.edu/nighttime_light/annual/v20/2020/VNL_v2_npp_2020_global_vcmslcfg_c202102150000.average_masked.tif.gz'

download_url_data(GIS_URL_Kenya, 'temp')
download_viirs(URL_viirs, 'temp')
unzip_all(KenyaUnzip, '../temp', '../GIS_data')

# 2. The files are then projected and clipped to the administrative boundaries.
date = datetime. now(). strftime("%Y_%m_%d-%I:%M:%S_%p")
print(date)
print("2. The files are then projected and clipped to the administrative boundaries.")

project_main('../GIS_Data', Kenyaprojectedfolder, files, crs)
# 3. Through QGIS make raster to point layer and save (MANUAL STEP)
date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("3. Through QGIS make raster to point layer and save (download from zenodo)")
#Make sure you are in the /src directory when you start this script
print(os.getcwd())
download_url_data("input_data/zenodo.txt", "Projected_files")

# 4. The GIS layers are prepared to for a heuristic approximation for electrified settlements
date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("4. The GIS layers are prepared to for a heuristic approximation for electrified settlements")
#Make sure you are in the /src directory when you start this script
print(os.getcwd())

rasterize = raster_proximity(Kenyaprojectedfolder, files, country)
points = raster_to_point(rasterize, Kenyapop_shp, Kenyaprojectedfolder, crs, country)

# 5. Approximate location of urban settlements and the electrified settlements 1kmx1km resolution
date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("5. Approximate location of urban settlements and the electrified settlements 1kmx1km resolution")

settlements = pd.DataFrame(settlement, copy=True)
urbansettlements = calibrate_pop_and_urban(settlements, pop_actual, urban, urban_cutoff, country)
elec_current_and_future(urbansettlements, elec_actual, pop_cutoff, min_night_lights,
                            max_grid_dist, urban_elec_ratio, rural_elec_ratio, max_road_dist, pop_actual, pop_cutoff2, start_year, dist_mv, dist_lv, crs, country, dist_to_trans, dist_to_sub, dist_minig, Kenyaprojectedfolder)


date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)

point = gpd.read_file(Kenyaelec_shp)
os.chdir(os.path.dirname(os.path.abspath(__file__)))

#
fig, ax = plt.subplots(1, 1)
point.plot(column='elec', ax=ax)
fig.suptitle('Estimated electrified popluation (in yellow) %s' %(country), fontsize=18)

plt.savefig(country +'_run/elec.png')

date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("1. Calculating the Pathfinder distribution lines to unelectrified cells")

tiffile = Kenyaprojectedfolder +'/' + files.loc['pop','filename']

pathfinder_main(Kenyaprojectedfolder,projpath, Kenyaelec_shp, Kenyascenarios_folder, tiffile, crs, country)