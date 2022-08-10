from datetime import datetime
import os
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import numpy as np
import seaborn as sns
from Download_files import *
from Project_GIS import *
from settlement_build import *
from elec_start import *
from Build_csv_files import *
from Pathfinder_processing_steps import *

print(os.getcwd())
os.chdir(os.path.dirname(os.path.abspath(__file__)))
files = pd.read_csv('input_data/Benin_GIS_files.csv', index_col=0)
import os


crs = "EPSG:32631"

# 1. The files in "input_data/GIS_data" are downloaded and placed in a "temp" folder.
date = datetime. now(). strftime("%Y_%m_%d-%I:%M:%S_%p")
print(date)
print("1. The files in '"input_data/GIS_data are"' downloaded and placed in a temp folder.")
URL_viirs = 'https://eogdata.mines.edu/nighttime_light/annual/v20/2020/VNL_v2_npp_2020_global_vcmslcfg_c202102150000.average_masked.tif.gz'

#download_url_data('input_data/GIS_URL.txt', 'temp')
#download_viirs(URL_viirs, 'temp')
#unzip_all('input_data/GIS_unzip.txt', '../temp', '../GIS_data')

# 2. The files are then projected and clipped to the administrative boundaries.
date = datetime. now(). strftime("%Y_%m_%d-%I:%M:%S_%p")
print(date)
print("2. The files are then projected and clipped to the administrative boundaries.")

#project_main('../GIS_Data', '../Projected_files', files, crs)
# 3. Through QGIS make raster to point layer and save (MANUAL STEP)
date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("3. Through QGIS make raster to point layer and save (download from zenodo)")
#Make sure you are in the /src directory when you start this script
print(os.getcwd())
#download_url_data("input_data/zenodo.txt", "Projected_files")

# 4. The GIS layers are prepared to for a heuristic approximation for electrified settlements
date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("4. The GIS layers are prepared to for a heuristic approximation for electrified settlements")
#Make sure you are in the /src directory when you start this script
print(os.getcwd())

pop_shp = '../Projected_files/Benin/raster_to_point_Benin_UTM31N.shp'
Projected_files_path = '../Projected_files'

#rasterize = raster_proximity(Projected_files_path, files)
#points = raster_to_point(rasterize, pop_shp, Projected_files_path, crs)

# 5. Approximate location of urban settlements and the electrified settlements 1kmx1km resolution
date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("5. Approximate location of urban settlements and the electrified settlements 1kmx1km resolution")

elec_actual = 0.414  # percent #https://data.worldbank.org/indicator/EG.ELC.ACCS.ZS accessed 2022-08-09
pop_cutoff = 400  # people
dist_mv = 1000 #meters
dist_lv = 1000 #meters
min_night_lights = 0.5
max_grid_dist = 5000  # meters
max_road_dist = 500  # meters
pop_cutoff2 = 3000 # people
urban_elec_ratio = 0.663  # percent https://data.worldbank.org/indicator/EG.ELC.ACCS.UR.ZS 2022-08-09
rural_elec_ratio = 0.182  # percent https://data.worldbank.org/indicator/EG.ELC.ACCS.RU.ZS accessed 2022-02-02
pop_actual = 12046162  # people
urban = 0.48  # percent https://data.worldbank.org/indicator/SP.URB.TOTL.IN.ZS 2021-02-02
urban_cutoff = 20000
start_year = 2019
settlement = gpd.read_file("../Projected_files/settlements.shp")

settlements = pd.DataFrame(settlement, copy=True)
urbansettlements = calibrate_pop_and_urban(settlements, pop_actual, urban, urban_cutoff)
elec_current_and_future(urbansettlements, elec_actual, pop_cutoff, min_night_lights,
                            max_grid_dist, urban_elec_ratio, rural_elec_ratio, max_road_dist, pop_actual, pop_cutoff2, start_year, dist_mv, dist_lv, crs)



date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
shp_path = (r'..\Projected_files\elec.shp')
point = gpd.read_file(shp_path)
os.chdir('../src')

#
fig, ax = plt.subplots(1, 1)
point.plot(column='elec', ax=ax)
fig.suptitle('Estimated electrified popluation (in yellow) Benin', fontsize=18)

plt.savefig('run/elec.png')

date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
print(date)
print("6. Calculating the Pathfinder distribution lines to unelectrified cells")
path = '../Projected_files/'
proj_path = 'temp/temp'
elec_shp = '../Projected_files/elec.shp'
tofolder = 'run/ref'
tiffile = '../Projected_files/' + files.loc['pop_raster','filename']

pathfinder_main(path,proj_path, elec_shp, tofolder, tiffile, crs)