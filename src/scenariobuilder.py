from datetime import datetime
import os
import pandas as pd

from Build_csv_files import *
from renewable_ninja_download import *
from post_elec_GIS_functions import *
os.chdir(os.path.dirname(os.path.abspath(__file__)))

files = pd.read_csv('input_data/Benin_GIS_files.csv', index_col=0)
crs = "EPSG:32631"

### Scenario settings ###

#TODO Integrate the scenario generator with SNAKEMAKE file. To understand is if I send a number or if I send a file.
scenario = pd.read_csv('modelruns/scenarios/sample_morris.csv', header=None)

#Read scenarios from sample file
for j in range(0,len(scenario.index)):
    print("Running scenario %i" %j)
    spatial = int(scenario[0][j])
    demand = int(scenario[1][j])
    discountrate = int(scenario[2][j])

#TODO Modify the Pathfinder file to run zonal statistics sum on polygon.
#TODO Add demand as scenario parameter
#TODO 

    polygon = str(spatial) + "_polygon.shp"
    point = str(spatial) + "_point.shp"

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)
    #Identify unelectrified polygons
    polygons_all = '../Projected_files/' + polygon

    #TODO check when these are defined
    noHV = 'run/noHV_cells.csv'
    shape =  "run/Demand/un_elec_polygons.shp"

    noHV_polygons(polygons_all, noHV, shape)

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)

    #To be able to download you need to install the package curl from R and also have R installed on your computer
    # Easiest is to write  install.packages("curl") in the R prompt

    # add your token for API from your own log in on Renewable Ninjas
    token = ''
    time_zone_offset = 1  # Benin is UTC + 1hours to adjust for the time zone

    shapefile = '../Projected_files/' + point
    #Add the path to the RScript.exe under Program Files and add here
    Rpath =  'C:\\TPFAPPS\\R\\R-4.1.0\\bin\\RScript.exe'
    srcpath = os.getcwd()
    print(srcpath)
    path = "temp"
    coordinates = project_vector(shapefile)
    wind, solar = csv_make(coordinates)
    down = download(path, Rpath, srcpath, wind, solar, token)
    adjust_timezone(path, time_zone_offset)

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)

    #Make sure you are in the /src directory when you start this script
    print(os.getcwd())
    shape =  '../Projected_files/' + files.loc['polygon','filename']
    gdp =  '../Projected_files/' + files.loc['gdp','filename']
    elec_shp = '../Projected_files/elec.shp'
    demandcells = os.path.join(os.getcwd(), 'run/Demand/demand_cells.csv')

    join(elec_shp, gdp, shape)
    elec(demandcells)

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)

    settlements = 'run/Demand/demand_cells.csv'
    demand = 'input_data/demand.csv'
    calculate_demand(settlements, demand)

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)

    #Identify unelectrified polygons

    polygons_all = '../Projected_files/Final_Polygons_Kenya.shp'
    noHV = 'run/noHV_cells.csv'
    shape =  "run/Demand/un_elec_polygons.shp"

    noHV_polygons(polygons_all, noHV, shape)

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)

    renewable_path = 'temp'
    pop_shp = 'new_40x40points_WGSUMT37S.shp'
    unelec = 'run/un_elec.csv'
    noHV = 'run/noHV_cells.csv'
    HV = 'run/HV_cells.csv'
    elec = 'run/elec.csv'
    Projected_files_path = '../Projected_files/'
    distribution_network = 'run/Demand/distributionlines.csv'
    distribution_length_cell = 'run/Demand/Distribution_network.xlsx'

    capital_cost_HV = 2.5 #kUSD MW-km
    substation = 15 #kUSD/MW
    capital_cost_LV = 4 #kUSD/MW
    capital_cost_LV_strengthening = 1 #kUSD/MW Assumed 25% of the cost
    capacitytoactivity = 31.536 #coversion MW to TJ
    distribution_cost = '0'
    path = 'run/ref'
    neartable = 'run/Demand/Near_table.csv'

    #Solar and wind csv files
    renewableninja(renewable_path, path)
    #Location file
    GIS_file(path, files)
    matrix = adjacency_matrix(neartable, noHV, HV, path)

    capital_cost_transmission_distrib(capital_cost_LV_strengthening, distribution_network, elec, noHV, HV, unelec, capital_cost_HV, substation, capital_cost_LV, capacitytoactivity, distribution_cost, path, distribution_length_cell, matrix)

