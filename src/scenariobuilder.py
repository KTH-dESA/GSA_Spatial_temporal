from datetime import datetime
import os
from configparser import ConfigParser
import pandas as pd
from modelgenerator.Build_csv_files import *
from modelgenerator.renewable_ninja_download import *
from modelgenerator.post_elec_GIS_functions import *
from modelgenerator.Pathfinder_processing_step import *
from create_sample import createsample
from expand_sample import expand
from create_modelrun import modify_parameters
from run.build_osemosysdatafiles import load_csvs
from modelgenerator.Distribution import *

os.chdir(os.path.dirname(os.path.abspath(__file__)))

#required basefiles
#configuring the basefiles

config = ConfigParser()
config.read('config/config_input.ini')

print(config['geospatialdata']['crs'])


sample_file = 'sensitivity/sample_morris.csv'
scenarios_folder = "run/scenarios"
parameters_file = 'config/parameters.csv'
replicates = 10
elec_shp = '../Projected_files/elec.shp'
scenario_ouput = "run/sensitivity_range/"
output_files = 'sensitivity/runs'
pathfinder_raster_country = os.path.join('temp/dijkstra','path_cleaned.tif')
files = pd.read_csv('input_data/Benin_GIS_files.csv', index_col=0)
crs = "EPSG:32631"
substation = 2.4 #kUSD/MW
capital_cost_HV = 3.3 #kUSD MW-km
capacitytoactivity = 31.536 #coversion MW to TJ
distr_losses = 0.83
token = '7a3a746a559cfe5638d6730b1af467beebaf7aa4'
time_zone_offset = 1  # Benin is UTC + 1hours to adjust for the time zone
capacitytoactivity = 31.536
distr_losses = 0.83
Rpath =  'C:\\TPFAPPS\\R\\R-4.1.0\\bin\\RScript.exe'
year_array = ['2020', '2021', '2022','2023','2024',	'2025',	'2026',	'2027',	'2028',	'2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040',	'2041',	'2042',	'2043',	'2044',	'2045',	'2046',	'2047',	'2048',	'2049',	'2050',	'2051',	'2052',	'2053',	'2054',	'2055']

def split_data_onecell(data):
    data_ = data.replace(']', '')
    data__ = data_.replace('[', '')
    data_clean = data__.split(',')
    data_float_clean = [float(i) for i in data_clean]
    data_clean_df = pd.DataFrame(data_float_clean, index = year_array)
    return data_clean_df

### Scenario settings ###
dict_df = load_csvs(scenarios_folder)

with open(parameters_file, 'r') as csv_file:
    reader = list(csv.DictReader(csv_file))
createsample(reader, sample_file, replicates)

with open(parameters_file, 'r') as csv_file:
    parameter_list = list(csv.DictReader(csv_file))
morris_sample = np.loadtxt(sample_file, delimiter=",")
samplelist = expand(morris_sample, parameter_list, output_files)

for sample in samplelist:
    with open(sample, 'r') as csv_file:
        sample_list = list(csv.DictReader(csv_file))
        stxt = sample.split('/')[-1]
        s = stxt.split('.')[0]
    modify_parameters(dict_df['input_data'], sample_list, s, scenario_ouput)

if not os.path.exists(scenarios_folder):
    os.makedirs(scenarios_folder)

dict_modelruns = load_csvs(scenario_ouput)

scenario_runs = {}
demand_runs = {}
for j in dict_modelruns.keys():
    print("Running scenario %s" %j)
    modelrun = dict_modelruns[j]
    spatial = int(float(modelrun.iloc[0][1]))
    #split mulityear parameter to dataframe
    DemandElectrified_raw = modelrun.iloc[1][1]
    elecdemand_df = split_data_onecell(DemandElectrified_raw)
   #split mulityear parameter to dataframe
    DemandUnelectrified_raw = modelrun.iloc[2][1]
    unelecdemand_df = split_data_onecell(DemandUnelectrified_raw)

    DiscountRate = float(modelrun.iloc[3][1])

    id = spatial

    if id not in scenario_runs.values():

        print('1. Aggregating the number of cells per polygon from Pathfinder')
        polygon = str(spatial) + "_polygon.shp"
        point = str(spatial) + "_point.shp"
        path_polygon = '../Projected_files/' + polygon

        zonalstat_pathfinder(pathfinder_raster_country, path_polygon, spatial)

        print("2. Create the demandcells.csv file and the classifications")
        
        shape =  '../Projected_files/' + polygon
        gdp =  '../Projected_files/' + files.loc['gdp','filename']
        demandcells = os.path.join(os.getcwd(), 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial))

        if not os.path.exists('run/scenarios/Demand'):
            os.makedirs('run/scenarios/Demand')

        join_elec(elec_shp, gdp, shape, spatial)
        elec(demandcells, spatial)

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)
        print("3. Identify unelectrified %i_polygons " %(spatial))
        polygons_all = '../Projected_files/' + polygon
        noHV = 'run/%i_noHV_cells.csv' %(spatial)
        shape =  "run/scenarios/Demand/%i_un_elec_polygons.shp" %(spatial)

        noHV_polygons(polygons_all, noHV, shape, crs)

        #To be able to download you need to install the package curl from R and also have R installed on your computer
        # Easiest is to write  install.packages("curl") in the R prompt

        print("4. Download Renewable Ninja files for scenario %i" %(id))
        # add your token for API from your own log in on Renewable Ninjas

        if not os.path.exists('temp/%i' %(spatial)):
            os.makedirs('temp/%i' %(spatial))

        shapefile = '../Projected_files/' + point
        #Add the path to the RScript.exe under Program Files and add here

        srcpath = os.path.dirname(os.path.abspath(__file__))
        #path = "temp/%i" %(spatial)
        #coordinates = project_vector(shapefile)
        #wind, solar = csv_make(coordinates, path)
        #down = download(path, Rpath, srcpath, wind, solar, token)
        #adjust_timezone(path, time_zone_offset)

        print("5. Build peakdemand, maxkmpercell, transmission technologies, capitalcostpercapacitykm")

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)

        demandcells = os.path.join(os.getcwd(), 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial))
        input_data =  os.path.join(os.getcwd(), 'run/scenarios/input_data.csv')
        distribution_length_cell_ref = network_length(demandcells, input_data, scenarios_folder, spatial)
        distribution = 'run/scenarios/%i_distributionlines.csv' %(spatial)
        distribution_row = "_%isum" %(spatial)

        topath = 'run/scenarios/Demand'
        noHV = 'run/%i_noHV_cells.csv' %(spatial)
        HV_file = 'run/%i_HV_cells.csv' %(spatial)
        minigrid = 'run/%i_elec_noHV_cells.csv' %(spatial)
        neartable = 'run/scenarios/Demand/%i_Near_table.csv' %(spatial)

        transmission_matrix(neartable, noHV, HV_file, minigrid, topath)

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)

        elec_noHV_cells = 'run/%i_elec_noHV_cells.csv' %(spatial)
        renewable_path = 'temp/%i' %(spatial)
        pop_shp = '../Projected_files/' + files.loc['pop_raster','filename']
        unelec = 'run/%i_un_elec.csv' %(spatial)
        elec_ = 'run/%i_elec.csv' %(spatial)
        Projected_files_path = '../Projected_files/'

        #Solar and wind csv files
        renewableninja(renewable_path, scenarios_folder, spatial)
        #Location file
        gisfile_ref = GIS_file(scenarios_folder, '../Projected_files/' + point, spatial)
        matrix = 'run/scenarios/Demand/%i_adjacencymatrix.csv' %(spatial)

        capital_cost_transmission_distrib(elec_, noHV, HV_file, elec_noHV_cells, unelec, capital_cost_HV, substation, capacitytoactivity, scenarios_folder, matrix, gisfile_ref, spatial, diesel = True)
        scenario_runs[j] = id
    else:
        print('Scenario already run')

#Read scenarios from sample file

    id_demand = spatial + elecdemand_df.iloc[35][0]

    if id_demand  not in demand_runs.values():
        #Scenarios that are sensitive to spatial and demand simultaneously
        print("Running scenario %s" %j)

        #TODO Modify the Pathfinder file to run zonal statistics sum on polygon.
        #TODO Add demand as scenario parameter
    #######################

        polygon = str(spatial) + "_polygon.shp"
        point = str(spatial) + "_point.shp"
        print("Build Demand")
        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)

        settlements = 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial)
        input_data =  os.path.join(os.getcwd(), 'run/scenarios/input_data.csv')
        calculate_demand(settlements, elecdemand_df, unelecdemand_df,elecdemand_df.iloc[35][0], spatial, input_data)

        print("Build peakdemand")

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)
        
        refpath = 'run/scenarios'
        demandcells = os.path.join(os.getcwd(), 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial))
        input_data =  os.path.join(os.getcwd(), 'run/scenarios/input_data.csv')
        distribution_length_cell_ref = 'run/scenarios/%i_distribution.csv' %(spatial)
        distribution = 'run/scenarios/%i_distributionlines.csv' %(spatial)
        distribution_row = "_%isum" %(spatial)

        topath = 'run/scenarios/Demand'
        noHV = 'run/%i_noHV_cells.csv' %(spatial)
        HV_file = 'run/%i_HV_cells.csv' %(spatial)
        minigrid = 'run/%i_elec_noHV_cells.csv' %(spatial)
        neartable = 'run/scenarios/Demand/%i_Near_table.csv' %(spatial)
        demand = 'run/scenarios/%i_demand_%i_spatialresolution.csv' %(elecdemand_df.iloc[35][0], spatial)
        specifieddemand= 'run/scenarios/demandprofile_rural.csv'

        yearsplit = 'run/scenarios/Demand/yearsplit.csv'
        reffolder = 'run/scenarios'

        peakdemand_csv(demand, specifieddemand,capacitytoactivity, yearsplit, distr_losses, HV_file, distribution, distribution_row, distribution_length_cell_ref, scenarios_folder, spatial, elecdemand_df.iloc[35][0])
        demand_runs[j] = id_demand
    else:
        print('Scenario already run')