from datetime import datetime
import os
import pandas as pd
import json
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

files = pd.read_csv('input_data/Benin_GIS_files.csv', index_col=0)
crs = "EPSG:32631"

### Scenario settings ###
dict_df = load_csvs("run/scenarios") 
sample_file = 'sensitivity/sample_morris.csv'
parameters_file = 'config/parameters.csv'
replicates = 10
with open(parameters_file, 'r') as csv_file:
    reader = list(csv.DictReader(csv_file))
createsample(reader, sample_file, replicates)

output_files = 'sensitivity/runs'
with open(parameters_file, 'r') as csv_file:
    parameter_list = list(csv.DictReader(csv_file))
morris_sample = np.loadtxt(sample_file, delimiter=",")
samplelist = expand(morris_sample, parameter_list, output_files)

scenario_ouput = "run/sensitivity_range/"
for sample in samplelist:
    with open(sample, 'r') as csv_file:
        sample_list = list(csv.DictReader(csv_file))
        stxt = sample.split('/')[-1]
        s = stxt.split('.')[0]
    modify_parameters(dict_df['input_data'], sample_list, s, scenario_ouput)


if not os.path.exists('run/scenarios'):
    os.makedirs('run/scenarios')

dict_modelruns = load_csvs("run/sensitivity_range")

scenario_runs = {}
demand_runs = {}
for j in dict_modelruns.keys():
    print("Running scenario %s" %j)
    modelrun = dict_modelruns[j]
    spatial = int(float(modelrun.iloc[0][1]))
    #Cleaning bad data
    DemandElectrified_raw = modelrun.iloc[1][1]
    DemandElectrified_ = DemandElectrified_raw.replace(']', '')
    DemandElectrified__ = DemandElectrified_.replace('[', '')
    DemandElectr = DemandElectrified__.split(',')
    DemandElectrified = [float(i) for i in DemandElectr]
    elecdemand_df = pd.DataFrame(DemandElectrified, index = ['2020', '2021', '2022',	'2023',	'2024',	'2025',	'2026',	'2027',	'2028',	'2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040',	'2041',	'2042',	'2043',	'2044',	'2045',	'2046',	'2047',	'2048',	'2049',	'2050',	'2051',	'2052',	'2053',	'2054',	'2055'])
    #Cleaning bad data
    DemandUnelectrified_raw = modelrun.iloc[2][1]
    DemandUnelectrified_ = DemandUnelectrified_raw.replace(']', '')
    DemandUnelectrified__ = DemandUnelectrified_.replace('[', '')
    DemandUnelec = DemandUnelectrified__.split(',')
    DemandUnelectrified = [float(i) for i in DemandUnelec]
    unelecdemand_df = pd.DataFrame(DemandUnelectrified, index = ['2020', '2021', '2022',	'2023',	'2024',	'2025',	'2026',	'2027',	'2028',	'2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040',	'2041',	'2042',	'2043',	'2044',	'2045',	'2046',	'2047',	'2048',	'2049',	'2050',	'2051',	'2052',	'2053',	'2054',	'2055'])


    DiscountRate = float(modelrun.iloc[3][1])

    id = spatial

    if id != scenario_runs.values():

        polygon = str(spatial) + "_polygon.shp"
        point = str(spatial) + "_point.shp"

        print('1. Aggregating the number of cells per polygon from Pathfinder')
        path_polygon = '../Projected_files/' + polygon
        pathfinder_raster_country = os.path.join('temp/dijkstra','path_cleaned.tif')
        zonalstat_pathfinder(pathfinder_raster_country, path_polygon, spatial)

        #Make sure you are in the /src directory when you start this script
        print(os.getcwd())
        print("2. Create the demandcells.csv file and the classifications")
        
        shape =  '../Projected_files/' + polygon
        gdp =  '../Projected_files/' + files.loc['gdp','filename']
        elec_shp = '../Projected_files/elec.shp'
        demandcells = os.path.join(os.getcwd(), 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial))

        if not os.path.exists('run/scenarios/Demand'):
            os.makedirs('run/scenarios/Demand')

        join_elec(elec_shp, gdp, shape, spatial)
        elec(demandcells, spatial)

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)
        #Identify unelectrified polygons
        polygons_all = '../Projected_files/' + polygon
        noHV = 'run/%i_noHV_cells.csv' %(spatial)
        shape =  "run/scenarios/Demand/%i_un_elec_polygons.shp" %(spatial)

        noHV_polygons(polygons_all, noHV, shape, crs)

        #To be able to download you need to install the package curl from R and also have R installed on your computer
        # Easiest is to write  install.packages("curl") in the R prompt

        print("Download Renewable Ninja files for scenario %i" %(spatial))
        # add your token for API from your own log in on Renewable Ninjas
        token = '7a3a746a559cfe5638d6730b1af467beebaf7aa4'
        time_zone_offset = 1  # Benin is UTC + 1hours to adjust for the time zone
        if not os.path.exists('temp/%i' %(spatial)):
            os.makedirs('temp/%i' %(spatial))

        shapefile = '../Projected_files/' + point
        #Add the path to the RScript.exe under Program Files and add here
        Rpath =  'C:\\TPFAPPS\\R\\R-4.1.0\\bin\\RScript.exe'
        srcpath = os.getcwd()
        print(srcpath)
        #path = "temp/%i" %(spatial)
        #coordinates = project_vector(shapefile)
        #wind, solar = csv_make(coordinates, path)
        #down = download(path, Rpath, srcpath, wind, solar, token)
        #adjust_timezone(path, time_zone_offset)

        print("Build peakdemand, maxkmpercell, transmission technologies, capitalcostpercapacitykm")

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)
        
        refpath = 'run/scenarios'

        demandcells = os.path.join(os.getcwd(), 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial))
        input_data =  os.path.join(os.getcwd(), 'run/scenarios/input_data.csv')
        distribution_length_cell_ref = network_length(demandcells, input_data, refpath, spatial)
        distribution = 'run/scenarios/%i_distributionlines.csv' %(spatial)
        distribution_row = "_%isum" %(spatial)

        topath = 'run/scenarios/Demand'
        noHV = 'run/%i_noHV_cells.csv' %(spatial)
        HV = 'run/%i_HV_cells.csv' %(spatial)
        minigrid = 'run/%i_elec_noHV_cells.csv' %(spatial)
        neartable = 'run/scenarios/Demand/%i_Near_table.csv' %(spatial)
        yearsplit = 'run/scenarios/Demand/yearsplit.csv'
        reffolder = 'run/scenarios'
        distr_losses = 0.83

        transmission_matrix(neartable, noHV, HV, minigrid, topath)

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)

        elec_noHV_cells = 'run/%i_elec_noHV_cells.csv' %(spatial)
        renewable_path = 'temp/%i' %(spatial)
        pop_shp = '../Projected_files/' + files.loc['pop_raster','filename']
        unelec = 'run/%i_un_elec.csv' %(spatial)
        noHV = 'run/%i_noHV_cells.csv' %(spatial)
        HV = 'run/%i_HV_cells.csv' %(spatial)
        elec_ = 'run/%i_elec.csv' %(spatial)
        Projected_files_path = '../Projected_files/'

        scenariopath = 'run/scenarios'

        substation = 2.4 #kUSD/MW
        capital_cost_HV = 3.3 #kUSD MW-km
        capacitytoactivity = 31.536 #coversion MW to TJ

        #Solar and wind csv files
        renewableninja(renewable_path, scenariopath, spatial)
        #Location file
        gisfile_ref = GIS_file(scenariopath, '../Projected_files/' + point, spatial)
        matrix = 'run/scenarios/Demand/%i_adjacencymatrix.csv' %(spatial)

        capital_cost_transmission_distrib(elec_, noHV, HV, elec_noHV_cells, unelec, capital_cost_HV, substation, capacitytoactivity, scenariopath, matrix, gisfile_ref, spatial, diesel = True)
        scenario_runs[j] = id
    else:
        print('Scenario already run')

#demand_scenario_nan = scenario[1]
#demand_scenario_list = [x for x in demand_scenario_nan if str(x) != 'nan']
#Read scenarios from sample file

    id_demand = spatial + DemandElectrified[35]

    if id_demand != demand_runs.values():
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
        demand = 'input_data/Benin_demand.csv'
        input_data =  os.path.join(os.getcwd(), 'run/scenarios/input_data.csv')
        calculate_demand(settlements, demand, elecdemand_df, unelecdemand_df, spatial, input_data)

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
        HV = 'run/%i_HV_cells.csv' %(spatial)
        minigrid = 'run/%i_elec_noHV_cells.csv' %(spatial)
        neartable = 'run/scenarios/Demand/%i_Near_table.csv' %(spatial)
        demand = 'run/scenarios/%i_demand_%i_spatialresolution.csv' %(demand_scenario, spatial)
        specifieddemand= 'run/scenarios/demandprofile_rural.csv'
        capacitytoactivity = 31.536
        yearsplit = 'run/scenarios/Demand/yearsplit.csv'
        reffolder = 'run/scenarios'
        distr_losses = 0.83

        peakdemand_csv(demand, specifieddemand,capacitytoactivity, yearsplit, distr_losses, HV, distribution, distribution_row, distribution_length_cell_ref, reffolder, spatial, demand_scenario)
        demand_runs[j] = id_demand
