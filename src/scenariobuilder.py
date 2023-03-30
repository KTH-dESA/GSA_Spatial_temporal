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
from modelgenerator.temporal_calculations import *
from run.build_osemosysdatafiles import load_csvs, make_outputfile, functions_to_run, write_to_file

os.chdir(os.path.dirname(os.path.abspath(__file__)))

#required basefiles
#configuring the basefiles

config = ConfigParser()
config.read('config/config_input.ini')

crs = config['geospatialdata']['crs']

sample_file = config['sensitivityanalysis']['sample_file']
parameters_file = config['sensitivityanalysis']['parameters_file']
replicates =  int(config['sensitivityanalysis']['replicates'])
scenarios_folder = config['inputfiles']['scenarios_folder']
elec_shp = config['inputfiles']['elec_shp']
scenario_ouput = config['sensitivityanalysis']['scenario_ouput']
output_files_sample = config['sensitivityanalysis']['output_files_sample']
pathfinder_raster_country = config['inputfiles']['pathfinder_raster_country']
files = pd.read_csv(config['inputfiles']['gisfiles'], index_col=0)
substation = float(config['model_settings']['substation'])
#capital_cost_HV = float(config['model_settings']['capital_cost_HV'])
capacitytoactivity = float(config['model_settings']['capacitytoactivity'])
distr_losses = float(config['model_settings']['distr_losses'])
token = config['renewableninja']['token']
time_zone_offset = int(config['renewableninja']['time_zone_offset'])
Rpath = config['renewableninja']['Rpath']
seasonAprSept = int(config['model_settings']['seasonAprSept'])
seasonOctMarch = int(config['model_settings']['seasonOctMarch'])

urban_profile = config['inputfiles']['urban_profile']
text_file = config['inputfiles']['text_file']
country = config['inputfiles']['country']
output_folder = config['inputfiles']['output_folder']
year_array = ['2020', '2021', '2022','2023','2024','2025','2026','2027','2028','2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040',	'2041',	'2042',	'2043',	'2044',	'2045',	'2046',	'2047',	'2048',	'2049',	'2050',	'2051',	'2052',	'2053',	'2054',	'2055']

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
samplelist = expand(morris_sample, parameter_list, output_files_sample)

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
temporal_runs = {}
CapacityOfone_runs = {}

for j in dict_modelruns.keys():
    print("Running scenario %s" %j)

    # The parameters are defined and split
    modelrun = dict_modelruns[j]
    spatial = int(float(modelrun.iloc[0][1]))
    DemandElectrified_raw = modelrun.iloc[1][1]
    elecdemand_df = split_data_onecell(DemandElectrified_raw)
    DemandUnelectrified_raw = modelrun.iloc[2][1]
    unelecdemand_df = split_data_onecell(DemandUnelectrified_raw)
    CapacityOfOneTechnologyUnit = int(float(modelrun.iloc[5][1]))
    Dailytemporalresolution= int(float(modelrun.iloc[4][1]))
    DiscountRate = float(modelrun.iloc[3][1])
    CapitalCost_PV_raw = modelrun.iloc[6][1]
    CapitalCost_PV = split_data_onecell(CapitalCost_PV_raw)
    CapitalCost_batt_raw = modelrun.iloc[7][1]
    CapitalCost_batt = split_data_onecell(CapitalCost_batt_raw)
    CapitalCost_WI_raw = modelrun.iloc[8][1]
    CapitalCost_WI = split_data_onecell(CapitalCost_WI_raw)
    CapitalCost_powerplant_raw = modelrun.iloc[9][1]
    CapitalCost_powerplant = split_data_onecell(CapitalCost_powerplant_raw)
    CapitalCost_transm = float(modelrun.iloc[10][1])
    CapitalCost_distribution = float(modelrun.iloc[11][1])
    CapacityFactor_adj = round(float(modelrun.iloc[12][1]), 4)
    DemandProfileTier = int(float(modelrun.iloc[13][1]))
    FuelpriceNG_raw = modelrun.iloc[14][1]
    FuelpriceNG = split_data_onecell(FuelpriceNG_raw)
    FuelpriceDIESEL_raw = modelrun.iloc[15][1]
    FuelpriceDIESEL = split_data_onecell(FuelpriceDIESEL_raw)
    FuelpriceCOAL_raw = modelrun.iloc[16][1]
    FuelpriceCOAL = split_data_onecell(FuelpriceCOAL_raw)
    CapitalCost_distribution_ext = float(modelrun.iloc[17][1])

    tier_profile = 'input_data/T%i_load profile_Narayan.csv' %(DemandProfileTier)

    id = spatial
    combined = spatial + CapacityFactor_adj

    if combined not in scenario_runs.values():

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



        shapefile = '../Projected_files/' + point
        #Add the path to the RScript.exe under Program Files and add here

        srcpath = os.path.dirname(os.path.abspath(__file__))
        path = "temp/%i" %(spatial)

        if not os.path.exists('temp/%i' %(spatial)):
            os.makedirs('temp/%i' %(spatial))
        coordinates = project_vector(shapefile)
        wind, solar = csv_make(coordinates, path)
        down = download(path, Rpath, srcpath, wind, solar, token)
        adjust_timezone(path, time_zone_offset)
        uncertainty_capacityfactor(path, CapacityFactor_adj)

        print("5. Build transmission technologies")

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

        #Solar and wind csv files
        renewableninja(renewable_path, scenarios_folder, spatial, CapacityFactor_adj)
        #Location file
        gisfile_ref = GIS_file(scenarios_folder, '../Projected_files/' + point, spatial)
        matrix = 'run/scenarios/Demand/%i_adjacencymatrix.csv' %(spatial)

        capital_cost_transmission_distrib(elec_, noHV, HV_file, elec_noHV_cells, unelec, CapitalCost_transm, substation, capacitytoactivity, scenarios_folder, matrix, gisfile_ref, spatial, CapitalCost_distribution_ext, diesel = True)
        scenario_runs[j] = combined
        
    else:
        print('Scenario already run')

#Read scenarios from sample file

    id_demand = spatial + elecdemand_df.iloc[35][0]
    temporal_unique = float(modelrun.iloc[4][1])+id_demand+DemandProfileTier

    if temporal_unique  not in demand_runs.values():
        #Scenarios that are sensitive to spatial and demand simultaneously
        print("Running scenario %s" %j)

    #######################

        polygon = str(spatial) + "_polygon.shp"
        point = str(spatial) + "_point.shp"
        print("6. Build Demand for location %s" %(id))
        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)

        settlements = 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial)
        inputdata =  os.path.join(os.getcwd(), 'run/scenarios/input_data.csv')
        calculate_demand(settlements, elecdemand_df, unelecdemand_df,elecdemand_df.iloc[35][0], spatial, inputdata)

        print("7. Build peakdemand")

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)
        
        refpath = 'run/scenarios'
        demandcells = os.path.join(os.getcwd(), 'run/scenarios/Demand/%i_demand_cells.csv' %(spatial))
        distribution_length_cell_ref = 'run/scenarios/%i_distribution.csv' %(spatial)
        distribution = 'run/scenarios/%i_distributionlines.csv' %(spatial)
        distribution_row = "_%isum" %(spatial)

        noHV = 'run/%i_noHV_cells.csv' %(spatial)
        HV_file = 'run/%i_HV_cells.csv' %(spatial)
        minigrid = 'run/%i_elec_noHV_cells.csv' %(spatial)
        neartable = 'run/scenarios/Demand/%i_Near_table.csv' %(spatial)
        demand = 'run/scenarios/%i_demand_%i_spatialresolution.csv' %(elecdemand_df.iloc[35][0], spatial)
        
        temporal_id = float(modelrun.iloc[4][1])
        if temporal_unique not in temporal_runs.values():
            yearsplit = yearsplit_calculation(temporal_id,seasonAprSept , seasonOctMarch, 'run/scenarios/yearsplit_%f.csv' %(temporal_id), year_array)
            specifieddemand, timesteps = demandprofile_calculation(tier_profile, temporal_id, seasonAprSept, seasonOctMarch, 'run/scenarios/specifiedrural_demand_time%i_tier%i.csv' %(int(temporal_id), DemandProfileTier), year_array, 'Minute')
            specifieddemandurban, timesteps = demandprofile_calculation(urban_profile, temporal_id, seasonAprSept, seasonOctMarch, 'run/scenarios/specifieddemand_%i.csv' %(int(temporal_id)), year_array, 'hour')
            
            peakdemand_csv(demand, specifieddemand,capacitytoactivity, yearsplit, distr_losses, HV_file, distribution, distribution_row, distribution_length_cell_ref, scenarios_folder, spatial, elecdemand_df.iloc[35][0])
            addtimestep(timesteps,input_data, 'run/scenarios/input_data_%i.csv' %(int(temporal_id)))

            load_yearly = annualload(tier_profile, 'run/scenarios/annualload_tier%i.csv' %(DemandProfileTier))
            loadprofile_high = 'input_data/high_Jan.csv'
            capacityfactor_pv = 'run/scenarios/uncertain%f_spatial%i_capacityfactor_solar.csv' %(CapacityFactor_adj,spatial)
            tofilePV = 'run/scenarios/capacityfactor_solar_batteries_Tier%i_loca%i_uncertain%f.csv' %(DemandProfileTier, spatial, CapacityFactor_adj)
            tofilePVhigh = 'run/scenarios/capacityfactor_solar_batteries_urban_loca%i_uncertain%f.csv' %(spatial, CapacityFactor_adj)
            efficiency_discharge = 0.98 # Koko (2022)
            efficiency_charge = 0.95 # Koko (2022)
            pvcost = 2540 #ATB 2021 version for 2021 value
            batterycost_kWh = 522  #ATB 2021 version for 2021 value with adjusted Kenyan value
            locations = 'run/scenarios/%i_GIS_data.csv' %(spatial)
            scenario = temporal_id
            startDate = pd.to_datetime("2016-01")
            endDate = pd.to_datetime("2016-02")
            startDate_load = pd.to_datetime("1900-01")
            endDate_load = pd.to_datetime("1900-02")
            battery_to_pv(load_yearly,  capacityfactor_pv, efficiency_discharge, efficiency_charge, locations, pvcost, batterycost_kWh, tofilePV, scenario,  startDate, endDate, startDate_load, endDate_load)
            battery_to_pv(loadprofile_high,  capacityfactor_pv, efficiency_discharge, efficiency_charge, locations, pvcost, batterycost_kWh, tofilePVhigh, scenario,  startDate, endDate, startDate, endDate)
            temporal_runs[j] = temporal_unique

        demand_runs[j] = temporal_unique
            
    else:
        print('Scenario already run')

    ####################### Make txt file #############################
    dict_df = load_csvs(scenarios_folder) #args.data_path) #
    outPutFile = make_outputfile(text_file)#args.template) #

    outPutFile = functions_to_run(dict_df, outPutFile, spatial, elecdemand_df.iloc[35][0], DiscountRate, temporal_id,CapacityOfOneTechnologyUnit, CapitalCost_PV, 
                                  CapitalCost_batt, CapitalCost_WI, CapitalCost_powerplant, CapitalCost_distribution, CapacityFactor_adj, 
                                  FuelpriceNG, FuelpriceDIESEL, FuelpriceCOAL, DemandProfileTier)

    #write data file
    if not os.path.exists(output_folder):
        os.makedirs('run/output')

    #write to DD-file
    comb = country+j+'.txt'
    write_to_file('run/output/', outPutFile, comb)      #args.output_path