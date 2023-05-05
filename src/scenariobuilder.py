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
from Benin_run.build_osemosysdatafiles import load_csvs
from modelgenerator.Distribution import *
from modelgenerator.temporal_calculations import *
from Benin_run.build_osemosysdatafiles import load_csvs, make_outputfile, functions_to_run, write_to_file

os.chdir(os.path.dirname(os.path.abspath(__file__)))

#required basefiles
#configuring the basefiles

config = ConfigParser()
config.read('config/config_input.ini')

#Choose which country tobuild
country = 'Kenya'

if country == 'Benin':
    # Benin
    crs = config['geospatialdata']['Benincrs']
    sample_file = config['sensitivityanalysis']['Beninsample_file']
    parameters_file = config['sensitivityanalysis']['Beninparameters_file']
    replicates =  int(config['sensitivityanalysis']['replicates'])
    scenarios_folder = config['inputfiles']['Beninscenarios_folder']
    elec_shp = config['inputfiles']['Beninelec_shp']
    scenario_ouput = config['sensitivityanalysis']['Beninscenario_ouput']
    output_files_sample = config['sensitivityanalysis']['Beninoutput_files_sample']
    pathfinder_raster_country = config['inputfiles']['Beinpathfinder_raster_country']
    files = pd.read_csv(config['inputfiles']['Beningisfiles'], index_col=0)
    substation = float(config['model_settings']['substation'])
    capacitytoactivity = float(config['model_settings']['capacitytoactivity'])
    distr_losses = float(config['model_settings']['distr_losses'])
    token = config['renewableninja']['token']
    time_zone_offset = int(config['renewableninja']['Benintime_zone_offset'])
    Rpath = config['renewableninja']['Rpath']
    seasonAprSept = int(config['model_settings']['seasonAprSept'])
    seasonOctMarch = int(config['model_settings']['seasonOctMarch'])

    urban_profile = config['inputfiles']['Beninurban_profile']
    text_file = config['inputfiles']['Benintext_file']
    country = config['inputfiles']['Benincountry']
    output_folder = config['inputfiles']['Beninoutput_folder']

else:
    #Kenya
    crs = config['geospatialdata']['Kenyacrs']
    sample_file = config['sensitivityanalysis']['Kenyasample_file']
    parameters_file = config['sensitivityanalysis']['Kenyaparameters_file']
    replicates =  int(config['sensitivityanalysis']['replicates'])
    scenarios_folder = config['inputfiles']['Kenyascenarios_folder']
    elec_shp = config['inputfiles']['Kenyaelec_shp']
    scenario_ouput = config['sensitivityanalysis']['Kenyascenario_ouput']
    projectedfolder = config['geospatialdata']['Kenyaprojectedfolder']
    output_files_sample = config['sensitivityanalysis']['Kenyaoutput_files_sample']
    pathfinder_raster_country = config['inputfiles']['Kenyapathfinder_raster_country']
    files = pd.read_csv(config['inputfiles']['Kenyagisfiles'], index_col=0)
    substation = float(config['model_settings']['substation'])
    capacitytoactivity = float(config['model_settings']['capacitytoactivity'])
    distr_losses = float(config['model_settings']['distr_losses'])
    token = config['renewableninja']['token']
    time_zone_offset = int(config['renewableninja']['Kenyatime_zone_offset'])
    Rpath = config['renewableninja']['Rpath']
    seasonAprSept = int(config['model_settings']['seasonAprSept'])
    seasonOctMarch = int(config['model_settings']['seasonOctMarch'])

    urban_profile = config['inputfiles']['Kenyaurban_profile']
    text_file = config['inputfiles']['Kenyatext_file']
    country = config['inputfiles']['Kenyacountry']
    output_folder = config['inputfiles']['Kenyaoutput_folder']

year_array = ['2020', '2021', '2022','2023','2024','2025','2026','2027','2028','2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040']

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
    Dailytemporalresolution= int(float(modelrun.iloc[2][1]))
    DiscountRate = float(modelrun.iloc[1][1])
    CapitalCost_PV_raw = modelrun.iloc[3][1]
    CapitalCost_PV = split_data_onecell(CapitalCost_PV_raw)
    CapitalCost_batt_raw = modelrun.iloc[4][1]
    CapitalCost_batt = split_data_onecell(CapitalCost_batt_raw)
    CapitalCost_WI_raw = modelrun.iloc[5][1]
    CapitalCost_WI = split_data_onecell(CapitalCost_WI_raw)
    CapitalCost_transm = float(modelrun.iloc[6][1])
    CapitalCost_distribution = float(modelrun.iloc[7][1])
    CapacityFactor_adj = round(float(modelrun.iloc[8][1]), 4)
    DemandProfileTier = int(float(modelrun.iloc[9][1]))
    FuelpriceNG_raw = modelrun.iloc[10][1]
    FuelpriceNG = split_data_onecell(FuelpriceNG_raw)
    FuelpriceDIESEL_raw = modelrun.iloc[11][1]
    FuelpriceDIESEL = split_data_onecell(FuelpriceDIESEL_raw)
    FuelpriceCOAL_raw = modelrun.iloc[12][1]
    FuelpriceCOAL = split_data_onecell(FuelpriceCOAL_raw)
    CapitalCost_distribution_ext = float(modelrun.iloc[13][1])

    tier_profile = '%sinput_data/T%i_load profile_Narayan.csv' %(country, DemandProfileTier)

    #read all demand levels as they are linked to the Tier
    df = pd.read_csv('%sinput_data/%s_demand.csv' %(country, country))

    # Electrified demand for selected tier
    elecdemand_df_all = df[(df['Tier'] == DemandProfileTier) & (df['Electrified'] == 1)]
    elecdemand_df = elecdemand_df_all.loc[:, '2020':'2040']
    elecdemand_df.columns = elecdemand_df.columns.astype('int')

    # Un-electrified demand for selected tier
    unelecdemand_df_all = df[(df['Tier'] == DemandProfileTier) & (df['Electrified'] == 0)]
    unelecdemand_df = unelecdemand_df_all.loc[:, '2020':'2040']
    unelecdemand_df.columns = unelecdemand_df.columns.astype('int')

    combined = str(spatial) + str(CapacityFactor_adj)

    if combined not in scenario_runs.values():

        print('1. Aggregating the number of cells per polygon from Pathfinder')
        polygon = str(spatial) + "_polygon.shp"
        point = str(spatial) + "_point.shp"
        path_polygon = projectedfolder + '/'+ polygon

        zonalstat_pathfinder(pathfinder_raster_country, path_polygon, spatial, country)

        print("2. Create the demandcells.csv file and the classifications")
        
        shape =  projectedfolder +'/'+ polygon
        gdp =  projectedfolder + '/'+files.loc['gdp','filename']
        demandcells = os.path.join(os.getcwd(), '%s_run/scenarios/Demand/%i_demand_cells.csv' %(country, spatial))

        if not os.path.exists('%s_run/scenarios/Demand' %(country)):
            os.makedirs('%s_run/scenarios/Demand' %(country))

        join_elec(elec_shp, gdp, shape, spatial, country)
        elec(demandcells, spatial, country)

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)
        print("3. Identify unelectrified %i_polygons " %(spatial))
        polygons_all = projectedfolder+ '/'+polygon
        noHV = '%s_run/%i_noHV_cells.csv' %(country, spatial)
        shape =  "%s_run/scenarios/Demand/%i_un_elec_polygons.shp" %(country, spatial)

        noHV_polygons(polygons_all, noHV, shape, crs)

        #To be able to download you need to install the package curl from R and also have R installed on your computer
        # Easiest is to write  install.packages("curl") in the R prompt

        print("4. Download Renewable Ninja files for scenario %i" %(spatial))
        # add your token for API from your own log in on Renewable Ninjas

        shapefile = projectedfolder + '/'+point
        #Add the path to the RScript.exe under Program Files and add here

        srcpath = os.path.dirname(os.path.abspath(__file__))
        path = "%stemp/%i" %(country, spatial)

        if not os.path.exists('%stemp/%i' %(country, spatial)):
            os.makedirs('%stemp/%i' %(country, spatial))
        coordinates = project_vector(shapefile)
        wind, solar = csv_make(coordinates, path)
        down = download(path, Rpath, srcpath, wind, solar, token)
        adjust_timezone(path, time_zone_offset)
        uncertainty_capacityfactor(path, CapacityFactor_adj)

        print("5. Build transmission technologies")

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)

        demandcells = os.path.join(os.getcwd(), '%s_run/scenarios/Demand/%i_demand_cells.csv' %(country, spatial))
        input_data =  os.path.join(os.getcwd(), '%s_run/scenarios/input_data.csv' %(country))
        if os.path.isfile('%s_run/scenarios/%i_distribution.csv' %(country, spatial)):
            print('File already exists, skipping calculations.')
        else:
            distribution_length_cell_ref = network_length(demandcells, input_data, scenarios_folder, spatial)
        distribution = '%s_run/scenarios/%i_distributionlines.csv' %(country, spatial)
        distribution_row = "_%isum" %(spatial)

        topath = '%s_run/scenarios/Demand' %(country)
        noHV = '%s_run/%i_noHV_cells.csv' %(country, spatial)
        HV_file = '%s_run/%i_HV_cells.csv' %(country, spatial)
        minigrid = '%s_run/%i_elec_noHV_cells.csv' %(country, spatial)
        neartable = '%s_run/scenarios/Demand/%i_Near_table.csv' %(country, spatial)
        if os.path.isfile('%s_run/scenarios/Demand/%i_adjacencymatrix.csv' %(country, spatial)):
            print('File already exists, skipping calculations.')
        else:
            transmission_matrix(neartable, noHV, HV_file, minigrid, topath, spatial, country)

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)

        elec_noHV_cells = '%s_run/%i_elec_noHV_cells.csv' %(country, spatial)
        renewable_path = '%stemp/%i' %(country, spatial)
        pop_shp = projectedfolder + files.loc['pop','filename']
        unelec = '%s_run/%i_un_elec.csv' %(country, spatial)
        elec_ = '%s_run/%i_elec.csv' %(country, spatial)

        #Solar and wind csv files
        renewableninja(renewable_path, scenarios_folder, spatial, CapacityFactor_adj)
        #Location file
        gisfile_ref = GIS_file(scenarios_folder, projectedfolder + '/'+ point, spatial)
        matrix = '%s_run/scenarios/Demand/%i_adjacencymatrix.csv' %(country, spatial)
        capital_cost_transmission_distrib(elec_, noHV, HV_file, elec_noHV_cells, unelec, CapitalCost_transm, substation, capacitytoactivity, scenarios_folder, matrix, gisfile_ref, spatial, CapitalCost_distribution_ext, diesel = True)
        scenario_runs[j] = combined
        
    else:
        print('Scenario already run')

#Read scenarios from sample file

    #temporal_unique = str(Dailytemporalresolution) + str(spatial) + str(elecdemand_df.iloc[0][2040]) + str(DemandProfileTier)+ str(CapacityFactor_adj)

    #if temporal_unique  not in demand_runs.values():
    #Scenarios that are sensitive to spatial and demand simultaneously
    print("Running scenario %s" %j)

#######################

    polygon = str(spatial) + "_polygon.shp"
    point = str(spatial) + "_point.shp"
    print("6. Build Demand for location %i" %(spatial))
    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)

    settlements = '%s_run/scenarios/Demand/%i_demand_cells.csv' %(country, spatial)
    inputdata =  os.path.join(os.getcwd(), '%s_run/scenarios/input_data.csv' %(country))
    calculate_demand(settlements, elecdemand_df, unelecdemand_df,elecdemand_df.iloc[0][2040], spatial, inputdata, country)

    print("7. Build peakdemand, yearsplit, specified demand")

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)
    
    refpath = '%s_run/scenarios' %(country)
    demandcells = os.path.join(os.getcwd(), '%s_run/scenarios/Demand/%i_demand_cells.csv' %(country, spatial))
    distribution_length_cell_ref = '%s_run/scenarios/%i_distribution.csv' %(country, spatial)
    distribution = '%s_run/scenarios/%i_distributionlines.csv' %(country, spatial)
    distribution_row = "_%isum" %(spatial)

    noHV = '%s_run/%i_noHV_cells.csv' %(country, spatial)
    HV_file = '%s_run/%i_HV_cells.csv' %(country, spatial)
    minigrid = '%s_run/%i_elec_noHV_cells.csv' %(country, spatial)
    neartable = '%s_run/scenarios/Demand/%i_Near_table.csv' %(country, spatial)
    demand = '%s_run/scenarios/%i_demand_%i_spatialresolution.csv' %(country, elecdemand_df.iloc[0][2040], spatial)
    
    temporal_id = float(Dailytemporalresolution)

    yearsplit = yearsplit_calculation(temporal_id,seasonAprSept , seasonOctMarch, '%s_run/scenarios/yearsplit_%f.csv' %(country, temporal_id), year_array)
    specifieddemand, timesteps = demandprofile_calculation(tier_profile, temporal_id, seasonAprSept, seasonOctMarch, '%s_run/scenarios/specifiedrural_demand_time%i_tier%i.csv' %(country, int(temporal_id), DemandProfileTier), year_array, 'Minute')
    specifieddemandurban, timesteps = demandprofile_calculation(urban_profile, temporal_id, seasonAprSept, seasonOctMarch, '%s_run/scenarios/specifieddemand_%i.csv' %(country, int(temporal_id)), year_array, 'hour')
    
    peakdemand_csv(demand, specifieddemand,capacitytoactivity, yearsplit, distr_losses, HV_file, distribution, distribution_row, distribution_length_cell_ref, scenarios_folder, spatial, elecdemand_df.iloc[0][2040])
    addtimestep(timesteps,input_data, '%s_run/scenarios/input_data_%i.csv' %(country, int(temporal_id)))
    residual_path = '%s_run/scenarios/residual_capacity%i_demand_%i_spatialresolution.csv' %(country, elecdemand_df.iloc[0][2040], spatial)
    distribution_elec_startyear(demand, capacitytoactivity, distr_losses, year_array, residual_path)

    print("8. Optimise PV and battery")

    date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
    print(date)

    load_yearly = annualload(tier_profile, '%s_run/scenarios/annualload_tier%i.csv' %(country, DemandProfileTier))
    loadprofile_high = '%sinput_data/high_Jan.csv' %(country)
    capacityfactor_pv = '%s_run/scenarios/uncertain%f_spatial%i_capacityfactor_solar.csv' %(country, CapacityFactor_adj,spatial)
    tofilePV = '%s_run/scenarios/capacityfactor_solar_batteries_Tier%i_loca%i_uncertain%f.csv' %(country, DemandProfileTier, spatial, CapacityFactor_adj)
    tofilePVhigh = '%s_run/scenarios/capacityfactor_solar_batteries_urban_loca%i_uncertain%f.csv' %(country, spatial, CapacityFactor_adj)
    efficiency_discharge = 0.98 # Koko (2022)
    efficiency_charge = 0.95 # Koko (2022)
    pvcost = 2540 #ATB 2021 version for 2021 value
    batterycost_kWh = 522  #ATB 2021 version for 2021 value with adjusted Kenyan value
    locations = '%s_run/scenarios/%i_GIS_data.csv' %(country, spatial)
    scenario = 'Tier%i_loca%i_uncertain%f.csv' %(DemandProfileTier, spatial, CapacityFactor_adj)
    startDate = pd.to_datetime("2016-01-02")
    endDate = pd.to_datetime("2016-02-02")
    startDate_load = pd.to_datetime("1900-01-02")
    endDate_load = pd.to_datetime("1900-02-02")
    if os.path.isfile(tofilePV):
        print('File already exists, skipping calculations.')
    else:
        battery_to_pv(load_yearly,  capacityfactor_pv, efficiency_discharge, efficiency_charge, locations, pvcost, batterycost_kWh, tofilePV, scenario,  startDate, endDate, startDate_load, endDate_load, country)
    if os.path.isfile(tofilePVhigh):
        print('File already exists, skipping calculations.')
    else:
        battery_to_pv(loadprofile_high,  capacityfactor_pv, efficiency_discharge, efficiency_charge, locations, pvcost, batterycost_kWh, tofilePVhigh, scenario,  startDate, endDate, startDate, endDate, country)

    #demand_runs[j] = temporal_unique
            
    #else:
        #print('Scenario already run')

    ####################### Make txt file #############################
    dict_df = load_csvs(scenarios_folder) #args.data_path) #
    outPutFile = make_outputfile(text_file)#args.template) #
    comb = country+j+'.txt'

    if os.path.isfile('%s_run/output/' %(country) +comb):
        print('File already exists, skipping calculations.')
    else:
        outPutFile = functions_to_run(dict_df, outPutFile, spatial, elecdemand_df.iloc[0][2040], DiscountRate, temporal_id, CapitalCost_PV, 
                                    CapitalCost_batt, CapitalCost_WI, CapitalCost_distribution, CapacityFactor_adj, 
                                    FuelpriceNG, FuelpriceDIESEL, FuelpriceCOAL, DemandProfileTier, country)

        #write data file
        if not os.path.exists(output_folder):
            os.makedirs('%s_run/output' %(country))

        write_to_file('%s_run/output/' %(country), outPutFile, comb)      #args.output_path