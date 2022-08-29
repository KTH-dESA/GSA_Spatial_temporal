from datetime import datetime
import os
import pandas as pd
from Build_csv_files import *
from renewable_ninja_download import *
from post_elec_GIS_functions import *
from Pathfinder_processing_step import *

os.chdir(os.path.dirname(os.path.abspath(__file__)))

files = pd.read_csv('input_data/Benin_GIS_files.csv', index_col=0)
crs = "EPSG:32631"

### Scenario settings ###

#TODO Integrate the scenario generator with SNAKEMAKE file. To understand is if I send a number or if I send a file.
scenario = pd.read_csv('sensitivity/unique_morris.csv', header=None)

if not os.path.exists('run/scenarios'):
    os.makedirs('run/scenarios')

for j in range(0,len(scenario.index)):
        print("Running scenario %i" %j)
        spatial = int(scenario[0][j])

        #TODO Modify the Pathfinder file to run zonal statistics sum on polygon.
        #TODO Add demand as scenario parameter
    #######################

        polygon = str(spatial) + "_polygon.shp"
        point = str(spatial) + "_point.shp"

        print('1. Aggregating the number of cells per polygon from Pathfinder')
        path_polygon = '../Projected_files/' + polygon
        pathfinder_raster_country = os.path.join('temp/dijkstra','path_cleaned.tif')
        zonalstat_pathfinder(pathfinder_raster_country, path_polygon, spatial)

        #Make sure you are in the /src directory when you start this script
        print(os.getcwd())
        print("2. Create the demandcells.csv file and the classifications")
        from post_elec_GIS_functions import *
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
        from Distribution import *
        from post_elec_GIS_functions import network_length
        
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
        elec = 'run/%i_elec.csv' %(spatial)
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

        capital_cost_transmission_distrib(elec, noHV, HV, elec_noHV_cells, unelec, capital_cost_HV, substation, capacitytoactivity, scenariopath, matrix, gisfile_ref, spatial, diesel = True)

demand_scenario_nan = scenario[1]
demand_scenario_list = [x for x in demand_scenario_nan if str(x) != 'nan']
#Read scenarios from sample file
for k in range(0,len(demand_scenario_list)):

    demand_scenario = int(demand_scenario_list[k])

    #Scenarios that are sensitive to spatial and demand simultaneously
    for j in range(0,len(scenario.index)):
        print("Running scenario %i" %j)
        spatial = int(scenario[0][j])

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
        calculate_demand(settlements, demand, demand_scenario, spatial, input_data)

        print("Build peakdemand")

        date = datetime.now().strftime("%Y %m %d-%I:%M:%S_%p")
        print(date)
        from Distribution import *
        from post_elec_GIS_functions import network_length
        
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

dr_scenario_nan = scenario[1]
dr_scenario_list = [x for x in dr_scenario_nan if str(x) != 'nan']
#Read scenarios from sample file
for m in range(0,len(dr_scenario_list)):
    dr = int(dr_scenario_list[m])
    discountr = 'input_data/Benin_discountrate.csv'
    discountrate_csv(discountr, dr)
