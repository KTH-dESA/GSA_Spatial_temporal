"""
Module: Build_csv_files
=============================

A module for building the csv-files for GEOSeMOSYS https://github.com/KTH-dESA/GEOSeMOSYS to run that code
In this module the logic around electrified and un-electrified cells are implemented for the 378 cells

---------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
import pandas as pd
from datetime import datetime, timedelta
import geopandas as gpd
import os
import fnmatch
import numpy as np


from modelgenerator.PV_battery_optimisation import optimize_battery_pv

pd.options.mode.chained_assignment = None

def battery_to_pv(loadprofile, capacityfactor_pv, efficiency_discharge, efficiency_charge, locations, pv_cost, battery_cost, tofilePV, scenario, startDate, endDate, startDate_load, endDate_load, country):
    """This function re-distributes the load based on the load and capacity factor from renewable ninja
    """

    def calculate_average(data, startdate, enddate):
        mask = (data.index > startdate) & (data.index <= enddate)
        thisMonthOnly = data.loc[mask]
        return (thisMonthOnly)


    
    def indexfix(df_path):
        df = pd.read_csv(df_path).dropna()
        df_copy = df.copy()
        df_copy_datetime = pd.to_datetime(df_copy['adjtime'], errors='coerce', format='%Y/%m/%d %H:%M')
        df_copy.index = df_copy_datetime
        df_copy = df_copy.drop(columns=['adjtime'])
        return df_copy

    capacityf_solar = indexfix(capacityfactor_pv)
    load = indexfix(loadprofile)
    df = pd.read_csv(locations, index_col=0, header=0)
    
    PV_range = calculate_average(capacityf_solar, startDate, endDate)
    load_range =calculate_average(load, startDate_load, endDate_load)

    #normalise data
    def reduceload_data(data):
        max = data['Load'].max()
        x = 1/max
        data_list = []
        for i in data['Load']:
            norm = i*x
            data_list.append(norm)
        
        adjusted_load = pd.DataFrame(data_list, index = data.index, columns = ['Load'])

        return adjusted_load

    adjusted_load = reduceload_data(load_range)

    PV_size = {}
    battery_size = {}
    capacityf_solar_batteries = PV_range.copy() #deep copy
    for row in df.iterrows():
        location = str(row[0])
        capacityf_solar_batteries_location = capacityf_solar_batteries[[location]]
        PVadj, batterytime = optimize_battery_pv(capacityf_solar_batteries_location, location, adjusted_load, efficiency_discharge,  efficiency_charge, pv_cost, battery_cost, scenario, country)
        PV_size[location]= PVadj
        battery_size[location] = batterytime
        print("location=", location, "PV-size =", PVadj, "Batterytime=", batterytime)

    df_PV_size = pd.DataFrame(PV_size.items(), columns=['location', 'PV_size'])
    df_battery_size = pd.DataFrame(battery_size.items(), columns=['location','Battery_hours'])
    df_PV_battery_size = pd.merge(df_PV_size, df_battery_size)

    df_PV_battery_size.to_csv(tofilePV)

def annualload(minuteload, topath, nr_timeslice):
    minutedf = pd.read_csv(minuteload)
    minutedf['Datetime'] = pd.to_datetime('1900-01-01 ' + minutedf['Minute'])
    minutedf.set_index('Datetime', inplace=True)
    length = str(1440/nr_timeslice)+'min'

    minutedf_group = minutedf.groupby([pd.Grouper(freq=length)]).mean().reset_index()
    minutedf_group.set_index('Datetime', inplace=True)

    hours = []
    for i in range(0,24):
        fromDate =  pd.to_datetime(f'1900-01-01 {i}:00')
        toDate = fromDate + timedelta(hours=1)

        result = minutedf_group.loc[fromDate:toDate]
        if (result.values.size == 0):
            value = hours[i-1]
        else:
            value = result.values[0][0]
     
        hours.append(value)
    rural_dataframe = pd.DataFrame(hours)
        
    s = rural_dataframe.squeeze()
    fullyearhours = pd.DataFrame(np.tile(s, 365), columns = ['Load'])
    # create a new DataFrame with a datetime index that spans the entire year
    start_date = pd.to_datetime('1900-01-01')
    end_date = pd.to_datetime('1901-01-01')
    idx = pd.date_range(start=start_date, end=end_date, freq='H')
    yearly_data = pd.DataFrame(index=idx)

    fullyearhours.index = yearly_data.index[:8760]
    fullyearhours['adjtime'] = fullyearhours.index

    fullyearhours.to_csv(topath)
    return topath

def highprofile_aggre(hourlyprofile, topath, nr_timeslice):
    hourlydf = pd.read_csv(hourlyprofile)
    hourlydf['adjtime'] = pd.to_datetime(hourlydf['adjtime'])
    hourlydf.set_index('adjtime', inplace=True)
    length = str(1440/nr_timeslice)+'min'

    hourlydf_group = hourlydf.groupby([pd.Grouper(freq=length)]).mean().reset_index()
    hourlydf_group.set_index('adjtime', inplace=True)
    
    hourly_data = hourlydf_group.resample('H').ffill()

    hourly_data.to_csv(topath)
    return topath

def renewableninja(path, dest, spatial, CapacityFactor_adj):
    """
    This function organize the data to the required format of a matrix with the
    location name on the x axis and hourly data on the y axis so that it can be fed into https://github.com/KTH-dESA/GEOSeMOSYS code
    the data is saved as capacityfactor_wind.csv and capacityfactor_solar.csv
    :param path:
    :param dest:
    :return:
    """
    files = os.listdir(path)
    outwind = []
    outsolar = []
    for file in files:

        if fnmatch.fnmatch(file, "uncertainty"+str(CapacityFactor_adj)+'w*'):

            file = os.path.join(path,file)
            wind = pd.read_csv(file, index_col='adjtime')
            outwind.append(wind)
    for file in files:

        if fnmatch.fnmatch(file, "uncertainty"+str(CapacityFactor_adj)+'s*'):

            file = os.path.join(path,file)
            solar = pd.read_csv(file, index_col='adjtime')
            outsolar.append(solar)
    try:
        solarbase = pd.concat(outsolar, axis=1)
        windbase = pd.concat(outwind, axis=1)
    except:
        print('Only one solar/wind file')
        solarbase = outsolar[0]
        windbase = outwind[0]
    header = solarbase.columns
    new_header = [x.replace('X','') for x in header]
    solarbase.columns = new_header

    #solarbase.drop('Unnamed: 0', axis='columns', inplace=True)
    solarbase.to_csv(os.path.join(dest, 'uncertain%f_spatial%i_capacityfactor_solar.csv' %(CapacityFactor_adj,spatial)))

    header = windbase.columns
    new_header = [x.replace('X','') for x in header]
    windbase.columns = new_header
    #windbase.drop('Unnamed: 0', axis='columns', inplace=True)
    windbase.to_csv(os.path.join(dest, 'uncertain%f_spatial%i_capacityfactor_wind.csv' %(CapacityFactor_adj,spatial)))
    return()

def GIS_file(dest, point, spatial):
    """
    Creates the GIS location file which determins the spatial resolution
    :param dest:
    :return:
    """
    df_point = gpd.read_file(point)
    GIS_data = df_point.id
    grid = pd.DataFrame(GIS_data, copy=True)
    grid.columns = ['Location']
    grid.to_csv(os.path.join(dest, '%i_GIS_data.csv' %(spatial)), index=False)
    return(os.path.join(dest, '%i_GIS_data.csv' %(spatial)))

## Build files with elec/unelec aspects
def capital_cost_transmission_distrib(elec, noHV_file, HV_file, elec_noHV_cells_file, unelec, capital_cost_HV, substation, capacitytoactivity, path, adjacencymatrix,gis_file, scenario,distribu_cost, diesel):
    """Reads the transmission lines shape file, creates empty files for inputactivity, outputactivity, capitalcost for transmission lines.

    :param distribution_network: a csv file with number of cells included in Pathfinder
    :param elec: are the 40*40m cells that have at least one cell of electrified 1x1km inside it
    :param unelec: are the 40*40m cells that have NO electrified cells 1x1km inside it
    :param noHV: are the cells that are electrified and not 5000 m from a minigrid and not 50,000 m from the exsiting HV-MV grid the cell are concidered electrified by transmission lines.
    :param transmission_near: Is the distance to closest HV line from the center of the 40*40 cell
    :param capital_cost_HV: kUSD/MW
    :param substation: kUSD/MW
    :param capital_cost_LV: kUSD/MW
    :param adajencymatrix is the model where the adjacent cell is located
    :return:
    """

    #gdf = gpd.read_file(transmission_near)
    #transm = pd.DataFrame(gdf)
    #transm.index = transm['id']

    capitalcost = pd.DataFrame(columns=['Technology', 'Capitalcost'], index=range(0,5000)) # dtype = {'Technology':'object', 'Capitalcost':'float64'}

    fixedcost = pd.DataFrame(columns=['Technology', 'Fixed Cost'], index=range(0,5000)) # dtype = {'Technology':'object', 'Fixed cost':'float64'}

    variablecost = pd.DataFrame(columns=['Technology', 'Variable Cost', 'ModeofOperation'], index=range(0,5000)) # dtype = {'Technology':'object', 'Variable cost':'float64',}

    inputactivity = pd.DataFrame(columns=['Column','Fuel','Technology','Inputactivity','ModeofOperation'], index=range(0,10000))

    outputactivity = pd.DataFrame(columns=['Column','Fuel',	'Technology','Outputactivity','ModeofOperation'], index=range(0,10000))

    operationallife = pd.DataFrame(columns=['Technology', 'Life'], index=range(0,5000)) # dtype = {'Technology':'object', 'Variable cost':'float64',}


    elec = pd.read_csv(elec)
    gis = pd.read_csv(gis_file)
    try:
        matrix = pd.read_csv(adjacencymatrix)
    except:
        print("No adjacencymatrix for this scenario")
        pass
    elec.id = elec.id.astype(int)
    un_elec = pd.read_csv(unelec)
    un_elec.id = un_elec.id.astype(int)
    noHV = pd.read_csv(noHV_file)
    HV = pd.read_csv(HV_file)
    elec_noHV_cells = pd.read_csv(elec_noHV_cells_file)
    noHV.id = noHV.id.astype(int)

    m = 0
    input_temp = []
    output_temp = []
    capital_temp = []

    ## Electrified by HV in baseyear
    for k in HV['id']:

        input_temp = [0, "TREL2", "TRLV_%i_0" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        #The TRLVM is introduced as one technology cannot input two fuels in the same timeslice
        #This is for minigrid supply
        input_temp = [0, "EL2_%i" %(k), "TRLVM_%i_0" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        input_temp = [0, "TREL2","EL00d_%i" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        capitalcost.loc[k]['Capitalcost'] = distribu_cost
        capitalcost.loc[k]['Technology'] =  "EL00d_%i" %(k)

        output_temp = [0, "EL3_%i_1" % (k), "EL00d_%i" % (k), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        operationallife_temp = ["EL00d_%i" % (k), 60]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

        output_temp = [0, "EL3_%i_0" % (k), "TRLV_%i_0" % (k), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        #The TRLVM is introduced as one technology cannot input two fuels in the same timeslice
        #This is for minigrid supply
        output_temp = [0, "EL3_%i_0" % (k), "TRLVM_%i_0" % (k), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (k), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k),  "SOPVBattery_%i_1" % (k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        fixedcost_temp = ["SOPVBattery_%i_0" % (k), 96]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()

        
        operationallife_temp = ["SOPVBattery_%i_0" % (k), 30]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()


        output_temp = [0, "EL3_%i_0" % (k),  "SOPVBattery_%i_0" % (k), 1,1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k),  "SOPV_%i_1" % (k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (k),  "SOPV_%i_0" % (k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        fixedcost_temp = ["SOPV_%i_0" % (k), 18]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()

        operationallife_temp = ["SOPV_%i_0" % (k), 30]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

    # Electrified by minigrid in base year
    for m in elec_noHV_cells['id']:
        input_temp = [0, "TREL2","EL00d_%i" %(m), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (m), "EL00d_%i" % (m), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        capitalcost.loc[k+m]['Capitalcost'] = distribu_cost
        capitalcost.loc[k+m]['Technology'] =  "EL00d_%i" %(m)

        operationallife_temp = ["EL00d_%i" % (m), 60]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()
        
        input_temp = [0,"EL2_%i" %(m),"TRLV_%i_0" %(m), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (m), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m), "TRLV_%i_0" % (m), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (m),  "SOPVBattery_%i_1" % (m), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m),  "SOPVBattery_%i_0" % (m), 1,1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        fixedcost_temp = ["SOPVBattery_%i_0" % (m), 96]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()

        operationallife_temp = ["SOPVBattery_%i_0" % (m), 30]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

        output_temp = [0, "EL3_%i_1" % (m),  "SOPV_%i_1" % (m), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m),  "SOPV_%i_0" % (m), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        fixedcost_temp = ["SOPV_%i_0" % (m), 18]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()

        operationallife_temp = ["SOPV_%i_0" % (m), 30]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

    # No electrified cells in the base year
    for j in noHV['id']:

        input_temp = [0, "EL2_%i" %(j),"TRLV_%i_0" %(j), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        operationallife_temp = ["TRLV_%i_0" %(j), 60]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

        output_temp = [0, "EL3_%i_0" % (j), "TRLV_%i_0" % (j), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL3_%i_0" % (j), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,"EL3_%i_0" % (j),"SOPVBattery_%i_0" % (j), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()
        
        fixedcost_temp = ["SOPVBattery_%i_0" % (j), 96]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()

        operationallife_temp = ["SOPVBattery_%i_0" % (j), 30]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

        output_temp = [0,"EL3_%i_0" % (j),"SOPV_%i_0" % (j), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        fixedcost_temp = ["SOPV_%i_0" % (j), 18]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()

        operationallife_temp = ["SOPV_%i_0" % (j), 30]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

    #For all cells
    for k in range(1,len(gis)+1):

        output_temp = [0,  "EL2_%i" % (k),"SOMGBattery_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()
        
        fixedcost_temp = ["SOMGBattery_%i" %(k), 44]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()

        operationallife_temp = ["SOMGBattery_%i" %(k), 30]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

        output_temp = [0,  "EL2_%i" % (k),"WI_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        fixedcost_temp = ["WI_%i" %(k), 39]
        fixedcost.loc[-1] = fixedcost_temp  # adding a row
        fixedcost.index = fixedcost.index + 1  # shifting index
        fixedcost = fixedcost.sort_index()
        
        operationallife_temp = ["WI_%i" %(k), 20]
        operationallife.loc[-1] = operationallife_temp  # adding a row
        operationallife.index = operationallife.index + 1  # shifting index
        operationallife = operationallife.sort_index()

        if diesel == True:
            output_temp = [0,  "EL2_%i" % (k),"DSGEN_%i" %(k), 1, 1]
            outputactivity.loc[-1] = output_temp  # adding a row
            outputactivity.index = outputactivity.index + 1  # shifting index
            outputactivity = outputactivity.sort_index()

            input_temp = [0,  "KEDS", "DSGEN_%i" %(k), 4, 1]
            inputactivity.loc[-1] = input_temp  # adding a row
            inputactivity.index = inputactivity.index + 1  # shifting index
            inputactivity = inputactivity.sort_index()

            variablecost.loc[k]['Variable Cost'] = 4.17
            variablecost.loc[k]['Technology'] = "DSGEN_%i" %(k)
            variablecost.loc[k]['ModeofOperation'] =1 

            fixedcost_temp = ["DSGEN_%i" %(k), 15]
            fixedcost.loc[-1] = fixedcost_temp  # adding a row
            fixedcost.index = fixedcost.index + 1  # shifting index
            fixedcost = fixedcost.sort_index()

            operationallife_temp = ["DSGEN_%i" %(k), 15]
            operationallife.loc[-1] = operationallife_temp  # adding a row
            operationallife.index = operationallife.index + 1  # shifting index
            operationallife = operationallife.sort_index()
            
    try:
        output_matrix = matrix.drop(['INFUEL','SendTech','Unnamed: 0'], axis=1)

        matrix_out = output_matrix.drop_duplicates()

        for l in matrix_out.index:
            output_temp = [0,  matrix.loc[l]['OUTFUEL'], matrix.loc[l]['INTECH'], 1, 1]
            outputactivity.loc[-1] = output_temp  # adding a row
            outputactivity.index = outputactivity.index + 1  # shifting index
            outputactivity = outputactivity.sort_index()

        input_matrix = matrix.drop(['OUTFUEL','SendTech','Unnamed: 0'], axis=1)
        matrix_in = input_matrix.drop_duplicates()

        for l in matrix_in.index:
            input_temp = [0,  matrix.loc[l]['INFUEL'], matrix.loc[l]['INTECH'], 1, 1]
            inputactivity.loc[-1] = input_temp  # adding a row
            inputactivity.index = inputactivity.index + 1  # shifting index
            inputactivity = inputactivity.sort_index()

        tech_matrix = matrix.drop(['SendTech','INFUEL','OUTFUEL','Unnamed: 0'], axis=1)
        tech_matr = tech_matrix.drop_duplicates()
        for h in tech_matr.index:
            #capitalcost.loc[m]['Capitalcost'] = tech_matr.loc[h]['DISTANCE'] /1000*capital_cost_HV + substation  #kUSD/MW divided by 1000 as it is in meters
            #capitalcost.loc[m]['Technology'] =  matrix.loc[h]['INTECH']

            capitalcost_temp = [matrix.loc[h]['INTECH'],tech_matr.loc[h]['DISTANCE'] /1000*capital_cost_HV + substation]
            capitalcost.loc[-1] = capitalcost_temp  # adding a row
            capitalcost.index = capitalcost.index + 1  # shifting index
            capitalcost = capitalcost.sort_index()

            #fixedcost.loc[m]['Fixed Cost'] = tech_matr.loc[h]['DISTANCE']/1000*capital_cost_HV*0.025 + substation*0.025  #kUSD/MW divided by 1000 as it is in meters
            #fixedcost.loc[m]['Technology'] =  matrix.loc[h]['INTECH']
            
            fixedcost_temp = [matrix.loc[h]['INTECH'], tech_matr.loc[h]['DISTANCE']/1000*capital_cost_HV*0.025 + substation*0.025 ]
            fixedcost.loc[-1] = fixedcost_temp  # adding a row
            fixedcost.index = fixedcost.index + 1  # shifting index
            fixedcost = fixedcost.sort_index()

            operationallife_temp = [matrix.loc[h]['INTECH'], 60]
            operationallife.loc[-1] = operationallife_temp  # adding a row
            operationallife.index = operationallife.index + 1  # shifting index
            operationallife = operationallife.sort_index()
            m = m+1
    except:
        pass

    df1 = outputactivity['Fuel']
    df2 = inputactivity['Fuel']

    fuels = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
    tech1 = outputactivity['Technology']
    tech2 = inputactivity['Technology']
    technolgies = pd.concat([tech1, tech2]).drop_duplicates().reset_index(drop=True)

    df3 = outputactivity['Technology']
    df4 = inputactivity['Technology']

    technologies = pd.concat([df3, df4]).drop_duplicates().reset_index(drop=True)
    techno = pd.Series(technologies[~technologies.str.startswith('SO_', na=False)])
    techno = pd.DataFrame(techno, columns = ['Technology'])

    capacitytoa = pd.DataFrame(columns=['Capacitytoactivity'], index= range(0,len(techno)))
    capacitytoact = pd.concat([capacitytoa, techno], axis=1, ignore_index=True)
    capacitytoactiv = capacitytoact.assign(Capacitytoactivity = capacitytoactivity)

    inputactivity.dropna(subset=['Technology'], inplace=True)
    capacitytoactiv.dropna(subset=[1], inplace=True)
    outputactivity.dropna(subset=['Technology'], inplace=True)
    capitalcost.dropna(subset=['Technology'], inplace=True)
    variablecost.dropna(subset=['Technology'], inplace=True)
    fixedcost.dropna(subset=['Technology'], inplace=True)
    operationallife.dropna(subset=['Technology'], inplace=True)

    fixedcost.to_csv(os.path.join(path, '%i_fixed_cost.csv' %(scenario)))
    variablecost.to_csv(os.path.join(path, '%i_variable_cost.csv' %(scenario)))
    operationallife.to_csv(os.path.join(path, '%i_operationallife.csv' %(scenario)))
    capitalcost.to_csv(os.path.join(path, '%i_%i_%icapitalcost.csv' %(scenario, int(distribu_cost), int(capital_cost_HV))))
    inputactivity.to_csv(os.path.join(path, '%i_inputactivity.csv' %(scenario)))
    outputactivity.to_csv(os.path.join(path, '%i_outputactivity.csv'%(scenario)))
    capacitytoactiv.to_csv(os.path.join(path, '%i_capacitytoactivity.csv'%(scenario)))
    fuels.to_csv(os.path.join(path, '%i_fuels.csv'%(scenario)))
    technolgies.to_csv(os.path.join(path, '%i_technologies.csv'%(scenario)))

def near_dist(pop_shp, un_elec_cells, path, CR):

    unelec = pd.read_csv(un_elec_cells, usecols= ["id"])
    point = gpd.read_file(os.path.join(path, pop_shp))
    point.index = point['id']
    unelec_shp = gpd.GeoDataFrame(crs=32737)
    for i in unelec['id']:
        unelec_point = point.loc[i]
        unelec_shp = unelec_shp.append(unelec_point)

    lines = gpd.read_file(os.path.join(path, 'Concat_Transmission_lines_UMT37S.shp'))

    unelec_shp['HV_dist'] = unelec_shp.geometry.apply(lambda x: lines.distance(x).min())
    unelec_shp.set_crs(CR)
    outpath = "run/Demand/transmission.shp"
    unelec_shp.to_file(outpath)

    return(outpath)

def noHV_polygons(polygons, noHV, outpath, CR):

    unelec = pd.read_csv(noHV, usecols=["id"])
    point = gpd.read_file(polygons)
    unelec_shp = gpd.GeoDataFrame(geometry=[], crs=CR)

    for i in unelec['id']:
        unelec_point = point[point['id'] == int(i)]
        unelec_shp = gpd.GeoDataFrame(pd.concat([unelec_shp, unelec_point]))

    unelec_shp.to_file(outpath)

    # unelec = pd.read_csv(noHV, usecols= ["id"])
    # point = gpd.read_file(polygons)
    # #point.index = point['FID']
    # unelec_shp = gpd.GeoDataFrame(geometry=[], crs=CR)
    # for i in unelec['id']:
    #     unelec_point = point.loc[i]
    #     unelec_shp = pd.concat([unelec_shp, unelec_point])
    # unelec_shp.to_file(outpath)
