import pandas as pd
import numpy as np
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime
import math

import logging

logger = logging.getLogger(__name__)

def load_csvs(paths):
    """Creates a dataframe dictionary from the csv files in /data : dict_df

    Arguments
    ---------
    param_file : paths
        Path to the data files (/data)
    """
    filepaths = [f for f in listdir(paths) if isfile(join(paths, f))]
    onlyfiles = [os.path.join(paths, f) for f in filepaths]
    dict_df = {}
    for files in onlyfiles:
    #validate that the files are csv. Else the read function will not work
        _, filename = os.path.split(files)
        name, ending = os.path.splitext(filename)
        if ending == '.csv':
            dict_df[name] = pd.read_csv(files, header=0)
        else:
            print('You have mixed file types in you directory, please make sure all are .csv type! {}'.format(files))

    return dict_df

def make_outputfile(param_file):
    """Creates a string from the template OSeMOSYS file

    Arguments
    ---------
    param_file : str
        Path to the parameter file
    """
    allLinesFromXy = ""
    with open(param_file, "r") as inputFile:
        allLinesFromXy = inputFile.read()
    outPutFile = allLinesFromXy
    return outPutFile

def functions_to_run(dict_df, outPutFile,spatial, demand_scenario, discountrate_scenario, temporal, CapitalCost_PV, 
                                  CapitalCost_batt, CapitalCost_WI, CapitalCost_distribution, CapacityFactor_adj, FuelpriceNG, FuelpriceDIESEL, FuelpriceCOAL, DemandProfileTier, country, COMBatt_df, SOMG_df, Heavyfueloil_df, distribution_str, transmiss):
    """Runs all the functions for the different parameters

    Arguments
    ---------
    dict_df, outPutFile
        dict_df: is a dictionary which contains all the csv files as dataframes from load_csv. Key is the name of the csv file
        outPutFile: is a string with the empty OSeMOSYS parameters file from make_outputfile
    """
    year_array = ['2020', '2021', '2022','2023','2024','2025','2026','2027','2028','2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040']

    if '%i_operationallife' %(spatial) in dict_df:
        outPutFile = operational_life(outPutFile, dict_df['input_data'], dict_df['%i_operationallife' %(spatial)])
    else:
        print('No operational_life file')
#######################################################################
    if '%i_%i_%ipeakdemand' %(spatial, demand_scenario, temporal) in dict_df:
        outPutFile = peakdemand(outPutFile, dict_df['input_data'], dict_df['%i_%i_%ipeakdemand' %(spatial, demand_scenario, temporal)])
    else:
        print('No peakdemand file')
#################################################################################
    if '%i_distributionlines' %(spatial) in dict_df:
        outPutFile = maxkm(outPutFile, dict_df['input_data'], dict_df['%i_distributionlines' %(spatial) ], dict_df['%i_distribution' %(spatial)], dict_df['%i_HV_cells' %(spatial)])
    else:
        print('No distributionlines file')
########################################################################################################
    outPutFile = capitalcostkmkW(outPutFile, dict_df['input_data'], CapitalCost_distribution, dict_df['%i_%i_%ipeakdemand' %(spatial, demand_scenario, temporal)])

########################################################################################################
    # if '%i_capitalcost'%(spatial) in dict_df:
    #     outPutFile = capapacityofonetech(outPutFile, dict_df['input_data'], dict_df['%i_capitalcost' %(spatial)], capacityofonetech_param,dict_df['capacitycostHV'])

    # else:
    #     print('No i_capitalcost file')
############################################################################################################

    if '%i_fixed_cost' %(spatial) in dict_df:
            outPutFile = fixedcost(dict_df['%i_GIS_data' %(spatial)], outPutFile, dict_df['input_data'], dict_df['%i_fixed_cost' %(spatial) ])
    else:
        print('No fixed_cost file')
#####################################################################################
    if 'total_annual_technology_limit' in dict_df:
        outPutFile = totaltechnologyannualactivityupperlimit(dict_df['%i_GIS_data' %(spatial)], outPutFile, dict_df['input_data'], dict_df['total_annual_technology_limit'])
    else:
        print('No total_annual_technology_limit file')
########################################################################################################
    if '%i_demand_%i_spatialresolution'%(demand_scenario, spatial) in dict_df:
            outPutFile = specifiedannualdemand(outPutFile, dict_df['%i_demand_%i_spatialresolution'%(demand_scenario, spatial) ], dict_df['input_data'])
    else:
        print('No demand file')
####################################################################################
    outPutFile = capitalcost_dynamic(dict_df['%i_GIS_data' %(spatial)], outPutFile,  CapitalCost_PV, 
                                   CapitalCost_batt, CapitalCost_WI, COMBatt_df, SOMG_df, dict_df['input_data'],dict_df['%i_elec' %(spatial)],dict_df['%i_un_elec' %(spatial)], dict_df['capacityfactor_solar_batteries_Tier%i_loca%i_uncertain%f' %(DemandProfileTier, spatial, CapacityFactor_adj)], dict_df['capacityfactor_solar_batteries_urban_loca%i_uncertain%f' %(spatial, CapacityFactor_adj)])
###########################################################################
    outPutFile = capitalcost(outPutFile, dict_df['%i_%i_%icapitalcost' %(spatial, int(distribution_str), int(transmiss))], dict_df['input_data'])

#################################################################################

    if '%i_capacitytoactivity' %(spatial) in dict_df:
        outPutFile = capacitytoactivity(dict_df['%i_capacitytoactivity'%(spatial)], outPutFile, dict_df['input_data'])
    else:
        print('No capacitytoactivity file')
#################################################################################
    if 'timedependent_params_temporal%i_tier%i_uncertaint%f_spatial%i' %(temporal, DemandProfileTier, CapacityFactor_adj,spatial) in dict_df:
       outPutFile = SpecifiedDemandProfile(outPutFile, dict_df['timedependent_params_temporal%i_tier%i_uncertaint%f_spatial%i' %(temporal, DemandProfileTier, CapacityFactor_adj,spatial)],
                                            dict_df['input_data'], dict_df['%i_demand_%i_spatialresolution'%(demand_scenario,spatial)], year_array)
    else:
        print('No timedependent file')
###########################################################
###################### Mode of operation parameters######################

    if 'emissions' in dict_df:
        outPutFile = emissionactivity(dict_df['%i_GIS_data'%(spatial)], outPutFile, dict_df['input_data'], dict_df['emissions'])
    else:
        print('No emissions file')
########################################################
    if '%i_variable_cost' %(spatial) in dict_df:
        outPutFile = variblecost(dict_df['%i_GIS_data' %(spatial)], outPutFile, dict_df['input_data'], dict_df['%i_variable_cost' %(spatial)],FuelpriceNG, FuelpriceDIESEL, FuelpriceCOAL, Heavyfueloil_df)
    else:
        print('No variable_cost file')
#############################################################
    if '%i_inputactivity'%(spatial) in dict_df:
        outPutFile = inputact(outPutFile, dict_df['%i_inputactivity'%(spatial)], dict_df['input_data'], country)
    else:
        print('No inputactivity file')

################################################################
    if '%i_outputactivity'%(spatial) in dict_df:
        outPutFile = outputactivity(outPutFile, dict_df['%i_outputactivity'%(spatial)], dict_df['input_data'])
    else:
        print('No outputactivity file')
################################################################

    if discountrate_scenario!=0:
        outPutFile = discountrate_(outPutFile,discountrate_scenario)
    else:
        print('No discountrate file')
    ##################################################
    #ResultsPath
    outPutFile = resultspath(outPutFile, spatial, demand_scenario, discountrate_scenario)

    ###########################################################

    if '%i_technologies' %(spatial) in dict_df:
        outPutFile = SETS(outPutFile, dict_df['%i_technologies' %(spatial)], dict_df['%i_fuels' %(spatial)],CapitalCost_WI, dict_df['yearsplit_temporal%i_Tier%i_uncertaint%f_spatial_%i' %(temporal, DemandProfileTier, CapacityFactor_adj,spatial)])
    else:
        print('No technologies file')
     ###########################################################   
    if 'yearsplit_temporal%i_Tier%i_uncertaint%f_spatial_%i' %(temporal, DemandProfileTier, CapacityFactor_adj,spatial) in dict_df:
        outPutFile = yearsplit_generator(outPutFile, dict_df['yearsplit_temporal%i_Tier%i_uncertaint%f_spatial_%i' %(temporal, DemandProfileTier, CapacityFactor_adj,spatial)])
    else:
        print('No yearsplit file')

    ##############################################################

    if 'residual_capacity%i_demand_%i_spatialresolution' %(demand_scenario,spatial) in dict_df:
        outPutFile = residualcapacity(outPutFile, dict_df['residual_capacity%i_demand_%i_spatialresolution' %(demand_scenario,spatial)], country)
    else:
        print('No residual capacity file')
    ################################################################

    if 'timedependent_params_temporal%i_tier%i_uncertaint%f_spatial%i' %(temporal, DemandProfileTier, CapacityFactor_adj,spatial) in dict_df.keys():
       outPutFile = capacityfactor(outPutFile, dict_df['%i_GIS_data'%(spatial)], dict_df['timedependent_params_temporal%i_tier%i_uncertaint%f_spatial%i' %(temporal, DemandProfileTier, CapacityFactor_adj,spatial)], 
                                   dict_df['input_data'], dict_df['%i_elec'%(spatial)], dict_df['%i_un_elec'%(spatial)], year_array)
    else:
       print('No timedependent_params_temporal file')
    ###############################################################################

    return(outPutFile)

def residualcapacity(outPutFile, residual_df, country):
    dataToInsert = ""
    print("FUEL SET", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    if country == 'Benin':
        param = "param ResidualCapacity default 0 :=\n[Benin,*,*]:\n2020	2021	2022	2023	2024	2025	2026	2027	2028	2029	2030	231	2032	2033	2034	2035	2036	2037	2038	2039	2040:=\n"
    else:
        param = "param ResidualCapacity default 0 :=\n[Kenya,*,*]:\n2020	2021	2022	2023	2024	2025	2026	2027	2028	2029	2030	2031	2032	2033	2034	2035	2036	2037	2038	2039	2040:=\n"

    startIndex = outPutFile.index(param) + len(param)

    dataToInsert = residual_df.to_string(index=False, header=None)+'\n'

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)


def SETS(outPutFile, technology, fuel, years, yearsplit):

    dataToInsert = ""
    print("FUEL SET", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "set FUEL := "
    startIndex = outPutFile.index(param) + len(param)
    
    fuel_s = fuel['Fuel']
    fu = [x for x in fuel_s if str(x) != 'nan']
    s = ", ".join(map(str, fu))
    s_wo_comma = s.replace(",", "")
    dataToInsert = s_wo_comma

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    dataToInsert = ""
    print("Technology SET", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "set TECHNOLOGY := "
    startIndex = outPutFile.index(param) + len(param)

    technology_s = technology['Technology']
    tech = [x for x in technology_s if str(x) != 'nan']
    t = ", ".join(map(str, tech))
    t_wo_comma = t.replace(",", "")
    dataToInsert = t_wo_comma

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    dataToInsert = ""
    print("YEAR SET", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "set YEAR := "
    startIndex = outPutFile.index(param) + len(param)
    
    years_s = years.index.values.tolist()
    y = [x for x in years_s if str(x) != 'nan']
    year_list = ", ".join(map(str, y))
    year_list_wo_comma = year_list.replace(",", "")
    dataToInsert = year_list_wo_comma

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    dataToInsert = ""
    print("TIMESLICE SET", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "set TIMESLICE := "
    startIndex = outPutFile.index(param) + len(param)
    
    ts_s = yearsplit.Timeslice.values.tolist()
    ts = [x for x in ts_s if str(x) != 'nan']
    ts_list = ", ".join(map(str, ts))
    ts_list_wo_comma = ts_list.replace(",", "")
    dataToInsert = ts_list_wo_comma

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)

def yearsplit_generator(outPutFile, yearsplit):
    
    dataToInsert = ""
    print("Yearsplit", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param YearSplit default 0 :\n"
    startIndex = outPutFile.index(param) + len(param)

    yearsraw = list(yearsplit.columns.values)
    years = [int(i) for i in yearsraw[1:]]
    years.append(':=')
    #df = pd.DataFrame (years)
    yearsplit2 = pd.DataFrame(yearsplit) 
    yearsplit2.columns = years
    
    dataToInsert += yearsplit.to_string(index = False)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)

def capapacityofonetech(outPutFile, input_data, capitalcostkm, capacityofonetech, capacity):
    dataToInsert = ""
    print("CapacityofOneTechnologyUnit", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param CapacityOfOneTechnologyUnit default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)
    
    tech = [x for x in capitalcostkm['Technology'] if 'TRHV' in x]

    if ((tech) and (capacityofonetech == 1)):
        for t in tech:
            year = int(input_data['startyear'][0])
            while year <= int(input_data['endyear'][0]):
                dataToInsert += "%s\t%s\t%i\t%i\n" % (input_data['region'][0],t, year,capacity.loc[0]['HV_capacity'])
                year += 1
    else:
        dataToInsert = ''

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)

def resultspath(outPutFile, spatial, demand_scenario, discountrate_scenario):

    dataToInsert = ""
    print("Resultspath", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param ResultsPath :="
    startIndex = outPutFile.index(param) + len(param)
    
    dataToInsert = " 'Benin%i%i%i'" %(spatial, demand_scenario, discountrate_scenario)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)


def operational_life(outPutFile, input_data, operational_life):
    """
    builds the OperationalLife (Region, Technology, OperationalLife)
    -------------
    Arguments
    outPutFile, GIS_data, input_data, operational_life
        outPutFile: is a string containing the OSeMOSYS parameters file
        GIS_data: is the location specific data which is used to iterate the data
        input_data: contains meta data such as region, start year, end year, months, timeslices
        OperationalLife: is the operational life per technology
    """
    dataToInsert = ""
    print("Operational life", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param OperationalLife default 1 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    for m, line in operational_life.iterrows():
        t = line['Technology']
        l = line['Life']
        dataToInsert += "%s\t%s\t%i\n" % (input_data['region'][0],t, l)
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)

def discountrate_(outPutFile, discountr):

    dataToInsert = ""
    print("Discount rate", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param DiscountRate default"
    startIndex = outPutFile.index(param) + len(param)

    dataToInsert = " %f :=" %(discountr)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)

def peakdemand(outPutFile,input_data, peakdemand):
    dataToInsert = ""
    print("Peak demand", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param Peakdemand default 9999999999999:=\n"
    startIndex = outPutFile.index(param) + len(param)

    peakdemand.index = peakdemand['Fuel']
    demand = peakdemand.drop(columns=['Fuel'])
    for j, row in demand.iterrows():
        year = demand.columns
        for k in year: #year is an object so I cannot match it with a number (e.g. startyear)
            demandForThisYearAndlocation = demand.loc[j][k]
            dataToInsert += "%s\t%s\t%s\t%s\n" % (input_data['region'][0], j, k, demandForThisYearAndlocation)
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def maxkm(outPutFile,input_data, distributionlines, distributioncelllength, HV):
    dataToInsert = ""
    print("Max km Distribution", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param MaxKmPerTech default 9999999999999 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    distributionlines = distributionlines.set_index(distributionlines.iloc[:, 0])
    distribution = distributionlines.drop(columns ='id')

    distributioncelllength.index = distributioncelllength['id']
    distribtionlength = distributioncelllength.drop(['Unnamed: 0', 'id', 'elec'], axis = 1)

    distribution_total = distribution.multiply(distribtionlength.LV_km, axis = "rows")

    for j, row in distribution_total.iterrows():
        km = distribution_total.loc[j]['sum']
        if math.isnan(km):
            km=0
        
        year = int(input_data['startyear'][0])
        while year <= int(input_data['endyear'][0]):
            dataToInsert += "%s\tTRLV_%i_0\t%i\t%f\n" % (input_data['region'][0],j, year, km)
            if HV['id'].eq(j).any():
                dataToInsert += "%s\tTRLVM_%i_0\t%i\t%f\n" % (input_data['region'][0], j, year, km)
            year += 1
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def capitalcostkmkW(outPutFile,input_data, capitalcost_distributionlines, peakdemand):
    dataToInsert = ""
    print("Captialcost per km", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param CapitalCostkmkW default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    peakdemand.index = peakdemand['Fuel']
    demand = peakdemand.drop(columns=['Fuel'])
    for j, row in peakdemand.iterrows():
        year = demand.columns
        for k in year:
            dataToInsert += "%s\t%s\t%s\t%f\n" % (input_data['region'][0], j, k, capitalcost_distributionlines)
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def fixedcost(df, outPutFile, input_data, fixed_cost):
    """
    Builds the Fixed cost (Region, Technology, Year, Fixed cost)
    -------------
    Arguments
    df, outPutFile, input_data, fixed_cost
        outPutFile: is a string containing the OSeMOSYS parameters file
        df: is the location specific data which is used to iterate the data
        input_data: contains meta data such as region, start year, end year, months, timeslices
        fixed_cost: is the fixed cost per technology
    """
    print("Fixed cost", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    dataToInsert = ""
    param = "param FixedCost default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    #for i, row in df.iterrows():
       #location = row['Location']

    for m, line in fixed_cost.iterrows():
        t = line['Technology']
        fc = line['Fixed Cost']
        year = int(input_data['startyear'][0])
        while year <= int(input_data['endyear'][0]):
            dataToInsert += "%s\t%s\t%i\t%f\n" % (input_data['region'][0], t,year, fc)
            year += 1
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def emissionactivity(df, outPutFile, input_data, emissions):
    """
    Builds the Emission activity (Region, Technology, Emissiontype, Technology, ModeofOperation, Year, emissionactivity)
    -------------
    Arguments
    df, outPutFile, input_data, emissions
        outPutFile: is a string containing the OSeMOSYS parameters file
        df: is the location specific data which is used to iterate the data
        input_data: contains meta data such as region, start year, end year, months, timeslices
        emissions: is the emissionactivity per technology and mode of operation
    """
    print("Emission activity", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    dataToInsert = ""
    param = "param EmissionActivityRatio default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)
    for i, row in df.iterrows():
       location = row['Location']
       for m, line in emissions.iterrows():
           year = int(input_data['startyear'][0])
           t = line['Technology']
           k = line['Modeofoperation']
           CO2 = line['CO2']
           while year <=  int(input_data['endyear'][0]):
               dataToInsert += "%s\t%s_%i\tCO2\t%i\t%i\t%f\n" % (input_data['region'][0], t, location, k, year, CO2)
               year += 1
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return (outPutFile)

def variblecost(df, outPutFile, input_data, variable_cost, gas, diesel, coal, heavyfueloil):
    """
    Builds the Variable cost (Region, Technology, ModeofOperation, Year, Variablecost)
    -------------
    Arguments
    df, outPutFile, input_data, variable_cost
        outPutFile: is a string containing the OSeMOSYS parameters file
        df: is the location specific data which is used to iterate the data
        input_data: contains meta data such as region, start year, end year, months, timeslices
        variable_cost: is the variable cost per technology and mode of operation
    """

    print("Variable cost", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    dataToInsert = ""
    param = "param VariableCost default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    for m,line in variable_cost.iterrows():
        year = int(input_data['startyear'][0])
        while year <= int(input_data['endyear'][0]):
            t = line['Technology']
            vc = line['Variable Cost']
            modeofop = line['ModeofOperation']
            dataToInsert += "%s\t%s\t%i\t%i\t%f\n" % (input_data['region'][0], t, modeofop, year, vc)
            year += 1

    for i in coal.index:
        coal_price = coal.loc[i][0]
        dataToInsert += "%s\t%s\t%i\t%s\t%f\n" % (input_data['region'][0], 'COAL_IMP', 1,i, coal_price)

    for i in gas.index:
        gas_price = gas.loc[i][0]
        dataToInsert += "%s\t%s\t%i\t%s\t%f\n" % (input_data['region'][0], 'GAS_IMP', 1,i, gas_price)
    
    for i in range(len(diesel.columns)):
        diesel_price = diesel.iloc[0,i]
        dataToInsert += "%s\t%s\t%i\t%s\t%f\n" % (input_data['region'][0], 'DIESEL_IMP', 1,diesel.columns[i], diesel_price)
    
    for i in range(len(heavyfueloil.columns)):
        heavyfueloil_price = heavyfueloil.iloc[0,i]
        dataToInsert += "%s\t%s\t%i\t%s\t%f\n" % (input_data['region'][0], 'HeavyFueloil_IMP', 1,heavyfueloil.columns[i], heavyfueloil_price)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def totaltechnologyannualactivityupperlimit(df,outPutFile, input_data, totalannuallimit):
    """
    Builds the TotalTechnologyAnnualActivityUpperLimit (Region, Technology, Year, TotalTechnologyUpperLimit)
    -------------
    Arguments
    df,outPutFile, input_data, totalannuallimit
        outPutFile: is a string containing the OSeMOSYS parameters file
        df: is the location specific data which is used to iterate for the location specific the data
        input_data: contains meta data such as region, start year, end year, months, timeslices
        totalannuallimit: is the total annual limit per technology
    """

    print("TotalTechnologyAnnualActivityUpperLimit", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    dataToInsert = ""
    param = "param TotalTechnologyAnnualActivityUpperLimit default -1 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    for index, row in df.iterrows():
       location = row['Location']
       year = int(input_data['startyear'][0])
       while year <= int(input_data['endyear'][0]):
           for m, line in totalannuallimit.iterrows():
               tech = line['Technology']
               cf = line[location]
               dataToInsert += "%s\t%s_%i\t%i\t%f\n" % (input_data['region'][0], tech, location, year, cf)
           year = year + 1
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def inputact(outPutFile, inputactivity, input_data, country):
    """
    Builds the InputactivityRatio (Region, Technology, Fuel, Modeofoperation, Year, InputactivityRatio)
    -------------
    Arguments
    outPutFile, inputactivity, input_data
        outPutFile: is a string containing the OSeMOSYS parameters file
        input_data: contains meta data such as region, start year, end year, months, timeslices
        inputactivity: is the inputactivity per fuel and technology
    """
    dataToInsert = ""
    print("Input activity", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    if country == 'Benin':
        param = "param InputActivityRatio default 0 :=\n"
    else:
        param = 'Kenya KEEL00t00 KEEL1 1 2040 1.000000\n'
    startIndex = outPutFile.index(param) + len(param)

    for j, row in inputactivity.iterrows():
       technology = row['Technology']
       fuel = row['Fuel']
       inputactivityratio = row['Inputactivity']
       modeofoperation = row['ModeofOperation']
       year = int(input_data['startyear'][0])
       dataToInsert += "\n[%s,%s,%s,*,*]:\n" % (
       input_data['region'][0], technology, fuel)
       while year<=int(input_data['endyear'][0]):
           dataToInsert += "%i\t" % (year)
           year = year + 1

       year = int(input_data['startyear'][0])
       dataToInsert += ":=\n%i\t" % (modeofoperation)
       while year <= int(input_data['endyear'][0]):
           dataToInsert += "%f\t" % (inputactivityratio)

           year = year + 1
    outPutFile = outPutFile[:startIndex] + dataToInsert +"\n" + outPutFile[startIndex:]
    return (outPutFile)

def SpecifiedDemandProfile(outPutFile, demandprofile, input_data, demand, years):
    """
    Builds the SpecifiedDemandProfile (Region, Fuel, Timeslice, Year, SpecifiedDemandProfile)
    -------------
    Arguments
    outPutFile, demandprofile,input_data
        outPutFile: is a string containing the OSeMOSYS parameters file
        input_data: contains meta data such as region, start year, end year, months, timeslices
        demandprofile: is the demandprofile per fuel and technology
    """
    dataToInsert = ""
    print("SpecifiedDemandProfile", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    param = "param SpecifiedDemandProfile default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)


    demandprofile.index= demandprofile['TimeSlice']
    demandprofile = demandprofile.drop(columns=['TimeSlice'])
    #Take out each column and feed into the next step
    for i in range(0, len(demandprofile.columns)):
        slicearray = demandprofile.iloc[:,i]
        current_dataframe = slicearray.name
        assert 0.99<sum(slicearray.values)<1.017

        df = pd.DataFrame.from_dict([slicearray])
        df= df.T
        df.index.names = ['Timeslice']
        multiple = pd.concat([df.T]*(len(years))).T
        multiple.columns = years
        multiple.index = df.index

        if current_dataframe == 'Load_Rural':
            fuels = demand['Fuel']
            demand_fuels = [x for x in fuels if str(x) != 'nan']
            d = multiple.columns[1:]
            for i in demand_fuels:
                start, mid, end = i.split('_')
                if end == '0':
                    for index in range(len(multiple)):
                        timeslice = multiple.index[index]
                        for j in d:
                            demand_value = multiple.loc[timeslice][j]
                            dataToInsert += "%s\t%s\t%s\t%s\t%f\n" % (input_data['region'][0], i, timeslice, j, demand_value)
        if current_dataframe == 'Load_Central':
            fuels = demand['Fuel']
            demand_fuels = [x for x in fuels if str(x) != 'nan']
            d = multiple.columns[1:]
            for i in demand_fuels:
                start, mid, end = i.split('_')
                if end == '1':
                    for index in range(len(multiple)):
                        timeslice = multiple.index[index]
                        for j in d:
                            value = multiple.loc[timeslice][j]
                            dataToInsert += "%s\t%s\t%s\t%s\t%f\n" % (input_data['region'][0], i, timeslice, j, value)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def capacityfactor(outPutFile, GIS_file, timedependent_df, input_data,  elec, un_elec,years):
    """
    builds the Capacityfactor(Region, Technolgy, Timeslice, Year, CapacityFactor)
    This method is for capacityfactor which does not use storage equations but still model batteries
    -------------
    Arguments
    outPutFile, df, capacityfactor_solar, input_data, capitalcost_RET
        outPutFile: is a string containing the OSeMOSYS parameters file
        df: is the location specific data which is used to iterate the data
        battery: contains meta data on technologies that you want ot model batteries, time and capacityfactor
        input_data: contains meta data such as region, start year, end year, months, timeslices
        capitalcost_RET: --
    """
    print("Capacity factor", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param CapacityFactor default 1 :=\n"
    startIndex = outPutFile.index(param) + len(param)
    dataToInsert = ""

    timedependent_df = timedependent_df.drop(columns=['Load_Central','Load_Rural'])
    timedependent_df.index= timedependent_df['TimeSlice']
    timedependent_df = timedependent_df.drop(columns=['TimeSlice'])
    for i in range(0, len(timedependent_df.columns)):
        slicearray = timedependent_df.iloc[:,i]
        current_dataframe = slicearray.name
        
        assert 0.99<sum(slicearray.values)<1.017

        df = pd.DataFrame.from_dict([slicearray])
        df= df.T
        df.index.names = ['Timeslice']
        multiple = pd.concat([df.T]*(len(years))).T
        multiple.columns = years
        multiple.index = df.index

        if 'PV' in current_dataframe:
            for index in range(len(multiple)):
                timeslice = multiple.index[index]
                d = multiple.columns[1:]
                if elec['id'].eq(GIS_file['Location']).any():
                    for j in d:
                        value = multiple.loc[timeslice][j]
                        dataToInsert += "%s\t%s_1\t%s\t%s\t%f\n" % (input_data['region'][0], current_dataframe, timeslice, j, value)
                        dataToInsert += "%s\t%s_0\t%s\t%s\t%f\n" % (input_data['region'][0], current_dataframe, timeslice, j, value)
                else:
                    for j in d:
                        value = multiple.loc[timeslice][j]
                        dataToInsert += "%s\t%s_0\t%s\t%s\t%f\n" % (input_data['region'][0], current_dataframe, timeslice, j, value)

        else:
            for index in range(len(multiple)):
                timeslice = multiple.index[index]
                d = multiple.columns[1:]
                for j in d:
                    value = multiple.loc[timeslice][j]
                    dataToInsert += "%s\t%s\t%s\t%s\t%f\n" % (input_data['region'][0],current_dataframe, timeslice, j, value)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return (outPutFile)

def outputactivity(outPutFile, outputactivity, input_data):
    """
    builds the Outputactivity(Region, Technology, Fuel, ModeofOperation, Year, outputactivity)
    -------------
    Arguments
    outPutFile, outputactivity, input_data
        outPutFile: is a string containing the OSeMOSYS parameters file
        outputactivity: The outputactivity between the Technology and Fuel
        input_data: contains meta data such as region, start year, end year, months, timeslices
    """
    dataToInsert = ""
    print("Outputactivity", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param OutputActivityRatio default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    for j, row in outputactivity.iterrows():
       technology = row['Technology']
       fuel = row['Fuel']
       outputactivityratio = row['Outputactivity']
       modeofoperation = row['ModeofOperation']
       year = int(input_data['startyear'][0])
       dataToInsert += "\n[%s,%s,%s,*,*]:\n" % (
       input_data['region'][0], technology, fuel)
       while year<=int(input_data['endyear'][0]):
           dataToInsert += "%i\t" % (year)
           year = year + 1

       year = int(input_data['startyear'][0])
       dataToInsert += ":=\n%i\t" % (modeofoperation)
       while year <= int(input_data['endyear'][0]):
           dataToInsert += "%f\t" % (outputactivityratio)

           year = year + 1

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def specifiedannualdemand(outPutFile, demand, input_data):
    """
    builds the SpecifiedAnnualDemand (Region, Fuel, Year, Demand)
    -------------
    Arguments
    outPutFile, demand, input_data
        outPutFile: is a string containing the OSeMOSYS parameters file
        demand: The demand per Fuel
        input_data: contains meta data such as region, start year, end year, months, timeslices
    """
    print("SpecifiedAnnualDemand", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param SpecifiedAnnualDemand default 0 :=\n"
    dataToInsert = ""
    startIndex = outPutFile.index(param) + len(param)
    demand.index = demand['Fuel']
    demand = demand.drop(columns=['Fuel'])
    for j, row in demand.iterrows():
        year = demand.columns
        for k in year: #year is an object so I cannot match it with a number (e.g. startyear)
            demandForThisYearAndlocation = demand.loc[j][k]
            dataToInsert += "%s\t%s\t%s\t%f\n" % (input_data['region'][0], j, k, demandForThisYearAndlocation)
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def capitalcost_dynamic(df, outPutFile, CapitalCost_PV, CapitalCost_batt, CapitalCost_WI, COMBatt_df, SOMG_df, input_data, elec, un_elec, PV_sizing_rural, PVsizing_urban):
    """
    builds the Capitalcost (Region, Technology, Year, CapitalCost) where the cost is dynamic. Here when the capacity factor vary for Wind
    -------------
    Arguments
    df, outPutFile, capitalcost_RET, capacityfactor_wind, capacityfactor_solar, input_data
        df: is the location specific data which is used to iterate the data
        outPutFile: is a string containing the OSeMOSYS parameters file
        capitalcost_RET: The capital cost per technology and capacity factor
        capacityfactor_wind: Hourly 8760 wind CF file for each location
        capacityfactor_solar: Hourly 8760 solar CF file for each location
        input_data: contains meta data such as region, start year, end year, months, timeslices
    """
    dataToInsert = ""
    print("Capital cost dynamic cost", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param CapitalCost default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    #Section the different technology types per CF and OSeMOSYS name

    PV_sizing_rural.index = PV_sizing_rural['location']
    PVsizing_urban.index = PVsizing_urban['location']

    for k, row in df.iterrows():
        location = str(row['Location'])
        # Wind
        for k in CapitalCost_WI.index:  # year is an object so I cannot match it with a number (e.g. startyear)
            windcapitalcost = CapitalCost_WI.loc[k][0]
            dataToInsert += "%s\t%s_%s\t%s\t%f\n" % (input_data['region'][0], "WI", location, k, windcapitalcost)
        
        for k in range(len(CapitalCost_PV.columns)):  # year is an object so I cannot match it with a number (e.g. startyear)
            pvcapitalcost = CapitalCost_PV.iloc[0,k]
            batterycost = CapitalCost_batt.iloc[0,k]
            MGPVcost = SOMG_df.iloc[0,k]
            COMBatterycost = COMBatt_df.iloc[0,k]
            sopvcapitalcostbatt_rural = pvcapitalcost*PV_sizing_rural.loc[row['Location']]['PV_size']+ batterycost*PV_sizing_rural.loc[row['Location']]['Battery_hours'] + 1339 #average constant+ capacity from ATB2022
            sopvcapitalcostbatt_urban = pvcapitalcost*PVsizing_urban.loc[row['Location']]['PV_size']+ batterycost*PVsizing_urban.loc[row['Location']]['Battery_hours'] + 1339 #average constant +capacity from ATB2022
            soMGcapitalcostbatt_rural = MGPVcost*PV_sizing_rural.loc[row['Location']]['PV_size']+ COMBatterycost*PV_sizing_rural.loc[row['Location']]['Battery_hours'] + 717 #average constant+ capacity from ATB2022
            soMGcapitalcostbatt_urban = MGPVcost*PVsizing_urban.loc[row['Location']]['PV_size']+ COMBatterycost*PVsizing_urban.loc[row['Location']]['Battery_hours'] + 717 #average constant +capacity from ATB2022
           
            if elec['id'].eq(row['Location']).any():
                dataToInsert += ("%s\t%s_%s_1\t%s\t%f\n" % (input_data['region'][0], "SOPV", location, CapitalCost_PV.columns[k], pvcapitalcost))
                dataToInsert += ("%s\t%s_%s_0\t%s\t%f\n" % (input_data['region'][0], "SOPV", location, CapitalCost_PV.columns[k], pvcapitalcost))
                dataToInsert += ("%s\t%s_%s_1\t%s\t%f\n" % (input_data['region'][0], "SOPVBattery", location, CapitalCost_PV.columns[k], sopvcapitalcostbatt_urban))
                dataToInsert += ("%s\t%s_%s_0\t%s\t%f\n" % (input_data['region'][0], "SOPVBattery", location, CapitalCost_PV.columns[k], sopvcapitalcostbatt_rural))
            if un_elec['id'].eq(row['Location']).any():
                dataToInsert += ("%s\t%s_%s_0\t%s\t%f\n" % (input_data['region'][0], "SOPV", location, CapitalCost_PV.columns[k], pvcapitalcost))
                dataToInsert += ("%s\t%s_%s_0\t%s\t%f\n" % (input_data['region'][0], "SOPVBattery", location, CapitalCost_PV.columns[k], sopvcapitalcostbatt_rural))
            dataToInsert += "%s\t%s_%s\t%s\t%f\n" % (input_data['region'][0], "SOMGBattery", location, CapitalCost_PV.columns[k], soMGcapitalcostbatt_rural)
    
    for k in CapitalCost_WI.index:  # year is an object so I cannot match it with a number (e.g. startyear)
        windcapitalcost = CapitalCost_WI.loc[k][0]
        dataToInsert += "%s\t%s\t%s\t%f\n" % (input_data['region'][0], 'WI31ph', k, windcapitalcost)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def capitalcost(outPutFile, trade_cost, input_data):
    """
    builds the Capital cost (region,technology,year,capitalcost)
    -------------
    Arguments
    outPutFile, outputactivity, trade_cost
        outPutFile: is a string containing the OSeMOSYS parameters file
        trade_cost: The Capitalcost which is not dynamic
        input_data: contains meta data such as region, start year, end year, months, timeslices
    """
    dataToInsert = ""

    print("Capital cost", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param CapitalCost default 0 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    for m, row in trade_cost.iterrows():
        if row['Technology'] != "":
            cost = row['Capitalcost']
            tech = row['Technology']
            year = int(input_data['startyear'][0])
            while year <= int(input_data['endyear'][0]):
                dataToInsert += "%s\t%s\t%i\t%f\n" % (input_data['region'][0], tech, year, cost)
                year += 1

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def capacitytoactivity(trade, outPutFile, input_data):
    """
    builds the CapacityToActivityUnit (Region, Technology, CapacitytoActivityUnit)
    -------------
    Arguments
    trade, outPutFile, input_data
        outPutFile: is a string containing the OSeMOSYS parameters file
        trade: The capacitytoactivity for each technology
        input_data: contains meta data such as region, start year, end year, months, timeslices
    """
    dataToInsert = ""
    print("Capacity to activity", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param CapacityToActivityUnit default 1 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    for m, row in trade.iterrows():
       capact = row['Capacitytoactivity']
       t = row['1']
       dataToInsert += "%s\t%s\t%f\n" % (input_data['region'][0],t ,capact)

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def write_to_file(file_object, outPutFile, comb):
    """
    write_to_file writes the string outPutFile to file_object
    -------------
    Arguments
    file_object, outPutFile
        file_object: is the path and name of the file where the datafile will be saved
        outPutFile:  is the accumulated final string of all data
    """
###############################################################
# write all to file
#########################################################
    print("write to file", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    with open(file_object + comb, "w") as actualOutputFile:
       actualOutputFile.truncate(0) #empty the file
       actualOutputFile.write(outPutFile)