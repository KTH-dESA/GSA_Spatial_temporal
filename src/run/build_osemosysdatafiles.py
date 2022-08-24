import pandas as pd
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime

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

def functions_to_run(dict_df, outPutFile,spatial, demand_scenario, discountrate_scenario):
    """Runs all the functions for the different parameters

    Arguments
    ---------
    dict_df, outPutFile
        dict_df: is a dictionary which contains all the csv files as dataframes from load_csv. Key is the name of the csv file
        outPutFile: is a string with the empty OSeMOSYS parameters file from make_outputfile
    """
    if 'operational_life' in dict_df:
        outPutFile = operational_life(outPutFile, dict_df['input_data'], dict_df['operational_life'])
    else:
        print('No operational_life file')
#######################################################################
    if '%i_%i_peakdemand' %(spatial, demand_scenario) in dict_df:
        outPutFile = peakdemand(outPutFile, dict_df['input_data'], dict_df['%i_%i_peakdemand' %(spatial, demand_scenario)])
    else:
        print('No peakdemand file')
#################################################################################
    if '%i_distributionlines' %(spatial) in dict_df:
        outPutFile = maxkm(outPutFile, dict_df['input_data'], dict_df['%i_distributionlines' %(spatial) ], dict_df['%i_distribution' %(spatial)], dict_df['%i_elec' %(spatial)])
    else:
        print('No distributionlines file')
########################################################################################################
    if 'capitalcostkm' in dict_df:
        outPutFile = capitalcostkmkW(outPutFile, dict_df['input_data'], dict_df['capitalcostkm'], dict_df['%i_%i_peakdemand'%(spatial, demand_scenario)])

    else:
        print('No distributionlines file')
############################################################################################################

    if '%i_fixed_cost' %(spatial) in dict_df:
            outPutFile = fixedcost(dict_df['%i_GIS_data' %(spatial)], outPutFile, dict_df['input_data'], dict_df['fixed_cost_ref'])
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
    if 'capitalcost_RET_ref' in dict_df:
            outPutFile = capitalcost_dynamic(dict_df['%i_GIS_data' %(spatial)], outPutFile, dict_df['capitalcost_RET_ref'],
                                         dict_df['%i_capacityfactor_wind' %(spatial)], dict_df['%i_capacityfactor_solar' %(spatial)],
                                         dict_df['input_data'],dict_df['%i_elec' %(spatial)],dict_df['%i_un_elec' %(spatial)], dict_df['battery'])
    else:
        print('No capitalcost_RET file')
###########################################################################
    if '%i_capitalcost'%(spatial) in dict_df:
        outPutFile = capitalcost(outPutFile, dict_df['%i_capitalcost'%(spatial)], dict_df['input_data'])
    else:
        print('No capitalcost file')

# ################################################################################

    if '%i_capacitytoactivity' %(spatial) in dict_df:
        outPutFile = capacitytoactivity(dict_df['%i_capacitytoactivity'%(spatial)], outPutFile, dict_df['input_data'])
    else:
        print('No capacitytoactivity file')
#################################################################################
    if 'demandprofile' in dict_df:
       outPutFile = SpecifiedDemandProfile(outPutFile, dict_df['demandprofile'], dict_df['demandprofile_rural'],
                                            dict_df['input_data'], dict_df['%i_demand'%(demand_scenario)])
    else:
        print('No demandprofile file')
###########################################################
###################### Mode of operation parameters######################

    if 'emissions' in dict_df:
        outPutFile = emissionactivity(dict_df['%i_GIS_data'%(spatial)], outPutFile, dict_df['input_data'], dict_df['emissions'])
    else:
        print('No emissions file')
########################################################
    if 'variable_cost' in dict_df:
        outPutFile = variblecost(dict_df['%i_GIS_data' %(spatial)], outPutFile, dict_df['input_data'], dict_df['variable_cost'])
    else:
        print('No variable_cost file')
#############################################################
    if '%i_inputactivity'%(spatial) in dict_df:
        outPutFile = inputact(outPutFile, dict_df['%i_inputactivity'%(spatial)], dict_df['input_data'])
    else:
        print('No inputactivity file')

################################################################
    if '%i_outputactivity'%(spatial) in dict_df:
        outPutFile = outputactivity(outPutFile, dict_df['%i_outputactivity'%(spatial)], dict_df['input_data'])
    else:
        print('No outputactivity file')
################################################################

    if '%i_discountrate' %(discountrate_scenario) in dict_df:
        outPutFile = discountrate_(outPutFile, dict_df['%i_discountrate' %(discountrate_scenario)])
    else:
        print('No discountrate file')

    ################################################################

    if ('%i_capacityfactor_solar' %(spatial) or '%i_capacityfactor_wind'%(spatial)) in dict_df.keys():
       outPutFile = capacityfactor(outPutFile, dict_df['%i_GIS_data'%(spatial)], dict_df['battery'], dict_df['input_data'],
                                  dict_df['%i_capacityfactor_wind'%(spatial)], dict_df['%i_capacityfactor_solar'%(spatial)], dict_df['%i_elec'%(spatial)], dict_df['%i_un_elec'%(spatial)])
    else:
       print('No capacityfactor_solar or capacityfactor_wind file')
    ###############################################################################

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

    #for i, row in GIS_data.iterrows():
        #location = row['Location']
    for m, line in operational_life.iterrows():
        t = line['Technology']
        l = line['Life']
        dataToInsert += "%s\t%s\t%i\n" % (input_data['region'][0],t, l)
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)

def discountrate_(outPutFile, discountr):

    dataToInsert = ""
    print("Operational life", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param DiscountRate default 0.12 :=\n"
    startIndex = outPutFile.index(param)

    #for i, row in GIS_data.iterrows():
        #location = row['Location']
    dataToInsert = "param DiscountRate default %i :=" %(discountr['Discountrate'])

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]

    return(outPutFile)


def peakdemand(outPutFile,input_data, peakdemand):
    dataToInsert = ""
    print("Peak demand", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param Peakdemand default 999999999:=\n"
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

def maxkm(outPutFile,input_data, distributionlines, distributioncelllength, elec):
    dataToInsert = ""
    print("Max km Distribution", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    param = "param MaxKmPerTech default 9999999999 :=\n"
    startIndex = outPutFile.index(param) + len(param)

    distributionlines = distributionlines.set_index(distributionlines.iloc[:, 0])
    distribution = distributionlines.drop(columns ='Unnamed: 0')

    distributioncelllength.index = distributioncelllength['index_right']
    distribtionlength = distributioncelllength.drop(['Unnamed: 0', 'index_right', 'elec'], axis = 1)

    distribution_total = distribution.multiply(distribtionlength.LV_km, axis = "rows")

    for j, row in distribution_total.iterrows():
        km = distribution_total.loc[j]['sum']
        year = int(input_data['startyear'][0])
        while year <= int(input_data['endyear'][0]):
            dataToInsert += "%s\tTRLV_%i_0\t%i\t%f\n" % (input_data['region'][0],j, year, km)
            if elec['index_right'].eq(j).any():
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
            dataToInsert += "%s\t%s\t%s\t%f\n" % (input_data['region'][0], j, k, capitalcost_distributionlines.loc[0]['LV_cost'])
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
           NOx = line['NOx']
           while year <=  int(input_data['endyear'][0]):
               dataToInsert += "%s\t%s_%i\tCO2\t%i\t%i\t%f\n" % (input_data['region'][0], t, location, k, year, CO2)
               dataToInsert += "%s\t%s_%i\tNOX\t%i\t%i\t%f\n" % (input_data['region'][0], t, location, k, year, NOx)
               year += 1
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return (outPutFile)

def variblecost(df, outPutFile, input_data, variable_cost):
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
    param = "param TotalTechnologyAnnualActivityUpperLimit default 99999999999 :=\n"
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

def inputact(outPutFile, inputactivity, input_data):
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
    param = "param InputActivityRatio default 0 :=\n"
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
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return (outPutFile)

def SpecifiedDemandProfile(outPutFile, demandprofile, demandprofile_rural, input_data, demand):
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

    fuels = demand['Fuel']
    demand_fuels = [x for x in fuels if str(x) != 'nan']
    d = demandprofile.columns[1:]
    dr = demandprofile_rural.columns[1:]
    demandprofile.index = demandprofile['Timeslice']
    demandprofile_rural.index = demandprofile_rural['Timeslice']
    for i in demand_fuels:
        start, mid, end = i.split('_')
        if end == '1':
            for k, line in demandprofile.iterrows():
                timeslice = line['Timeslice']
                for j in d:
                    demand_value = demandprofile.loc[timeslice][j]
                    dataToInsert += "%s\t%s\t%s\t%s\t%f\n" % (input_data['region'][0], i, timeslice, j, demand_value)
        else:
            for k, line in demandprofile_rural.iterrows():
                timeslice = line['Timeslice']
                for j in dr:
                    value = demandprofile_rural.loc[timeslice][j]
                    dataToInsert += "%s\t%s\t%s\t%s\t%f\n" % (input_data['region'][0], i, timeslice, j, value)
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return(outPutFile)

def capacityfactor(outPutFile, df, battery, input_data, capacityfactor_wind, capacityfactor_solar, elec, un_elec):
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

    #read input data
    def convert(value):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value
    # month = (input_data['Month']) #the list includes Nan
    # mon = [convert(value) for value in month]
    # mont = [x for x in mon if str(x) != 'nan']
    # months = ['{:02d}'.format(x) for x in mont] #padding numbers
    timeslice_ = input_data['Timeslice']
    timeslice = [x for x in timeslice_ if str(x) != 'nan']

    timeslicemonthstart_ = input_data['Timeslicemonthstart']
    timeslicemonthstart_ = [convert(value) for value in timeslicemonthstart_]
    timeslicemonthstart = [x for x in timeslicemonthstart_ if str(x) != 'nan']
    #timeslicemonthstart = ['{:02d}'.format(x) for x in timeslicemonthstart]

    timeslicemonthend_ = input_data['Timeslicemonthend']
    timeslicemonthend_ = [convert(value) for value in timeslicemonthend_]
    timeslicemonthend = [x for x in timeslicemonthend_ if str(x) != 'nan']
    #timeslicemonthend = ['{:02d}'.format(x) for x in timeslicemonthend]

    daysplit_ = input_data['Daysplit']
    daysplit = [x for x in daysplit_ if str(x) != 'nan']

    daysplitstart_ = input_data['Daysplitstart']
    daysplitstart = [x for x in daysplitstart_ if str(x) != 'nan']

    daysplitend_ = input_data['Daysplitend']
    daysplitend = [x for x in daysplitend_ if str(x) != 'nan']

    region = input_data['region'][0]
    startyear = input_data['startyear'][0]
    endyear = input_data['endyear'][0]
    type = input_data.groupby('renewable ninjafile')
    solar = type.get_group('capacityfactor_solar')
    solar_tech = solar['Tech']
    wind = type.get_group('capacityfactor_wind')
    wind_tech = wind['Tech']

    #deep copy renewable ninja data
    capacityfactor_solar_ = capacityfactor_solar.copy()
    capacityfactor_solar_p = pd.to_datetime(capacityfactor_solar_['adjtime'], errors='coerce', format='%Y/%m/%d %H:%M')
    capacityfactor_solar_.index = capacityfactor_solar_p
    capacityfactor_solar_pv = capacityfactor_solar_.drop(columns=['adjtime'])

    def calculate_average(data, startdate, enddate, sliceStart, sliceEnd, location):
        mask = (data.index > startdate) & (data.index <= enddate)
        thisMonthOnly = data.loc[mask]
        slice = sum(thisMonthOnly[(location)].between_time(sliceStart, sliceEnd))
        try:
            average = ((slice / len(thisMonthOnly.between_time(sliceStart, sliceEnd))))
        except ZeroDivisionError:
            average = 0
        return (average)

    #SolarPV
    for k, row in df.iterrows():
        location = str(row['Location'])
        year = startyear
        while year <= endyear:
            m = 0
            while m < len(timeslice):
                startDate = pd.to_datetime("2016-%s" % (timeslicemonthstart[m]))
                endDate = pd.to_datetime("2016-%s" % (timeslicemonthend[m]))
                average_solar_day = calculate_average(capacityfactor_solar_pv, startDate, endDate, daysplitstart[0], daysplitend[0], location)
                tsday = timeslice[m] + "_" + daysplit[0]
                average_solar_evening = calculate_average(capacityfactor_solar_pv, startDate, endDate, daysplitstart[1], daysplitend[1], location)
                tsevening = timeslice[m] + "_" + daysplit[1]
                average_solar_night = calculate_average(capacityfactor_solar_pv, startDate, endDate, daysplitstart[2], daysplitend[2], location)
                tsnight = timeslice[m] + "_" + daysplit[2]
                for t in solar_tech:
                    if t == 'SOPV':
                        if elec['index_right'].eq(row['Location']).any():
                            dataToInsert += "%s\t%s_%s_1\t%s\t%i\t%f\n" % (region, t, location, tsday, year, average_solar_day)
                            dataToInsert += "%s\t%s_%s_0\t%s\t%i\t%f\n" % (region, t, location, tsday, year, average_solar_day)
                            dataToInsert += "%s\t%s_%s_1\t%s\t%i\t%f\n" % (region, t, location, tsevening, year, average_solar_evening)
                            dataToInsert += "%s\t%s_%s_0\t%s\t%i\t%f\n" % (region, t, location, tsevening, year, average_solar_evening)
                            dataToInsert += "%s\t%s_%s_1\t%s\t%i\t%f\n" % (region, t, location, tsnight, year, average_solar_night)
                            dataToInsert += "%s\t%s_%s_0\t%s\t%i\t%f\n" % (region, t, location, tsnight, year, average_solar_night)
                        if un_elec['index_right'].eq(row['Location']).any():
                            dataToInsert += "%s\t%s_%s_0\t%s\t%i\t%f\n" % (region, t, location, tsday, year, average_solar_day)
                            dataToInsert += "%s\t%s_%s_0\t%s\t%i\t%f\n" % (region, t, location, tsevening, year, average_solar_evening)
                            dataToInsert += "%s\t%s_%s_0\t%s\t%i\t%f\n" % (region, t, location, tsnight, year, average_solar_night)
                    else:
                        dataToInsert += "%s\t%s_%s\t%s\t%i\t%f\n" % (region, t, location, tsday, year, average_solar_day)
                        dataToInsert += "%s\t%s_%s\t%s\t%i\t%f\n" % (region, t, location, tsevening, year, average_solar_evening)
                        dataToInsert += "%s\t%s_%s\t%s\t%i\t%f\n" % (region, t, location, tsnight, year, average_solar_night)
                m +=1
            year += 1
    if battery is None:
        pass
    else:

        tech = battery.groupby('renewable ninjafile')
        solar_battery = tech.get_group('capacityfactor_solar')
        for j, line in solar_battery.iterrows():
            capacityfactor_solar_batt = capacityfactor_solar.copy()  # deep copy
            for k, row in df.iterrows():
                location = str(row['Location'])
                batteryCapacityFactor = line['Batterycapacityfactor']
                batteryTime = line['BatteryTime']
                lastRowWasZero = False
                batteryConsumed = False
                index = 0
                for solarCapacity in capacityfactor_solar_batt[location].values:
                    currentRowIsZero = solarCapacity == 0
                    if not currentRowIsZero:
                       # This will happen when the current row is not zero. We should "reset" everything.
                       batteryTime = line['BatteryTime']
                       batteryCapacityFactor = line['Batterycapacityfactor']
                       batteryConsumed = False
                       lastRowWasZero = False
                    elif batteryTime == int(0):
                       # This will happen when the current value is 0, the last value was zero and there is no batterytime left.
                       batteryConsumed = True
                       batteryTime = line['BatteryTime']
                       batteryCapacityFactor = line['Batterycapacityfactor']
                    elif solarCapacity == 0 and lastRowWasZero and not batteryConsumed:
                       # This will happen when the last row was zero and the current row is 0.
                       capacityfactor_solar_batt.at[index, location] = batteryCapacityFactor
                       lastRowWasZero = True
                       batteryTime -= 1
                    elif not batteryConsumed:
                       # This will happen when the last row was not zero and the current row is 0. Same as above?
                       capacityfactor_solar_batt.at[index, location] = batteryCapacityFactor
                       lastRowWasZero = True
                       batteryTime -= 1
                    index += 1

                capacityfactor_solar_b = capacityfactor_solar_batt.copy()
                capacityfactor_solar_b.index = capacityfactor_solar_p
                capacityfactor_solar_battery = capacityfactor_solar_b.drop(columns=['adjtime'])

                year = startyear
                while year <= endyear:
                    m = 0
                    while m < len(timeslice):
                        startDate = pd.to_datetime("2016-%s" % (timeslicemonthstart[m]))
                        endDate = pd.to_datetime("2016-%s" % (timeslicemonthend[m]))
                        average_solar_day = calculate_average(capacityfactor_solar_battery,  startDate, endDate, daysplitstart[0], daysplitend[0], location)
                        tsday = timeslice[m] + "_" + daysplit[0]
                        average_solar_evening = calculate_average(capacityfactor_solar_battery,  startDate, endDate, daysplitstart[1], daysplitend[1], location)
                        tsevening = timeslice[m] + "_" + daysplit[1]
                        average_solar_night = calculate_average(capacityfactor_solar_battery,  startDate, endDate, daysplitstart[2], daysplitend[2], location)
                        tsnight = timeslice[m] + "_" + daysplit[2]

                        if line['Technology'] == 'SOPV':
                            if elec['index_right'].eq(row['Location']).any():
                                dataToInsert += "%s\t%s%ir_%s_1\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsday, year, average_solar_day)
                                dataToInsert += "%s\t%s%ir_%s_0\t%s\t%i\t%f\n" % (
                                region, line['Technology'], line['BatteryTime'], location, tsday, year, average_solar_day)
                                dataToInsert += "%s\t%s%ir_%s_1\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsevening, year, average_solar_evening)
                                dataToInsert += "%s\t%s%ir_%s_0\t%s\t%i\t%f\n" % (
                                region, line['Technology'], line['BatteryTime'], location, tsevening, year, average_solar_evening)
                                dataToInsert += "%s\t%s%ir_%s_1\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsnight, year, average_solar_night)
                                dataToInsert += "%s\t%s%ir_%s_0\t%s\t%i\t%f\n" % (
                                region, line['Technology'], line['BatteryTime'], location, tsnight, year, average_solar_night)
                            if un_elec['index_right'].eq(row['Location']).any():
                                dataToInsert += "%s\t%s%ir_%s_0\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsday, year, average_solar_day)
                                dataToInsert += "%s\t%s%ir_%s_0\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsevening, year, average_solar_evening)
                                dataToInsert += "%s\t%s%ir_%s_0\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsnight, year, average_solar_night)
                        else:
                            dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsday, year, average_solar_day)
                            dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsevening, year, average_solar_evening)
                            dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, tsnight, year, average_solar_night)
                        m +=1
                    year +=1

#WIND
    capacityfactor_windcop = capacityfactor_wind.copy()
    capacityfactor_windc = pd.to_datetime(capacityfactor_windcop['adjtime'], errors='coerce', format='%Y/%m/%d %H:%M')
    capacityfactor_windcop.index = capacityfactor_windc
    capacityfactor_windcopy = capacityfactor_windcop.drop(columns=['adjtime'])

    for k, row in df.iterrows():
        location = str(row['Location'])
        year = startyear
        while year <= endyear:
            m = 0
            while m < len(timeslice):
                startDate = pd.to_datetime("2016-%s" % (timeslicemonthstart[m]))
                endDate = pd.to_datetime("2016-%s" % (timeslicemonthend[m]))
                average_wind_day = calculate_average(capacityfactor_windcopy, startDate, endDate,
                                                      daysplitstart[0], daysplitend[0], location)
                tsday = timeslice[m] + "_" + daysplit[0]
                average_wind_evening = calculate_average(capacityfactor_windcopy, startDate, endDate,
                                                          daysplitstart[1], daysplitend[1], location)
                tsevening = timeslice[m] + "_" + daysplit[1]
                average_wind_night = calculate_average(capacityfactor_windcopy, startDate, endDate,
                                                        daysplitstart[2], daysplitend[2], location)
                tsnight = timeslice[m] + "_" + daysplit[2]

                for t in wind_tech:
                    dataToInsert += "%s\t%s_%s\t%s\t%i\t%f\n" % (region, t, location , tsday, year, average_wind_day)
                    dataToInsert += "%s\t%s_%s\t%s\t%i\t%f\n" % (region, t, location, tsevening, year, average_wind_evening)
                    dataToInsert += "%s\t%s_%s\t%s\t%i\t%f\n" % (region, t, location, tsnight, year, average_wind_night)
                m = m + 1
            year = year + 1
    # if battery is None:
    #     pass
    # else:
    #     tech = battery.groupby('renewable ninjafile')
    #     wind_battery = tech.get_group('capacityfactor_wind')
    #     for j, line in wind_battery.iterrows():
    #         capacityfactor_wind_batt = capacityfactor_wind.copy()  # deep copy
    #         for k, row in df.iterrows():
    #             location = str(row['Location'])
    #             batteryCapacityFactor = line['Batterycapacityfactor']
    #             batteryTime = line['BatteryTime']
    #             lastRowWasZero = False
    #             batteryConsumed = False
    #             index = 0
    #             for solarCapacity in capacityfactor_wind_batt[location].values:
    #                 currentRowIsZero = solarCapacity == 0
    #                 if not currentRowIsZero:
    #                    # This will happen when the current row is not zero. We should "reset" everything.
    #                    batteryTime = line['BatteryTime']
    #                    batteryCapacityFactor = line['Batterycapacityfactor']
    #                    batteryConsumed = False
    #                    lastRowWasZero = False
    #                 elif batteryTime == int(0):
    #                    # This will happen when the current value is 0, the last value was zero and there is no batterytime left.
    #                    batteryConsumed = True
    #                    batteryTime = line['BatteryTime']
    #                    batteryCapacityFactor = line['Batterycapacityfactor']
    #                 elif solarCapacity == 0 and lastRowWasZero and not batteryConsumed:
    #                    # This will happen when the last row was zero and the current row is 0.
    #                    capacityfactor_wind_batt.at[index, location] = batteryCapacityFactor
    #                    lastRowWasZero = True
    #                    batteryTime -= 1
    #                 elif not batteryConsumed:
    #                    # This will happen when the last row was not zero and the current row is 0. Same as above?
    #                    capacityfactor_wind_batt.at[index, location] = batteryCapacityFactor
    #                    lastRowWasZero = True
    #                    batteryTime -= 1
    #                 index += 1
    #
    #             capacityfactor_wind_b = capacityfactor_wind_batt.copy()
    #             capacityfactor_wind_b.index = capacityfactor_windc
    #             capacityfactor_wind_battery = capacityfactor_wind_b.drop(columns=['adjtime'])
    #
    #             year = startyear
    #             while year <= endyear:
    #                 m = 0
    #                 while m < 11:
    #                     currentMonth = months[m]
    #                     startDate = pd.to_datetime("2016-%s-01" % (currentMonth))
    #                     endDate = pd.to_datetime("2016-%s-01" % (months[m + 1]))
    #                     mask = (capacityfactor_wind_battery.index > startDate) & (capacityfactor_wind_battery.index <= endDate)
    #                     thisMonthOnly = capacityfactor_wind_battery.loc[mask]
    #                     sliceStart = timesliceDN
    #                     sliceEnd = timesliceDE
    #                     ts = "%iD" % (m + 1)
    #                     slice = sum(thisMonthOnly[(location)].between_time(sliceStart, sliceEnd))
    #                     try:
    #                        average_wind = ((slice / len(thisMonthOnly.between_time(sliceStart, sliceEnd)._values)))
    #                     except ZeroDivisionError:
    #                        average_wind = 0
    #                     dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, ts, year, average_wind)
    #
    #                     sliceStart = timesliceED
    #                     sliceEnd = timesliceEN
    #                     ts = "%iE" % (m + 1)
    #                     slice = sum(thisMonthOnly[(location)].between_time(sliceStart, sliceEnd))
    #                     try:
    #                        average_wind = (
    #                            (slice / len(thisMonthOnly.between_time(sliceStart, sliceEnd)._values)))
    #                     except ZeroDivisionError:
    #                        average_wind = 0
    #                     dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, ts, year, average_wind)
    #
    #                     sliceStart = timesliceNE
    #                     sliceEnd = timesliceND
    #                     ts = "%iN" % (m + 1)
    #                     slice = sum(thisMonthOnly[(location)].between_time(sliceStart, sliceEnd))
    #                     try:
    #                        average_wind = (
    #                            (slice / len(thisMonthOnly.between_time(sliceStart, sliceEnd)._values)))
    #                     except ZeroDivisionError:
    #                        average_wind = 0
    #                     dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, ts, year, average_wind)
    #                     m = m + 1
    #
    #                 while m == 11:
    #                     currentMonth = months[m]
    #                     startDate = pd.to_datetime("2016-%s-01" % (currentMonth))
    #                     endDate = pd.to_datetime("2016-%s-31" % (months[m]))
    #                     mask = (capacityfactor_wind_battery.index > startDate) & (capacityfactor_wind_battery.index <= endDate)
    #                     thisMonthOnly = capacityfactor_wind_battery.loc[mask]
    #
    #                     sliceStart = timesliceDN
    #                     sliceEnd = timesliceDE
    #                     ts = "%iD" % (m + 1)
    #                     slice = sum(thisMonthOnly[(location)].between_time(sliceStart, sliceEnd))
    #                     try:
    #                        average_wind = (
    #                            (slice / len(thisMonthOnly.between_time(sliceStart, sliceEnd)._values)))
    #                     except ZeroDivisionError:
    #                        average_wind = 0
    #                     dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, ts, year, average_wind)
    #
    #                     sliceStart = timesliceED
    #                     sliceEnd = timesliceEN
    #                     ts = "%iE" % (m + 1)
    #                     slice = sum(thisMonthOnly[(location)].between_time(sliceStart, sliceEnd))
    #                     try:
    #                        average_wind = (slice / len(thisMonthOnly.between_time(sliceStart, sliceEnd)._values))
    #                     except ZeroDivisionError:
    #                        average_wind = 0
    #                     dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, ts, year, average_wind)
    #
    #                     sliceStart = timesliceNE
    #                     sliceEnd = timesliceND
    #                     ts = "%iN" % (m + 1)
    #                     slice = sum(thisMonthOnly[(location)].between_time(sliceStart, sliceEnd))
    #                     try:
    #                        average_wind = (
    #                            (slice / len(thisMonthOnly.between_time(sliceStart, sliceEnd)._values)))
    #                     except ZeroDivisionError:
    #                        average_wind = 0
    #                     dataToInsert += "%s\t%s%ic_%s\t%s\t%i\t%f\n" % (region, line['Technology'], line['BatteryTime'], location, ts, year, average_wind)
    #                     m = m + 1
    #                 year = year + 1

    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return (outPutFile)

#dict_df =load_csvs("ref")
#outPutFile = make_outputfile("Kenya_basefile_reference.txt")
#outPutFile = capacityfactor(outPutFile, dict_df['GIS_data'], dict_df['battery'], dict_df['input_data'],
#                            dict_df['capacityfactor_wind'], dict_df['capacityfactor_solar'], dict_df['elec'],
#                            dict_df['un_elec'])


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

def capitalcost_dynamic(df, outPutFile, capitalcost_RET, capacityfactor_wind, capacityfactor_solar, input_data, elec, un_elec, battery):
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
    cf_tech = capitalcost_RET.groupby('Technology')
    wind_tech = cf_tech.get_group('Wind')
    wind_CF = wind_tech['CF']
    wind_tech_name = wind_tech.loc[0]['Technology_name_OSeMOSYS']
    comm_PV_tech =  cf_tech.get_group('Comm PV')
    comm_PV_CF = comm_PV_tech['CF']
    comm_PV_tech_name = comm_PV_tech.loc[1]['Technology_name_OSeMOSYS']
    pv_tech = cf_tech.get_group('PV')
    pv_CF = pv_tech['CF']
    pv_tech_name = pv_tech.loc[2]['Technology_name_OSeMOSYS']
    battery_tech = cf_tech.get_group('Battery')
    battery_tech_name = battery_tech.loc[3:4]['Technology_name_OSeMOSYS']
    battery_CF = battery_tech['CF']
    battery_tech.index = battery_tech['CF']

    #Caluculate the CF for the location over the year
    for m, row in df.iterrows():
       location = str(row['Location']) # This is needed because the columns in capacityfactor_wind isn't int64. They are strings.
       slice_wind = sum(capacityfactor_wind[location])
       average_wind = (slice_wind / len(capacityfactor_wind._values))

       slice_solar = sum(capacityfactor_solar[location])
       average_solar = (slice_solar / len(capacityfactor_solar._values))

       # Wind
       for k in wind_tech.columns[3:]:  # year is an object so I cannot match it with a number (e.g. startyear)
          def find_nearest(wind_CF, average_wind):
              #arraywind = np.asarray(float(wind_CF))
              #idx = (np.abs(arraywind - average_wind)).argmin()
              return str(0.25)
          cf=find_nearest(wind_CF, average_wind)
          wind_tech.index = wind_tech['CF']
          windcapitalcost = wind_tech.loc[cf][k]
          dataToInsert += "%s\t%s_%s\t%s\t%f\n" % (input_data['region'][0], wind_tech_name, location, k, windcapitalcost)

       if battery_tech is None:
           pass
       else:
           for k in wind_tech.columns[3:]:  # year is an object so I cannot match it with a number (e.g. startyear)
               windcapitalcostbatt = wind_tech.loc[cf][k] + battery_tech.loc['4c'][k]
               techname = wind_tech_name + battery_tech_name.loc[4]
               #dataToInsert += ("%s\t%s_%s\t%s\t%f\n" % (input_data['region'][0], techname, location, k, windcapitalcostbatt))

       #Solar PV
       for k in pv_tech.columns[3:]: # year is an object so I cannot match it with a number (e.g. startyear)
          def find_nearest(pv_CF, average_solar):
             #arraysun = np.asarray(float(pv_CF))
             #idx = (np.abs(arraysun - average_solar)).argmin()
             return str(0.21)
          cf=find_nearest(pv_CF, average_solar)
          pv_tech.index = pv_tech['CF']
          pvcapitalcost = pv_tech.loc[cf][k]
          if elec['index_right'].eq(row['Location']).any():
            dataToInsert += ("%s\t%s_%s_1\t%s\t%f\n" % (input_data['region'][0], pv_tech_name, location, k, pvcapitalcost))
            dataToInsert += (
                        "%s\t%s_%s_0\t%s\t%f\n" % (input_data['region'][0], pv_tech_name, location, k, pvcapitalcost))
          if un_elec['index_right'].eq(row['Location']).any():
              dataToInsert += ("%s\t%s_%s_0\t%s\t%f\n" % (input_data['region'][0], pv_tech_name, location, k, pvcapitalcost))
       if battery_tech is None:
            pass
       else:
           for k in pv_tech.columns[3:]:  # year is an object so I cannot match it with a number (e.g. startyear)
              battery_tech_n = battery_tech_name.loc[3]
              sopvcapitalcostbatt = pv_tech.loc[cf][k] + battery_tech.loc['8r'][k]
              techname = pv_tech_name+battery_tech_n
              if elec['index_right'].eq(row['Location']).any():
                  dataToInsert += ("%s\t%s_%s_1\t%s\t%f\n" % (input_data['region'][0], techname, location, k, sopvcapitalcostbatt))
                  dataToInsert += ("%s\t%s_%s_0\t%s\t%f\n" % (
                  input_data['region'][0], techname, location, k, sopvcapitalcostbatt))
              if un_elec['index_right'].eq(row['Location']).any():
                  dataToInsert += ("%s\t%s_%s_0\t%s\t%f\n" % (input_data['region'][0], techname, location, k, sopvcapitalcostbatt))

       #Solar MG i excluded from the optimization
       for k in comm_PV_tech.columns[3:]:
          def find_nearest(comm_PV_CF, average_solar):
             #arraysun = np.asarray(comm_PV_CF)
             #idx = (np.abs(arraysun - average_solar)).argmin()
             return str(0.2)
          cf = find_nearest(comm_PV_CF, average_solar)
          comm_PV_tech.index = comm_PV_tech['CF']
          somgcapitalcost = comm_PV_tech.loc[cf][k]
       #    dataToInsert += ("%s\t%s_%s\t%s\t%f\n" % (input_data['region'][0], comm_PV_tech_name, location, k, somgcapitalcost))

       # Battery
       if battery_tech is None:
           pass
       else:
           for k in comm_PV_tech.columns[3:]:
               battery_tech_n = battery_tech_name.loc[4]
               somgcapitalcostbatt = comm_PV_tech.loc[cf][k] + battery_tech.loc['4c'][k]
               techname = comm_PV_tech_name + '8c'
               dataToInsert += ("%s\t%s_%s\t%s\t%f\n" % (input_data['region'][0], techname, location, k, somgcapitalcostbatt))

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