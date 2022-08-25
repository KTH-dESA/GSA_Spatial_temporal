import pandas as pd
import datetime
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime

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


def capacityfactor_modification(outPutFile,input_data, capacityfactor_other):
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
    df_ = capacityfactor_other.columns.drop('adjtime')
    capacityfactor_solar_ = capacityfactor_other.copy()
    capacityfactor_solar_p = pd.to_datetime(capacityfactor_solar_['adjtime'], errors='coerce', format='%Y/%m/%d %H:%M')
    capacityfactor_solar_.index = capacityfactor_solar_p
    capacityfactor_solar_pv = capacityfactor_solar_.drop(columns=['adjtime'])
    capacityfactor_solar_pv = capacityfactor_solar_pv.dropna(how='all')


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
    for k in df_:
        location = k
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
                dataToInsert += "%s\t%s\t%s\t%i\t%f\n" % (region, k, tsday, year, average_solar_day)
                dataToInsert += "%s\t%s\t%s\t%i\t%f\n" % (region, k, tsevening, year, average_solar_evening)
                dataToInsert += "%s\t%s\t%s\t%i\t%f\n" % (region, k, tsnight, year, average_solar_night)
                m +=1
            year += 1
    outPutFile = outPutFile[:startIndex] + dataToInsert + outPutFile[startIndex:]
    return (outPutFile)
def write_to_file(file_object, outPutFile):
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
    with open(file_object, "w") as actualOutputFile:
       actualOutputFile.truncate(0) #empty the file
       actualOutputFile.write(outPutFile)

dict_df =load_csvs("run/scenarios")
outPutFile = make_outputfile("run/Benin.txt")
outPutFile = capacityfactor_modification(outPutFile, dict_df['input_data'], dict_df['capacity_factor_other'])
write_to_file('run/Benin_modified.txt', outPutFile)