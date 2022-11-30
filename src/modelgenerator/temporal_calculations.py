import pandas as pd
from datetime import datetime, timedelta

def yearsplit_calculation(dayslices, seasonAprSep, seasonOctMarch, savepath, years):
    slicearray = {}
    hoursofyear = 8760
    hours_per_day = round(24/dayslices)
    assert hours_per_day*dayslices == 24
    print('hours of day adds up to 24')
    #no_timeslices = hours_per_day * round(365/seasonAprSep)

    for i in range(1,int(dayslices)+1):
        ts_sum = 'SUMMER'+str(i)
        ts_wint = 'WINTER'+str(i)
        slicearray[ts_sum] = hours_per_day*seasonAprSep/hoursofyear
        slicearray[ts_wint] = hours_per_day*seasonOctMarch/hoursofyear

    assert 0.99<sum(slicearray.values())<1.01
    print('yearsplit adds up to 1')

    df = pd.DataFrame.from_dict([slicearray])
    df.T
    df.index.names = ['Timeslice']
    multiple = pd.concat([df.T]*36).T
    multiple.columns = years
    multiple.index = df.index
    multiple.to_csv(savepath)

    return savepath

def demandprofile_calculation(profile, dayslices, seasonAprSep, seasonOctMarch, savepath, years):
    minute_profile = pd.read_csv(profile)
    hours_per_timeslice = 24/dayslices
    minute_profile.index = pd.to_datetime(minute_profile['Minute'], format='%H:%M')

    slicearray = {}
    hours_per_day = round(24/dayslices)
    assert hours_per_day*dayslices == 24
    print('hours of day adds up to 24')
    #no_timeslices = hours_per_day * round(365/seasonAprSep)

    def calculate_sum(data, startdate, enddate):
        mask = (data.index >= startdate) & (data.index <= enddate)
        thisMonthOnly = data.loc[mask]
        slice = sum(thisMonthOnly['Load'])

        return slice

    def calculate_slice(data, startdate, enddate, summer, winter, startDay, endDay):
        slice = calculate_sum(data, startdate, enddate)
        totaldayload = calculate_sum(data, startDay, endDay)

        totalloadyear= totaldayload*365
        summer_ts = slice*summer/totalloadyear
        winter_ts = slice*winter/totalloadyear

        return summer_ts, winter_ts


    for i in range(1,int(dayslices)+1):
        ts_sum = 'SUMMER'+str(i)
        ts_wint = 'WINTER'+str(i)
        m = i-1
        startDay =pd.to_datetime('1900-01-01 00:00:00')
        endDay = pd.to_datetime('1900-01-01 23:59:00')
        startDate = pd.to_datetime('1900-01-01 00:00:00')+ timedelta(hours = m*hours_per_timeslice)
        endDate = pd.to_datetime('1900-01-01 00:00:00') + timedelta(hours = m*hours_per_timeslice+hours_per_timeslice)
        slicearray[ts_sum],slicearray[ts_wint] = calculate_slice(minute_profile, startDate, endDate, seasonAprSep, seasonOctMarch, startDay, endDay)

    assert 0.99<sum(slicearray.values())<1.01
    print("Demandprofile sum is 1 over the year")

    df = pd.DataFrame.from_dict([slicearray])
    df= df.T
    df.index.names = ['Timeslice']
    profile = df[0]
    multiple = pd.concat([df.T]*36).T
    multiple.columns = years
    multiple.index = df.index
    multiple.to_csv(savepath)

    return savepath

profile = "src/input_data/T3_load profile_Narayan.csv"
dayslices = 2
seasonAprSep = 183
seasonOctMarch = 182
savepath = 'src/run/scenarios/demandprofile_rural_test.csv'
year_array = ['2020', '2021', '2022','2023','2024','2025','2026','2027','2028','2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040',	'2041',	'2042',	'2043',	'2044',	'2045',	'2046',	'2047',	'2048',	'2049',	'2050',	'2051',	'2052',	'2053',	'2054',	'2055']
system = demandprofile_calculation(profile, dayslices, seasonAprSep, seasonOctMarch, savepath, year_array)