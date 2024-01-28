import pandas as pd
from datetime import timedelta
import tsam.timeseriesaggregation as tsam
import matplotlib.pyplot as plt
import copy

def join_demand_cf(demand_rural, demand_urban, solar_pv, wind):
    joined_df = pd.DataFrame([1,2,3])
    return joined_df

#Clustering based on tsam with typical periods representing the seasons and segmentation the intraday 
def clustering_tsam(timeseries_df, typicalperiods, intraday_steps):

    aggregation = tsam.TimeSeriesAggregation(timeseries_df,
    noTypicalPeriods = typicalperiods,
    hoursPerPeriod = 24,
    segmentation = True,
    noSegments = intraday_steps,
    representationMethod = "distributionAndMinMaxRepresentation",
    distributionPeriodWise = False,
    clusterMethod = 'hierarchical'
    )

    typPeriods_clusters = aggregation.createTypicalPeriods()
    typPeriods_clusters.to_csv('typperiods_segmentation_%.csv')

    typical_series_index = aggregation.indexMatching()
    typical_series_index.to_csv('typicalperiods_segmentation_index.csv')
    
    return typical_series_index, typPeriods_clusters


def yearsplit_calculation(temporal_clusters_index, years, savepath):
    """ This function takes the number of timestamps (hours) in each cluster and creates the 
        year split based on the time duration for each.
    """
    temporal_clusters_index['TimeSlice']=  temporal_clusters_index["PeriodNum"].astype(str) + temporal_clusters_index["SegmentIndex"].astype(str)
    total_length = temporal_clusters_index.TimeSlice.size
    slices = temporal_clusters_index.groupby(['TimeSlice']).size().reset_index()
    slices.index = slices.TimeSlice
    slicearray = slices.iloc[:,1].div(total_length, axis=0)
    slicearray.drop(columns=["TimeSlice"])

    assert 0.99<sum(slicearray.values)<1.01
    print('yearsplit adds up to 1')

    df = pd.DataFrame.from_dict([slicearray])
    df = df.T
    df.index.names = ['Timeslice']
    multiple = pd.concat([df.T]*(len(years))).T
    multiple.columns = years
    multiple.index = df.index
    multiple.to_csv(savepath)

    return savepath

def demandprofile_calculation(profile, dayslices, seasonAprSep, seasonOctMarch, savepath, years, header):

    minute_profile = pd.read_csv(profile)
    hours_per_timeslice = 24/dayslices
    minute_profile.index = pd.to_datetime(minute_profile[header], format='%H:%M')

    slicearray = {}
    hours_per_day = 24/dayslices
    #assert hours_per_day*dayslices == 24
    #print('hours of day adds up to 24')
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

        return summer_ts

    times = []
    for i in range(1,round(dayslices)+1):
        ts_sum = 's'+str(i)
        #ts_wint = 'WINTER'+str(i)
        m = i-1
        startDay =pd.to_datetime('1900-01-01 00:00:00')
        endDay = pd.to_datetime('1900-01-02 00:00:00')
        startDate = pd.to_datetime('1900-01-01 00:00:00')+ timedelta(hours = m*hours_per_timeslice)
        endDate = pd.to_datetime('1900-01-01 00:00:00') + timedelta(hours = m*hours_per_timeslice+hours_per_timeslice)- timedelta(minutes=1)
        times += [(startDate.strftime('%H:%M'), endDate.strftime('%H:%M'), i)]
        slicearray[ts_sum]= calculate_slice(minute_profile, startDate, endDate, seasonAprSep, seasonOctMarch, startDay, endDay)

    assert 0.99<sum(slicearray.values())<1.017
    print("Demandprofile sum is 1 over the year")

    df = pd.DataFrame.from_dict([slicearray])
    df= df.T
    df.index.names = ['Timeslice']
    profile = df[0]
    multiple = pd.concat([df.T]*(len(years))).T
    multiple.columns = years
    multiple.index = df.index
    multiple.to_csv(savepath)

    return savepath, times

def addtimestep(time, inputdata, savepath):
    input_data_df = pd.read_csv(inputdata)
    timeslices =  pd.DataFrame(time, columns =['Daysplitstart','Daysplitend','Daysplit'])
    inputdata_timestep = pd.concat((input_data_df, timeslices),axis=1)

    inputdata_timestep.to_csv(savepath)
