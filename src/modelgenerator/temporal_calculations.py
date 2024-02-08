import pandas as pd
import os
from datetime import timedelta, datetime
import tsam.timeseriesaggregation as tsam
#import matplotlib.pyplot as plt
import copy
#from modelgenerator.Build_csv_files import *

def join_demand_cf(demand_rural, demand_urban, solar_pv, wind):
    """
    This function joins together the datasets demand profile_rural, demandprofile_central, 
    wind and solar for all locations
    """
    print(os.getcwd())
    os. chdir(r"C:\\Users\\nandi\\OneDrive - KTH\\box_files\\PhD\\Paper 4 - GSA\\GSA_reprod\\GSA_Spatial_temporal\\src")
    print(os.getcwd())
    
    demand_rural =  demand_rural.rename(columns={c: c+'_Rural' for c in demand_rural.columns if c not in ['adjtime']})
    high_demand_df = pd.read_csv(demand_urban)
    high_demand_df =  high_demand_df.rename(columns={c: c+'_Central' for c in high_demand_df.columns if c not in ['adjtime']})
    print(high_demand_df)
    pv_df = pd.read_csv(solar_pv)
    pv_df = pv_df.rename(columns={c: c+'_PV' for c in pv_df.columns if c not in ['adjtime']})
    wind_df = pd.read_csv(wind)
    wind_df =  wind_df.rename(columns={c: c+'_Wind' for c in wind_df.columns if c not in ['adjtime']})
    
    wind_df = wind_df.drop('adjtime', axis=1)
    
    cf_merger = pd.concat([pv_df, wind_df], axis=1)
    cf_merger['adjtime'] = pd.to_datetime(cf_merger["adjtime"], format="%Y-%m-%d %H:%M:%S.%f")
    high_demand_df['adjtime'] = pd.to_datetime(high_demand_df["adjtime"], format="%Y-%m-%d %H:%M:%S.%f")
    cf_merge_highdem = pd.merge(high_demand_df, cf_merger, on=['adjtime'], how='left')

    cf_merge_highdem['adjtime'] = pd.to_datetime(cf_merge_highdem['adjtime'], format='%Y-%m-%d %H:%M:%S.%f')
    demand_rural['adjtime'] = pd.to_datetime(demand_rural['adjtime'], format='%Y-%m-%d %H:%M:%S.%f')

    demand_rural['adjtime_2016'] = demand_rural.adjtime.apply(lambda x:x.replace(year=2016))

    # Create a new column representing the hour for each dataframe
    cf_merge_highdem['date'] = cf_merge_highdem['adjtime'].dt.date
    cf_merge_highdem['hour'] = cf_merge_highdem['adjtime'].dt.hour
    demand_rural['date'] = demand_rural['adjtime_2016'].dt.date
    demand_rural['hour'] = demand_rural['adjtime_2016'].dt.hour

    # Merge or join on the 'date' and 'hour' columns
    result = pd.merge(demand_rural,cf_merge_highdem, on=['date', 'hour'], how='inner')

    # Drop the temporary columns if needed
    result.index = result['adjtime_2016']
    result = result.drop(['hour', 'date', 'adjtime_x', 'adjtime_y','adjtime_2016'], axis=1)

    result.to_csv('join_df.csv')
    return result

#Clustering based on tsam with typical periods representing the seasons and segmentation the intraday 
def clustering_tsam(timeseries_df, typicalperiods, intraday_steps, country):
    """
    This function clusters the dataframe timeseries_df to the parameters typical periods and number of 
    intraday_steps and saves it to the indexed and clustered 
    """

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
    typPeriods_clusters.to_csv('%s_run/scenarios/typperiods_segmentation_%i.csv' %(country, intraday_steps))

    typical_series_index = aggregation.indexMatching()
    typical_series_index.to_csv('%s_run/scenarios/typicalperiods_segmentation_index_daytypes%i.csv' %(country, intraday_steps))
    
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

def demandprofile_calculation(profile, temporal_clusters_index, savepath, years, header):

    minute_profile = pd.read_csv(profile)
    minute_profile.index = pd.to_datetime(minute_profile[header], format='%H:%M')

    temporal_clusters_index['TimeSlice']=  temporal_clusters_index["PeriodNum"].astype(str) + temporal_clusters_index["SegmentIndex"].astype(str)
    total_length = temporal_clusters_index.TimeSlice.size
    
    slices = temporal_clusters_index.groupby(['TimeSlice']).size().reset_index()
    slices.index = slices.TimeSlice
    slicearray = slices.iloc[:,1].div(total_length, axis=0)
    slicearray.drop(columns=["TimeSlice"])

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

    # times = []
    # for i in range(1,round(dayslices)+1):
    #     ts_sum = 's'+str(i)
    #     #ts_wint = 'WINTER'+str(i)
    #     m = i-1
    #     startDay =pd.to_datetime('1900-01-01 00:00:00')
    #     endDay = pd.to_datetime('1900-01-02 00:00:00')
    #     startDate = pd.to_datetime('1900-01-01 00:00:00')+ timedelta(hours = m*hours_per_timeslice)
    #     endDate = pd.to_datetime('1900-01-01 00:00:00') + timedelta(hours = m*hours_per_timeslice+hours_per_timeslice)- timedelta(minutes=1)
    #     times += [(startDate.strftime('%H:%M'), endDate.strftime('%H:%M'), i)]
    #     slicearray[ts_sum]= calculate_slice(minute_profile, startDate, endDate, seasonAprSep, seasonOctMarch, startDay, endDay)

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

    return savepath #, times

def addtimestep(time, inputdata, savepath):
    input_data_df = pd.read_csv(inputdata)
    timeslices =  pd.DataFrame(time, columns =['Daysplitstart','Daysplitend','Daysplit'])
    inputdata_timestep = pd.concat((input_data_df, timeslices),axis=1)

    inputdata_timestep.to_csv(savepath)


# country = 'Kenya'
# DemandProfileTier = 3
# # tier_profile = '%sinput_data/T%i_load profile_Narayan.csv' %(country, DemandProfileTier)
# highload_yearly = '%sinput_data/halfhourly_data_long.csv' %(country)
# # temporal_id = 2
# load_yearly_df = pd.read_csv('Kenya_run/scenarios/annualload_tier4_temporal9.csv')
# # load_yearly = annualload(tier_profile, '%s_run/scenarios/annualload_tier%i_temporal%i.csv' %(country, DemandProfileTier, int(temporal_id)))

# # timeseries_df = join_demand_cf(load_yearly, highload_yearly, '%stemp/1/timezoneoffset_solar_0-1.csv' %(country), '%stemp/1/timezoneoffset_wind_0-1.csv' %(country))
# timeseries_df = join_demand_cf(load_yearly_df, highload_yearly, '%s_run/scenarios/uncertain0.016700_spatial34_capacityfactor_solar.csv' %(country),'%s_run/scenarios/uncertain0.016700_spatial34_capacityfactor_wind.csv' %(country))
 