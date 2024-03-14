import pandas as pd
import os
from datetime import timedelta, datetime
import tsam.timeseriesaggregation as tsam
#import matplotlib.pyplot as plt
import copy
#from modelgenerator.Build_csv_files import *

def join_demand_cf(demand_rural, demand_urban, solar_pv, wind, capacityfactor_other, tofile):
    """
    This function joins together the datasets demand profile_rural, demandprofile_central, 
    wind and solar, and the central power plants for all locations
    """    
    capacityfactor_other_df = pd.read_csv(capacityfactor_other).drop('adjtime', axis=1)
    demand_rural =  demand_rural.rename(columns={c: c+'_Rural' for c in demand_rural.columns if c not in ['adjtime']})
    high_demand_df = pd.read_csv(demand_urban)
    high_demand_df =  high_demand_df.rename(columns={c: c+'_Central' for c in high_demand_df.columns if c not in ['adjtime']})
    pv_df = pd.read_csv(solar_pv)
    pv_df = pv_df.rename(columns={c: 'SOPV_'+c for c in pv_df.columns if c not in ['adjtime']})
    wind_df = pd.read_csv(wind)
    wind_df =  wind_df.rename(columns={c: 'WI_'+c for c in wind_df.columns if c not in ['adjtime']})
    
    wind_df = wind_df.drop('adjtime', axis=1)
    
    cf_merger = pd.concat([pv_df, wind_df,capacityfactor_other_df], axis=1)
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
    result.to_csv(tofile)
    return result

#Clustering based on tsam with typical periods representing the seasons and segmentation the intraday 
def clustering_tsam(timeseries_df, typicalperiods, intraday_steps, to_indexfile, to_clusterfile):
    """
    This function clusters the dataframe timeseries_df to the parameters typical periods and number of 
    intraday_steps and saves it to the indexed and clustered csv to the scenario folder per country
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
    typPeriods_clusters.to_csv(to_clusterfile)
    #Fix season index
    typerioddf = pd.read_csv(to_clusterfile)
    typerioddf.iloc[:,0] = typerioddf.iloc[:,0].astype(str).radd('S')
    typerioddf.rename(columns={'Unnamed: 0':'Season'}, inplace=True )
    typerioddf.to_csv(to_clusterfile)

    typical_series_index = aggregation.indexMatching()
    typical_series_index.to_csv(to_indexfile)
    #Fix season index
    typical_index = pd.read_csv(to_indexfile)
    typical_index['PeriodNum'] = typical_index['PeriodNum'].astype(str).radd('S')
    typical_index.to_csv(to_indexfile)

    
    return to_indexfile, to_clusterfile


def yearsplit_calculation(temporal_clusters_index, years, savepath):
    """ This function takes the number of timestamps (hours) in each cluster and creates the 
        year split based on the time duration for each.
    """
    temporal_clusters_index_df= pd.read_csv(temporal_clusters_index)
    temporal_clusters_index_df['TimeSlice']=  temporal_clusters_index_df["PeriodNum"].astype(str) + temporal_clusters_index_df["SegmentIndex"].astype(str)
    total_length = temporal_clusters_index_df.TimeSlice.size
    slices = temporal_clusters_index_df.groupby(['TimeSlice']).size().reset_index()
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

def timedependentprofile_calculation(clusters, temporal_clusters_index, savepath, years, rural_csv):
    """This function calculates the timeslices from the clustering approach for demand, and capacityfactors for each location
    """
    #First identify the Timeslices which are Periodnum + SegmentIndex from the clustering

    clusters_df= pd.read_csv(clusters)
    temporal_clusters_index_df= pd.read_csv(temporal_clusters_index)

    temporal_clusters_index_df['TimeSlice']=  temporal_clusters_index_df["PeriodNum"].astype(str) + temporal_clusters_index_df["SegmentIndex"].astype(str)
    #clusters_df.columns.values[0] = "PeriodIndex"
    clusters_df['TimeSlice'] =  clusters_df["Season"].astype(str) + clusters_df["Segment Step"].astype(str)
    clusters_df_dem = clusters_df[['TimeSlice','Load_Rural','Load_Central']]
    clusters_df.index = clusters_df['TimeSlice']
    cluster_df_nodem = clusters_df.drop(columns=['Season','Unnamed: 0','TimeSlice','Load_Rural','Load_Central','Segment Step',	'Segment Duration'])

    #Join the two datasets together to get the full dataset for the year.
    merged_cluster_index = pd.merge(temporal_clusters_index_df, clusters_df_dem, how="left", on=["TimeSlice"])
    specf_dem_clusters = merged_cluster_index.groupby("TimeSlice").sum().reset_index()
    specf_dem_clusters.index = specf_dem_clusters['TimeSlice']
    specf_dem_clusters = specf_dem_clusters.drop(columns=['Unnamed: 0', "SegmentIndex", "TimeStep", 'TimeSlice'])
  
    #Divide the sum of each time slice with the total sum per column
    specifiecdem_clusters_ts = specf_dem_clusters.div(specf_dem_clusters.sum(axis=0), axis=1)    
    timeslices_clusters = cluster_df_nodem.join(specifiecdem_clusters_ts)
    timeslices_clusters.to_csv(savepath)

    ### Special case for rural demand which need to be for each year for peak demand calculation
    demand_rural = specifiecdem_clusters_ts['Load_Rural']
    df = pd.DataFrame.from_dict([demand_rural])
    df= df.T
    df.index.names = ['Timeslice']
    multiple = pd.concat([df.T]*(len(years))).T
    multiple.columns = years
    multiple.index = df.index
    multiple.to_csv(rural_csv)

    return timeslices_clusters, rural_csv


  