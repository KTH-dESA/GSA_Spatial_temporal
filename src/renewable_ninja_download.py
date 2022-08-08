"""
Module: Renewable_ninja_download
===================================

A module for downloading data from Renewable ninja. You need to have a token (account) and also a shape file of the points that you want to download.

Usage limits https://www.renewables.ninja/documentation
Anonymous users are limited to a maximum of 5 requests per day.
To increase this limit to 50 per hour, please register for a free user account on renewable.ninja


----------------------------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
import time
import csv
import os
import sys
import geopandas as gpd
import pandas as pd
import subprocess

def project_vector(vectordata):
    """This function projects the vectordata to EPSG: 4326 as the renewable ninja data is in that CRS

    :param vectordata:
    :return:
    """
    #print(vectordata)
    gdf = gpd.read_file(vectordata)
    gdf_wgs84 = gdf.to_crs(epsg=4326)

    return(gdf_wgs84)

#Make CSV files for the download loop
def csv_make(coordinates):
    """This function extracts the coordinates for the csv build to renewable ninja

    :param coordinates:
    :return:
    """

    coordinates['lon'] = coordinates.geometry.apply(lambda p: p.x)
    coordinates['lat'] = coordinates.geometry.apply(lambda p: p.y)
    df = pd.DataFrame(coordinates)
    wind = pd.DataFrame(index=df.index, columns=(['name', 'lat', 'lon', 'from', 'to', 'dataset', 'capacity', 'height', 'turbine']))
    wind["name"] = df ["pointid"]
    wind["lat"] = df["lat"]
    wind["lon"] = df["lon"]
    wind["from"] = "01/01/2016"
    wind["to"] = "31/12/2016"
    wind["dataset"] = "merra2"
    wind["capacity"] = 1
    wind["height"] = 55
    wind["turbine"] = "Vestas+V42+600"
    solar = pd.DataFrame(index=df.index, columns=(['name', 'lat', 'lon', 'from', 'to', 'dataset', 'capacity', 'system_loss', 'tracking', 'tilt', 'azim']))
    solar["name"] = df ["pointid"]
    solar["lat"] = df["lat"]
    solar["lon"] = df["lon"]
    solar["from"] = "01/01/2016"
    solar["to"] = "31/12/2016"
    solar["dataset"] = "merra2"
    solar["capacity"] = 1
    solar["system_loss"] = 0.1
    solar["tracking"] = 0
    solar["tilt"] = 35
    solar["azim"] = 180
    wind_csv = []
    #Make wind csv-files
    i = 0
    try:
        while i < len(wind.index+6):
            temp = []
            for x in range(i,i+6):
                if x <=len(wind.index):
                    currentLine = list(wind.iloc[[x]].iloc[0])
                    temp.append(currentLine)
            fields = ['name', 'lat', 'lon', 'from', 'to', 'dataset', 'capacity', 'height', 'turbine']
            rows = temp
            with open("temp/wind_%i-%i.csv" %(i, i+6), 'w') as f:
                write = csv.writer(f)
                write.writerow(fields)
                write.writerows(rows)

            wind_csv.append("wind_%i-%i.csv" %(i, i+6))
            i += 6
    except:
        modulus = len(wind.index)%6
        while i < len(wind.index + modulus):
            temp = []
            for x in range(i, i + modulus):
                if x <= len(wind.index):
                    currentLine = list(wind.iloc[[x]].iloc[0])
                    temp.append(currentLine)
            fields = ['name', 'lat', 'lon', 'from', 'to', 'dataset', 'capacity', 'height', 'turbine']
            rows = temp
            with open("temp/wind_%i-%i.csv" % (i, i + modulus), 'w') as f:
                write = csv.writer(f)
                write.writerow(fields)
                write.writerows(rows)

            wind_csv.append("wind_%i-%i.csv" % (i, i + modulus))
            i += modulus
    solar_csv = []
    #Make solar csv-files
    j = 0
    try:
        while j < len(solar.index+6):
            temp = []
            for x in range(j,j+6):
                if x <=len(solar.index+6):
                    currentLine = list(solar.iloc[[x]].iloc[0])
                    temp.append(currentLine)
            fields = ['name', 'lat', 'lon', 'from', 'to', 'dataset', 'capacity', 'system_loss', 'tracking', 'tilt', 'azim']
            rows = temp
            with open("temp/solar_%i-%i.csv" %(j, j+6), 'w') as f:
                write = csv.writer(f)
                write.writerow(fields)
                write.writerows(rows)

            solar_csv.append("solar_%i-%i.csv" %(j, j+6))
            j += 6
    except:
        modulus = len(solar.index) % 6
        while j < len(solar.index+modulus):
            temp = []
            for x in range(j,j+modulus):
                if x <=len(solar.index+modulus):
                    currentLine = list(solar.iloc[[x]].iloc[0])
                    temp.append(currentLine)
            fields = ['name', 'lat', 'lon', 'from', 'to', 'dataset', 'capacity', 'system_loss', 'tracking', 'tilt', 'azim']
            rows = temp
            with open("temp/solar_%i-%i.csv" %(j, j+modulus), 'w') as f:
                write = csv.writer(f)
                write.writerow(fields)
                write.writerows(rows)

            solar_csv.append("solar_%i-%i.csv" %(j, j+modulus))
            j += modulus
    return(wind_csv, solar_csv)

def download(path,  Rpath, srcpath, wind, solar, token):
    """This function downloads the renewable ninja data according to the limit of the maximum per hour

    :param path:
    :param wind:
    :param solar:
    :return:
    """

    i = 0
    try:
        while i < len(wind)+8:
            for x in range(i,i+8): #50/6 is 8.3 so we will upload 8 files per hour
                if x < len(wind):
                    type = "wind"
                    csvfiles = path + "/"+ wind[x]
                    csvfilesout = path + "/out_"+wind[x]
                    subprocess.call([
                         Rpath, 'GEOSeMOSYS_download.r',srcpath, token, type, csvfiles, csvfilesout], shell=True)
            print("Waiting to download next 50 data sets")
            time.sleep(3601)
            i += 8
    except:
        modulus = len(wind)%8
        while i < len(wind)+modulus:
            for x in range(i,i+modulus): #50/6 is 8.3 so we will upload 8 files per hour
                if x < len(wind):
                    type = "wind"
                    csvfiles = path + "/"+ wind[x]
                    csvfilesout = path + "/out_"+wind[x]
                    subprocess.call([
                         Rpath, 'GEOSeMOSYS_download.r',srcpath, token, type, csvfiles, csvfilesout], shell=True)
            print("Waiting to download next 50 data sets")
            time.sleep(3601)
            i += modulus

    j = 0
    try:
        while j < len(solar)+8:
            for x in range(j,j+8): #50/6 is 8.3 so we will upload 8 files per hour
                if x < len(solar):
                    type = "solar"
                    csvfiles = path + "/"+ solar[x]
                    csvfilesout = path + "/out_"+solar[x]
                    subprocess.call([
                         Rpath, 'GEOSeMOSYS_download.r',srcpath, token, type, csvfiles, csvfilesout], shell=True)
            print("Waiting to download next 50 data sets")
            time.sleep(3601)
            j += 8
    except:
        modulus = len(solar)%8
        while j < len(solar)+modulus:
            for x in range(j,j+modulus): #50/6 is 8.3 so we will upload 8 files per hour
                if x < len(solar):
                    type = "solar"
                    csvfiles = path + "/"+ solar[x]
                    csvfilesout = path + "/out_"+solar[x]
                    subprocess.call([
                         Rpath, 'GEOSeMOSYS_download.r',srcpath, token, type, csvfiles, csvfilesout], shell=True)
            print("Waiting to download next 50 data sets")
            time.sleep(3601)
            j += modulus

##
def adjust_timezone(path, time_zone_offset):
    """
    The renewable.ninja dataset is in UCT timezone so this function adjusts the dataset to time_zone_offset time
    :param path:
    :param time_zone_offset:
    :return:
    """
    files = [i for i in os.listdir(path) if os.path.isfile(os.path.join(path, i)) and \
             'out_' in i]

    for f in files:
        df = pd.read_csv(path+"/"+f)
        time = df["time"]
        time = time.iloc[time_zone_offset:]
        new_index = range(0,8781)
        time.index = new_index
        df["adjtime"] = time
        df = df.drop(columns=['Unnamed: 0','time'])
        df.to_csv(path+"/timezoneoffset"+f)
