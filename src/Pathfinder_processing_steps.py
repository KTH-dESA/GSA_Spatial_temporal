"""
Module: Pathfinder_processing_steps
===============================================

A module that runs the GIS functions for Pathfinder, removes overlapping grid from the results and then mosaic the results to a tif file
----------------------------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

from Pathfinder import *
from Pathfinder_GIS_steps import *
import numpy as np
import pandas as pd
import os
import gdal
import ogr
import csv


def mosaic(dict_raster, proj_path):
    """
    This function mosaic the tiles (dict_raster) from Pathfinder to one tif file and places it in Projected_files folder
    :param dict_raster:
    :param proj_path:
    :return:
    """
    pathfinder = []
    for key, value in dict_raster.items():
        src = rasterio.open(value)
        pathfinder.append(src)

    mosaic, out_trans = merge(pathfinder)
    out_meta = src.meta.copy()
    out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans,
                     "crs": ({'init': 'EPSG:32737'})})
    with rasterio.open('%s/pathfinder.tif' % proj_path, "w", **out_meta) as dest:
        dest.write(mosaic)
    print('Pathfinder is now mosaicked to pathfinder.tif')
    return ()

def remove_grid_from_results_multiply_with_lenght(dict_pathfinder, dict_weight,tofolder, polygon, scenario):
    """
    This function sets the results (shortest path network) from Pathfinder that are overlapping weights less than 0.5
    so that where the grid route is utilized this is not double counted in the final results. It then runs zonal statistics 
    using the polygon for each scenario.
    :param dict_pathfinder:
    :param dict_weight:
    :param tofolder
    :return:dict_pathfinder
    """

    def boundingBoxToOffsets(bbox, geot):
        col1 = int((bbox[0] - geot[0]) / geot[1])
        col2 = int((bbox[1] - geot[0]) / geot[1]) + 1
        row1 = int((bbox[3] - geot[3]) / geot[5])
        row2 = int((bbox[2] - geot[3]) / geot[5]) + 1
        return [row1, row2, col1, col2]


    def geotFromOffsets(row_offset, col_offset, geot):
        new_geot = [
        geot[0] + (col_offset * geot[1]),
        geot[1],
        0.0,
        geot[3] + (row_offset * geot[5]),
        0.0,
        geot[5]
        ]
        return new_geot


    def setFeatureStats(fid, min, max, mean, median, sd, sum, count, names=["min", "max", "mean", "median", "sd", "sum", "count", "id"]):
        featstats = {
        names[0]: min,
        names[1]: max,
        names[2]: mean,
        names[3]: median,
        names[4]: sd,
        names[5]: sum,
        names[6]: count,
        names[7]: fid,
        }
        return featstats

    sum_distribution = {}
    for key in dict_pathfinder:
        elec_path = dict_pathfinder[key]
        path_weight = dict_weight[key]
        assert elec_path.size == path_weight.size
        col_length = len(elec_path.columns)
        row_length = len(elec_path)
        i = 0
        k = 0
        j = 0
        while j < row_length:
            m = 0
            while m < col_length:
                if path_weight.iloc[(i + j), (k + m)] < 0.5:
                    elec_path.iloc[(i + j), (k + m)] = 0
                m += 1
            j += 1

        #sum_distribution[key] = elec_path.values.sum()
    
    elec_path_np = elec_path.to_numpy()
    #rasterize the cleaned numpyarray
    make_raster(elec_path_np, "cleaned", "1")

    mem_driver = ogr.GetDriverByName("Memory")
    mem_driver_gdal = gdal.GetDriverByName("MEM")
    shp_name = "run/zonalstatistics"

    fn_raster =  os.path.join('temp/dijkstra','path_cleaned.tif')
    fn_zones = polygon

    r_ds = gdal.Open(fn_raster)
    p_ds = ogr.Open(fn_zones)

    lyr = p_ds.GetLayer()
    geot = r_ds.GetGeoTransform()
    nodata = r_ds.GetRasterBand(1).GetNoDataValue()

    zstats = []

    p_feat = lyr.GetNextFeature()
    niter = 0

    while p_feat:
        if p_feat.GetGeometryRef() is not None:
            if os.path.exists(shp_name):
                mem_driver.DeleteDataSource(shp_name)
            tp_ds = mem_driver.CreateDataSource(shp_name)
            tp_lyr = tp_ds.CreateLayer('polygons', None, ogr.wkbPolygon)
            tp_lyr.CreateFeature(p_feat.Clone())
            offsets = boundingBoxToOffsets(p_feat.GetGeometryRef().GetEnvelope(),\
            geot)
            new_geot = geotFromOffsets(offsets[0], offsets[2], geot)
            
            tr_ds = mem_driver_gdal.Create(\
            "", \
            offsets[3] - offsets[2], \
            offsets[1] - offsets[0], \
            1, \
            gdal.GDT_Byte)
            
            tr_ds.SetGeoTransform(new_geot)
            gdal.RasterizeLayer(tr_ds, [1], tp_lyr, burn_values=[1])
            tr_array = tr_ds.ReadAsArray()
            
            r_array = r_ds.GetRasterBand(1).ReadAsArray(\
            offsets[2],\
            offsets[0],\
            offsets[3] - offsets[2],\
            offsets[1] - offsets[0])
            
            id = p_feat.GetFID()
            
            if r_array is not None:
                maskarray = np.ma.MaskedArray(\
                r_array,\
                maskarray=np.logical_or(r_array==nodata, np.logical_not(tr_array)))
                
                if maskarray is not None:
                    zstats.append(setFeatureStats(\
                    id,\
                    maskarray.min(),\
                    maskarray.max(),\
                    maskarray.mean(),\
                    np.ma.median(maskarray),\
                    maskarray.std(),\
                    maskarray.sum(),\
                    maskarray.count()))
                else:
                    zstats.append(setFeatureStats(\
                    id,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata))
            else:
                zstats.append(setFeatureStats(\
                    id,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata,\
                    nodata))
            
            tp_ds = None
            tp_lyr = None
            tr_ds = None
            
            p_feat = lyr.GetNextFeature()
            
    fn_csv = "run/scenarios/%i_distributionslines.csv" %(scenario)
    col_names = zstats[0].keys()
    with open(fn_csv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, col_names)
        writer.writeheader()
        writer.writerows(zstats)
    
    
    
    
    #df = pd.DataFrame.from_dict(sum_distribution, orient='index')
    #Modified to not sum at this point
    #elec_path.to_csv(os.path.join(tofolder,'distributionlines.csv'))



def pathfinder_main(path,proj_path, elec_shp, tofolder, tiffile, crs, polygon, scenario):
    """
    This is the function which runs all GIS functions and Pathfinder
    :param path:
    :param proj_path:
    :param elec_shp:
    :param tofolder:
    :return:
    """
    elec_shape = convert_zero_to_one(elec_shp)
    #The elec_raster will serve as the points to connect and the roads will create the weights
    #Returns the path to elec_raster
    elec_raster = rasterize_elec(elec_shape, path, tiffile)

    #Concatinate the highway with high- medium and low voltage lines
    grid_weight = merge_grid(path)

    #returns the path to highway_weights
    highway_shp, grid_shp = highway_weights(grid_weight, path, crs)
    highway_raster = rasterize_road(highway_shp, path)
    transmission_raster = rasterize_transmission(grid_shp, path)
    weights_raster = merge_raster(transmission_raster, highway_raster, crs)


    files = os.listdir(proj_path)
    shapefiles = []
    for file in files:
        if file.endswith('.shp'):
            f = os.path.join(proj_path, file)
            shapefiles += [f]

    print("Calculating Pathfinder for each cell, used for the OSeMOSYS-file")
    #This is the final version and the other is as reference for uncertainty analysis
    dict_pathfinder = {}
    dict_raster = {}
    dict_weight = {}
    for f in shapefiles:
        name, end = os.path.splitext(os.path.basename(f))
        weight_raster_cell = masking(f, weights_raster, '%s_weight.tif' %(name))
        elec_raster_cell = masking(f, elec_raster, '%s_elec.tif' % (name))

        # make csv files for Dijkstra
        weight_csv = make_weight_numpyarray(weight_raster_cell, name)
        target_csv = make_target_numpyarray(elec_raster_cell, name)
        if not os.path.exists(target_csv):
          e = "No targets in square"
        try:
            if os.path.exists(target_csv):
                targets = np.genfromtxt(os.path.join('temp/dijkstra', "%s_target.csv" % (name)), delimiter=',')
                weights = np.genfromtxt(os.path.join('temp/dijkstra', "%s_weight.csv" % (name)), delimiter=',')
                origin_csv = make_origin_numpyarray(target_csv, name)
                origin = np.genfromtxt(os.path.join('temp/dijkstra', "%s_origin.csv" % (name)), delimiter=',')
                # Run the Pathfinder alogrithm seek(origins, target, weights, path_handling='link', debug=False, film=False)
                pathfinder = seek(origin, targets, weights, path_handling='link', debug=False, film=False)
                elec_path = pathfinder['paths']
                elec_path_trimmed = elec_path[1:-1,1:-1]
                weights_trimmed= weights[1:-1,1:-1]
                electrifiedpath = pd.DataFrame(elec_path_trimmed)
                weight_pandas = pd.DataFrame(weights_trimmed)
                electrifiedpath.to_csv("temp/dijkstra/elec_path_%s.csv" % (name))
                dict_pathfinder[name] = electrifiedpath
                dict_weight[name] = weight_pandas
                raster_pathfinder = make_raster(elec_path_trimmed, name, name)
                dict_raster[name] = raster_pathfinder

        except Exception as e:
            print(e)
            continue

    print("Make raster of pathfinder")
    mosaic(dict_raster, path)
    print("Remove pathfinder where grid is passed to not double count")
    remove_grid_from_results_multiply_with_lenght(dict_pathfinder, dict_weight, tofolder, polygon, scenario)

