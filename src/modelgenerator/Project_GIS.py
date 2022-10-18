"""
Module: Project_GIS
=============================

A module for projecting and clipping the GIS data to the appropriate EPSG CRS
-----------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>


"""

import geopandas as gpd
import pandas as pd
import os
import fiona
import rasterio
import win32api
rasterio.gdal_version()
import rasterio.fill
from rasterio.mask import mask
import gdal
import warnings
warnings.filterwarnings('ignore')
from osgeo import gdal
import numpy as np
import sys
import shutil
gdal.UseExceptions()


def masking(admin,tif_file):
    """ This function masks the raster data (tif-file) with the GADM Admin 0 boundaries (admin)
    :param admin:
    :param tif_file:
    :return: tif_file
    """
    try:
        with fiona.open(admin, "r") as shapefile:
            shapes = [feature["geometry"] for feature in shapefile]
        with rasterio.open(tif_file) as src:
            out_image, out_transform = rasterio.mask.mask(src, shapes, crop = True)
            out_meta = src.meta
        out_meta.update({"driver": "GTiff",
                         "height": out_image.shape[1],
                         "width": out_image.shape[2],
                         "transform": out_transform})

        with rasterio.open(tif_file, "w", **out_meta) as dest:
            dest.write(out_image)
        src = None
        dest = None

    except:
        print("Already masked")
    return(tif_file)

def project_raster(rasterdata, output_raster, CR):
    """This function projects the raster data (rasterdata) to EPSG:32737 and save it with the name extension "masked_UMT37S_%s" (outputraster)

    :param rasterdata:
    :param output_raster:
    :return:
    """
    #try:
    print(rasterdata)
    input_raster = gdal.Open(rasterdata)
    gdal.Warp(output_raster, input_raster, dstSRS=CR)
    #except:
     #   print("Already projected")
    return()

def project_vector(vectordata, crs):
    """This function projects the vector data (vectordata) to EPSG:32737

    :param vectordata:
    :param outputvector:
    :return: vector
    """
    gdf = gpd.read_file(vectordata)
    gdf_umt37 = gdf.to_crs(crs)
    return(gdf_umt37)

def clip_vector(admin, vectordata, vectordata_location, outputvector):
    """This function clips the vector data (vectordata) to admin boundaries and save it with the name extension "UMT37S_%s" (outputvector)

    :return: vector
    """
    adm_multi = gpd.read_file(admin)
    adm = adm_multi.explode()
    try:
        vector = gpd.clip(vectordata, adm)
        vschema = gpd.io.file.infer_schema(vector)
        vector.to_file(outputvector, schema = vschema )
    except:
        print("%s, Already clipped" %vectordata)
        vector = vectordata
    #with fiona.open(os.path.dirname(os.path.abspath(vectordata))) as f:
    #    input_schema = f.schema
    #    print(input_schema)

    return(vector)

def merge_transmission(proj_path):
    """This function concatinates the shapefiles which contains the keyword '132kV' and '220kV' to "Concat_Transmission_lines_UMT37S.shp"

    :param proj_path:
    :return:
    """
    current = os.getcwd()
    os.chdir(proj_path)
    files = os.listdir(proj_path)
    shapefiles = []
    for file in files:
        if file.endswith('.shp'):
            f = os.path.abspath(file)
            shapefiles += [f]
    keyword = ['132kV', '220kV']
    tmiss_list = []
    out = [f for f in shapefiles if any(xs in f for xs in keyword)]
    for f in out:
        shpfile = gpd.read_file(f)
        tmiss_list += [shpfile]
    try:
        gdf = pd.concat([shp for shp in tmiss_list]).pipe(gpd.GeoDataFrame)
        gdf.to_file(os.path.join(proj_path,"Concat_Transmission_lines_UMT37S.shp"))
    except:
        print("No transmission lines")
    os.chdir(current)

def merge_mv(proj_path, fil):
    """This function concatinates the shapefiles which contains the keyword '33kV' and '66kV' to "Concat_MV_lines_UMT37S.shp"

    :param proj_path:
    :return:
    """
    current = os.getcwd()
    os.chdir(proj_path)
    files = os.listdir(proj_path)
    shapefiles = []
    for file in files:
        if file.endswith('.shp'):
            f = os.path.abspath(file)
            shapefiles += [f]
    keyword = ['33kV', '66kV']
    tmiss_list = []
    out = [f for f in shapefiles if any(xs in f for xs in keyword)]
    for f in out:
        shpfile = gpd.read_file(f)
        tmiss_list += [shpfile]

    try:
        gdf = pd.concat([shp for shp in tmiss_list]).pipe(gpd.GeoDataFrame)
        gdf.to_file(os.path.join(proj_path,fil.loc['minigrid_concat','filename']))
    except:
        print("No MV lines")
    os.chdir(current)

    return()


def merge_minigrid(proj_path, fil):
    """This function concatinates the shapefiles which contains the keyword 'MiniGrid' to Concat_Mini-grid_UMT37S.shp"

    :param proj_path:
    :return:
    """
    current = os.getcwd()
    os.chdir(proj_path)
    files = os.listdir(proj_path)
    shapefiles = []
    for file in files:
        if file.endswith('.shp'):
            f = os.path.abspath(file)
            shapefiles += [f]
    keyword = 'MiniGrid'
    minigr_list = []
    for fname in shapefiles:
        if keyword in fname:
            shpfile = gpd.read_file(fname)
            minigr_list += [shpfile]

    try:
        gdf = pd.concat([shp for shp in minigr_list]).pipe(gpd.GeoDataFrame)
        gdf.to_file(os.path.join(proj_path,fil.loc['MV_concat','filename']))
    except:
        print("No minigrid files")
    os.chdir(current)

def project_main(GIS_files_path, topath, files, CR):
    """ This main function reads the GIS-layers in GIS_files_path and separates them by raster and vector data.
    Projects the data to WGS84 UMT37S
    Moves all files to ../Projected_files
    Merges the files named 'kV' to two merged shape file of Transmission and Medium Voltage lines
    Merges the files named 'MiniGrid' to one merged shape file

    :param GIS_files_path:
    :return:
    """
    print(os.getcwd())
    basedir = os.getcwd()
    os.chdir(GIS_files_path)
    current = os.getcwd()
    print(os.getcwd())
    #All shp-files in all folders in dir current
    adm = project_vector(os.path.join(current,files.loc['adm_WGS84','filename']), CR)
    adm.to_file(os.path.join(current,files.loc['adm','filename']))
    shpFiles = [os.path.join(dp, f) for dp, dn, filenames in os.walk(current) for f in filenames if
             os.path.splitext(f)[1] == '.shp']
    for s in shpFiles:
        path, filename = os.path.split(s)
        projected = project_vector(s, CR)
        clip_vector(os.path.join(current,files.loc['adm','filename']), projected, s, os.path.join(path, files.loc['crs','filename']+"_%s" % (filename)))

    #All tif-files in all folders in dir current
    tifFiles = [os.path.join(dp, f) for dp, dn, filenames in os.walk(current) for f in filenames if
              os.path.splitext(f)[1] == '.tif']
    for t in tifFiles:
        path, filename = os.path.split(t)
        masking(os.path.join(current,files.loc['adm_WGS84','filename']), t)
        project_raster(os.path.join(path,'%s' % (filename)), os.path.join(path,files.loc['crs','filename']+"_%s" % (filename)),CR)

    #All files containing "UMT37S" is copied to ../Projected_files dir
    def create_dir(dir):
        if not os.path.exists(dir):
            os.makedirs(dir)
    create_dir((topath))
    allFiles = [os.path.join(dp, f) for dp, dn, filenames in os.walk(current) for f in filenames]
    keyword = files.loc['crs','filename']
    for fname in allFiles:
        if keyword in fname:
            shutil.copy("\\\\?\\"+fname, os.path.join(current, topath)) #Due to really long name the \\\\?\\ can trick Windows accepting it
    os.chdir(basedir)
    merge_transmission(topath)
    merge_minigrid(topath, files)
    merge_mv(topath, files)
    return ()


