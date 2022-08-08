"""
Module: Prepare_elec_start
=============================

A module for extracting values from raster to point layer (provided by the user).
Columns are added with proximity analysis to transmission lines, nighttimelight, mini-grid roads etc.

----------------------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
import rasterio
from rasterio.merge import merge
from osgeo import gdal, ogr, gdalconst
import os
import sys
import geopandas as gpd
import subprocess
gdal.UseExceptions()


def raster_to_point(raster_list, pop_shp, proj_path):
    """Find all VIIRS files named avg_rade9h and mosaic them.
    Extract values to point layer (population layer 1kmx1km)
    :param raster_list:
    :param pop_shp:
    :param proj_path:
    :return:
    """
    current = os.getcwd()
    os.chdir(proj_path)
    files = os.listdir(proj_path)
    tiffiles = []
    for file in files:
        if file.endswith('.tif'):
            f = os.path.abspath(file)
            tiffiles += [f]
    keyword = 'average_masked'  #	(avergage_masked) nW/cm2/sr https://eogdata.mines.edu/products/vnl/#annual_v2 doi:10.3390/rs13050922
    viirs = []
    for fname in tiffiles:
        if keyword in fname:
            src = rasterio.open(fname)
            viirs.append(src)

    mosaic, out_trans = merge(viirs)
    out_meta = src.meta.copy()
    out_meta.update({"driver": "GTiff", "height": mosaic.shape[1],"width": mosaic.shape[2], "transform": out_trans, "crs": ({'init': 'EPSG:32737'})})
    print('TIF files that contain %s is now mosaicked to viirs.tif' %(keyword))

    with rasterio.open('%s/viirs.tif' % proj_path, "w", **out_meta) as dest:
        dest.write(mosaic)

    settlements = gpd.read_file(pop_shp)
    print(settlements.crs)
    settlements = settlements[['ID', 'hrslmultipl', 'geometry']]
    settlements.index = range(len(settlements))
    coords = [(x, y) for x, y in zip(settlements.geometry.x, settlements.geometry.y)]

    # Open the raster and store metadata
    nighttimelight = rasterio.open('%s/viirs.tif' % proj_path)
    print(nighttimelight.crs)

    # Sample the raster at every point location and store values in DataFrame
    settlements['Nighttime'] = [x[0] for x in nighttimelight.sample(coords)]
    print("Nighttime light")

    print('The viirs.tif file is converted to a dataframe')

    _, filename = os.path.split(raster_list[0])
    name, ending = os.path.splitext(filename)
    trans = rasterio.open(raster_list[0])
    settlements['Grid'] = [x[0] for x in trans.sample(coords)]
    print(name)

    print('The %s.tif file is added to the dataframe' %(name))

    _, filename = os.path.split(raster_list[1])
    name, ending = os.path.splitext(filename)
    subs = rasterio.open(raster_list[1])
    print(subs.crs)
    settlements['Substation'] = [x[0] for x in subs.sample(coords)]
    print(name)

    print('The %s.tif file is added to the dataframe' %(name))

    _, filename = os.path.split(raster_list[2])
    name, ending = os.path.splitext(filename)
    transf = rasterio.open(raster_list[2])
    settlements['Transform'] = [x[0] for x in transf.sample(coords)]

    print('The %s.tif file is added to the dataframe' %(name))

    _, filename = os.path.split(raster_list[3])
    name, ending = os.path.splitext(filename)
    minig = rasterio.open(raster_list[3])
    settlements['Minigrid'] = [x[0] for x in minig.sample(coords)]

    print('The %s.tif file is added to the dataframe' %(name))

    _, filename = os.path.split(raster_list[4])
    name, ending = os.path.splitext(filename)
    road = rasterio.open(raster_list[4])
    settlements['Road'] = [x[0] for x in road.sample(coords)]

    print('The %s.tif file is added to the dataframe' %(name))

    _, filename = os.path.split(raster_list[5])
    name, ending = os.path.splitext(filename)
    mv = rasterio.open(raster_list[5])
    settlements['MV'] = [x[0] for x in mv.sample(coords)]

    print('The %s.tif file is added to the dataframe' %(name))

    _, filename = os.path.split(raster_list[6])
    name, ending = os.path.splitext(filename)
    lv = rasterio.open(raster_list[6])
    settlements['LV'] = [x[0] for x in lv.sample(coords)]

    print('The %s.tif file is added to the dataframe' %(name))

    settlements.to_file(os.path.join(proj_path, 'settlements.shp'))
    print('The dataframe is saved to settlements.shp')

    return()

def raster_proximity(proj_path):
    """This function creates raster file of lines an polygons and creates a proximity raster in the same resolution as population points (1kmx1km)
    :param proj_path:
    :return:
    """
    # Rasterise the shapefile to the same projection & pixel resolution as population layer in 1x1km resolution.
    raster_list = [os.path.join(proj_path,'Concat_Transmission_lines_UMT37S.shp'), os.path.join(proj_path,'UMT37S_Primary_Substations.shp'), os.path.join(proj_path,'UMT37S_Distribution_Transformers.shp'), os.path.join(proj_path,'Concat_Mini-grid_UMT37S.shp'),os.path.join(proj_path,'UMT37S_Roads.shp'), os.path.join(proj_path,'Concat_MV_lines_UMT37S.shp'), os.path.join(proj_path,'UMT37S_11kV.shp')]
    raster_out = []
    raster_prox = []
    for i in range(len(raster_list)):
        print('Rasterizing %s' %(raster_list[i]))
        InputVector = raster_list[i]
        _, filename = os.path.split(raster_list[i])
        name, ending = os.path.splitext(filename)

        OutputImage = os.path.join(proj_path, name+'.tif')
        raster_out.append(OutputImage)

        RefImage = os.path.join(proj_path,'projected_HRSL_2019_UMT37S.tif')

        gdalformat = 'GTiff'
        datatype = gdal.GDT_Byte
        burnVal = 1  # value for the output image pixels
        # Get projection info from reference image
        Image = gdal.Open(RefImage, gdal.GA_ReadOnly)

        # Open Shapefile
        Shapefile = ogr.Open(InputVector)
        Shapefile_layer = Shapefile.GetLayer()

        # Rasterise
        Output = gdal.GetDriverByName(gdalformat).Create(OutputImage, Image.RasterXSize, Image.RasterYSize, 1, datatype,
                                                         options=['COMPRESS=DEFLATE'])
        Output.SetProjection(Image.GetProjectionRef())
        Output.SetGeoTransform(Image.GetGeoTransform())

        # Write data to band 1
        Band = Output.GetRasterBand(1)
        Band.SetNoDataValue(0)
        gdal.RasterizeLayer(Output, [1], Shapefile_layer, burn_values=[burnVal])

        # Build image overviews
        subprocess.call("gdaladdo --config COMPRESS_OVERVIEW DEFLATE " + OutputImage + " 2 4 8 16 32 64", shell=True)
        # Close datasets
        Band = None
        Output = None
        Image = None
        #Shapefile = None

    # Proximity raster of 50 000 m
    for j in range(len(raster_out)):
        print('Making proximity analysis %s' %(raster_out[j]))
        src_ds = gdal.Open(raster_out[j])
        _, filename = os.path.split(raster_list[j])
        name, ending = os.path.splitext(filename)

        srcband = src_ds.GetRasterBand(1)
        dst_filename = os.path.join(proj_path, name+'proximity.tif')
        raster_prox.append(dst_filename)

        drv = gdal.GetDriverByName('GTiff')
        dst_ds = drv.Create(dst_filename,
                            src_ds.RasterXSize, src_ds.RasterYSize, 1,
                            gdal.GetDataTypeByName('Float32'))

        dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
        dst_ds.SetProjection(src_ds.GetProjectionRef())
        dstband = dst_ds.GetRasterBand(1)
        gdal.ComputeProximity(srcband, dstband, ["DISTUNITS=GEO", "maxdist=50000", "nodata=99999"])
        # srcband = None
        # dstband = None
        # src_ds = None
        # dst_ds = None

    return(raster_prox)