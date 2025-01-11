''' Convert selected variable(s) in WRF-Hydro NetCDF output file to GeoTIFF file(s)
    One .tif file per variable, per layer, and per time step, with .tif file named as:
        [WRF-Hydro file name]_[variable]_[layer]_[time stamp].tif
    Caution: may create a lot of .tif files!

Usage:
    python convert_to_tiff.py [domain] [WRF-Hydro file] [variable1] [variable2] ...
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, subprocess
from datetime import datetime, timedelta
from osgeo import gdal
import netCDF4 as nc
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config

prj="PROJCS[\"Lambert_Conformal_Conic\",GEOGCS[\"GCS_Sphere\",DATUM[\"D_Sphere\",SPHEROID[\"Sphere\",6370000.0,0.0]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Conformal_Conic_2SP\"],PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],PARAMETER[\"central_meridian\",-97.0],PARAMETER[\"standard_parallel_1\",30.0],PARAMETER[\"standard_parallel_2\",60.0],PARAMETER[\"latitude_of_origin\",40.0],UNIT[\"Meter\",1.0]]"

driver = gdal.GetDriverByName('GTiff')

## main function
def main(argv):

    domain = argv[0]
    fname  = argv[1]
    vnames = []
    for v in range(2, len(argv)):
        vnames.append(argv[v])

    fnc = nc.Dataset(fname, 'r')

    if domain=='cnrfc':
        gtf = [ -2224000, 1000, 0, -640000, 0, 1000 ]
    elif domain=='basins24':
        gtf = [ -2114000, 1000, 0, -260000, 0, 1000 ]
    elif domain=='conus':
        gtf = [ -2304000, 1000, 0, -1920000, 0, 1000 ]

    for vname in vnames:
        
        if len(fnc[vname].shape)==3:
            nlayers = 1
            data = fnc[vname][0, :, :]
        elif len(fnc[vname].shape)==4:
            nlayers = fnc.dimensions[fnc[vname].dimensions[2]].size
            data = fnc[vname][0, :, 0, :]
        elif len(fnc[vname].shape)==2:
            nlayers = 1
            data = fnc[vname][:, :]
        else:
            print('Can\'t convert non-gridded variable to tiff.')
            return 1
        
        ny, nx = data.shape
        no_data = float(fnc[vname]._FillValue)

        if len(fnc[vname].shape)>2 and 'time' in fnc.variables:

            for i in range(fnc['time'].size):

                t = nc.num2date(fnc['time'][i], fnc['time'].units)

                for l in range(nlayers):
                    if nlayers==1:
                        data = fnc[vname][i, :, :]
                        fout = f'{fname}_{vname}_{t:%Y%m%d}.tif'
                    else:
                        data = fnc[vname][i, :, l, :]
                        fout = f'{fname}_{vname}_{l+1:d}_{t:%Y%m%d}.tif'
                
                    ftif = driver.Create(fout, nx, ny, 1, gdal.GDT_Float32)

                    if hasattr(fnc[vname], 'scale_factor') and hasattr(fnc[vname], 'add_offset'):
                        data = data.astype(float)*fnc[vname].scale_factor + fnc[vname].add_offset
                    else:
                        data = data.astype(float)
                    ftif.GetRasterBand(1).WriteArray(data)
                    ftif.GetRasterBand(1).SetNoDataValue(no_data)
        
                    ftif.SetGeoTransform(gtf)
                    ftif.SetProjection(prj)

        else:
            
            data = fnc[vname][:, :]
            fout = f'{fname}_{vname}.tif'
            
            ftif = driver.Create(fout, nx, ny, 1, gdal.GDT_Float32)

            if hasattr(fnc[vname], 'scale_factor') and hasattr(fnc[vname], 'add_offset'):
                data = data.astype(float)*fnc[vname].scale_factor + fnc[vname].add_offset
            else:
                data = data.astype(float)
            ftif.GetRasterBand(1).WriteArray(data)
            ftif.GetRasterBand(1).SetNoDataValue(no_data)
        
            ftif.SetGeoTransform(gtf)
            ftif.SetProjection(prj)
            

    fnc.close()

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])
