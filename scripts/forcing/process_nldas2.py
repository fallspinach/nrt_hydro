###############################################################################
# Download and process NLDAS-2 data, up to 3.5 days behind real time
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time, yaml
from glob import glob
import numpy as np
import numpy.ma as ma
import netCDF4 as nc
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

    
## some setups
workdir  = config['base_dir'] + '/forcing/nldas2'
lockfile = 'nldas2.lock'

httpshost  = 'hydro1.gesdisc.eosdis.nasa.gov'
httpspath  = 'data/NLDAS/NLDAS_FORA0125_H.002'

nld2_path  = 'NLDAS_FORA0125_H.002' # path to NLDAS-2 archive folder
override_flag = True       # override the old output files or not

varinfo = [
    {'name': 'apcpsfc',     'id': '61',  'long_name': 'surface Total precipitation',                              'units': 'kg/m^2'},
    {'name': 'cape180_0mb', 'id': '157', 'long_name': '180-0 mb above gnd Convective available potential energy', 'units': 'J/Kg'},
    {'name': 'dlwrfsfc',    'id': '205', 'long_name': 'surface Downward longwave radiation flux',                 'units': 'W/m^2'},
    {'name': 'dswrfsfc',    'id': '204', 'long_name': 'surface Downward shortwave radiation flux',                'units': 'W/m^2'},
    {'name': 'pevapsfc',    'id': '228', 'long_name': 'surface Potential evaporation',                            'units': 'Kg/m^2'},
    {'name': 'pressfc',     'id': '1',   'long_name': 'surface Pressure',                                         'units': 'Pa'},
    {'name': 'spfh2m',      'id': '51',  'long_name': '2 m above ground Specific humidity',                       'units': 'kg/kg'},
    {'name': 'tmp2m',       'id': '11',  'long_name': '2 m above ground Temperature',                             'units': 'K'},
    {'name': 'ugrd10m',     'id': '33',  'long_name': '10 m above ground u wind',                                 'units': 'm/s'},
    {'name': 'vgrd10m',     'id': '34',  'long_name': '10 m above ground v wind',                                 'units': 'm/s'},
    {'name': 'var153sfc',   'id': '153', 'long_name': 'surface undefined',                                        'units': ''}
]

cdocmd1 = 'cdo -f nc4 -z zip chname,' +  ','.join(['var'+v['id']+','+v['name'] for v in varinfo])
ncocmd1 = 'ncatted ' + ' '.join(['-a long_name,'+v['name']+',a,c,"'+v['long_name']+'"' for v in varinfo]) + ' ' + ' '.join(['-a units,'+v['name']+',a,c,"'+v['units']+'"' for v in varinfo])
ncocmd2 = 'ncwa -a height,height_2,lev'

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)
    
    # simple file to avoid running multiple instances of this code
    if os.path.isfile(lockfile):
        print('%s is exiting: another copy of the program is running.' % os.path.basename(__file__))
        #return 1
    else:
        #os.system('touch '+lockfile)
        pass
    
    # keep the time
    time_start = time.time()
    
    # get current UTC time
    curr_time = datetime.utcnow()
    curr_time = curr_time.replace(tzinfo=pytz.utc)
    
    curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
    
    #print(workdir)
    #print(cdocmd)
    #print(ncocmd)
    
    # find the last nc file
    lastnc_day  = find_last_time(nld2_path+'/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.002.nc')
    back_day    = curr_day - timedelta(days=3, hours=12)
    lastnc_day_old = lastnc_day
    
    print('Time range to download and process: %s to %s.' % ((lastnc_day+timedelta(hours=1)).isoformat(), back_day.isoformat()))
    
    #sys.exit("here")
   
    t = lastnc_day + timedelta(hours=1) 
    while t <= back_day:

        print(t.strftime('Downloading %Y-%m-%d %H:00'))

        # download archive
        fgrb = 'NLDAS_FORA0125_H.A'+t.strftime('%Y%m%d.%H')+'00.002.grb'
        fnc  = fgrb.replace('grb', 'nc')
        premo = 'https://%s/%s/%s' % (httpshost, httpspath, t.strftime('%Y/%j'))
        parch = '%s/%s' % (nld2_path, t.strftime('%Y/%j'))
        if not os.path.isdir(parch):
            os.system('mkdir -p '+parch)
        cmd = 'wget --user=fallspinach --password=TsingHua1911 -q %s/%s -O %s/%s' % (premo, fgrb, parch, fgrb)
        print(cmd); os.system(cmd)
        cmd = '%s %s/%s %s/%s' % (cdocmd1, parch, fgrb, parch, fnc)
        print(cmd); os.system(cmd)
        cmd = '%s %s/%s' % (ncocmd1, parch, fnc)
        print(cmd); os.system(cmd)
        cmd = '%s %s/%s %s/%s4' % (ncocmd2, parch, fnc, parch, fnc)
        print(cmd); os.system(cmd)
        cmd = '/bin/mv %s/%s4 %s/%s' % (parch, fnc, parch, fnc)
        print(cmd); os.system(cmd)
        fix_latlon('%s/%s' % (parch, fnc))
        cmd = '/bin/rm -f %s/%s' % (parch, fgrb)
        print(cmd); os.system(cmd)

        t = t + timedelta(hours=1)
    
    lastnc_day = find_last_time(nld2_path+'/20??/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.002.nc')
    
    time_finish = time.time()
    print('Total download/process time %.1f seconds' % (time_finish-time_start))
    
    os.system('/bin/rm -f '+lockfile)

    return 0
    
def fix_latlon(fnldas2):

    f = nc.Dataset(fnldas2, 'a')
    f.variables['lon'][:] = np.linspace(-124.9375, -67.0625, 464)
    f.variables['lat'][:] = np.linspace(25.0625, 52.9375, 224)
    f.sync()
    f.close()

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])
