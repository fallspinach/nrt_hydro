''' Download and process NLDAS-2 data, up to 3.5 days behind real time

Usage:
    python process_nldas2.py [yyyymmddhh1] [yyyymmddhh2]
Default values:
    [yyyymmddhh1]: time of last NLDAS-2 analysis on disk
    [yyyymmddhh2]: time of latest NLDAS-2 analysis on server
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, yaml
from glob import glob
import numpy as np
import numpy.ma as ma
import netCDF4 as nc
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

    
## some setups
workdir  = f'{config["base_dir"]}/forcing/nldas2'
lockfile = 'nldas2.lock'

httpshost  = 'hydro1.gesdisc.eosdis.nasa.gov'
httpspath  = 'data/NLDAS/NLDAS_FORA0125_H.2.0'

nld2_path  = 'NLDAS_FORA0125_H.2.0' # path to NLDAS-2 archive folder

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)
    
    if len(argv)==2:
        t1 = datetime.strptime(argv[0], '%Y%m%d%H')
        t2 = datetime.strptime(argv[1], '%Y%m%d%H')
        t1 = t1.replace(tzinfo=pytz.utc)
        t2 = t2.replace(tzinfo=pytz.utc)

    # simple file to avoid running multiple instances of this code
    if os.path.isfile(lockfile):
        print(f'{os.path.basename(__file__)} is exiting: another copy of the program is running.')
        #return 1
    else:
        #os.system(f'touch {lockfile}')
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
    lastnc_day  = find_last_time(nld2_path+'/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.020.nc')
    back_day    = curr_day - timedelta(days=3, hours=12)
    lastnc_day_old = lastnc_day
    
    print(f'Time range to download and process: {(lastnc_day+timedelta(hours=1)):%Y-%m-%dT%H} to {back_day:%Y-%m-%dT%H}.')
    
    #sys.exit("here")
    
    if len(argv)==2:
        lastnc_day = t1 - timedelta(hours=1)
        back_day   = t2
   
    t = lastnc_day + timedelta(hours=1)
    while t <= back_day:

        print(f'Downloading {t:%Y-%m-%d %H:00}')

        # download archive
        fnc = f'NLDAS_FORA0125_H.A{t:%Y%m%d.%H}00.020.nc'
        premo = f'https://{httpshost}/{httpspath}/{t:%Y/%j}'
        parch = f'{nld2_path}/{t:%Y/%j}'
        if not os.path.isdir(parch):
            os.system(f'mkdir -p {parch}')
            
        cmd = f'wget --user=fallspinach --password=TsingHua1911 -q {premo}/{fnc} -O {parch}/{fnc}'
        print(cmd); os.system(cmd)
        
        t = t + timedelta(hours=1)
    
    lastnc_day = find_last_time(nld2_path+'/20??/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.020.nc')
    
    time_finish = time.time()
    print(f'Total download/process time {(time_finish-time_start):.1f} seconds')
    
    os.system(f'/bin/rm -f {lockfile}')

    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
