''' Download and process Stage IV archive version

Usage:
    python process_stage4_archive.py
Default values:
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, yaml, subprocess
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta, UTC
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## some setups
workdir  = f'{config["base_dir"]}/forcing/stage4'
lockfile = 'archive.lock'

ftphost  = 'ftp.emc.ncep.noaa.gov'
ftppath  = 'mmb/sref/st2n4.arch'

stg4_start = datetime(2020, 7, 20, 0, 0, 0, 0, pytz.utc)
stg4_old   = '/cw3e/mead/projects/cwp101/wrf_hydro/forcing/stage4/archive/grb2'
override_flag = True       # override the old output files or not
copyold_flag  = False      # copy from old instead of download

cdocmd1 = 'cdo -f nc4 -z zip chname,tp,apcpsfc -remap,latlon_conus_0.04deg.txt' #',stage4_to_0.04deg_weight.nc'
cdocmd3 = 'cdo -f nc4 -z zip chname,tp,apcpsfc -remapcon,latlon_conus_0.04deg.txt' #',stage4_to_0.04deg_weight.nc'
cdocmd2 = 'cdo -s outputtab,value -fldsum -gtc,-20'

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)

    copyold_flag  = False
    if len(argv)>0:
        if argv[0] == 'copy':
            copyold_flag  = True
    
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
    curr_time = datetime.now(UTC)
    curr_time = curr_time.replace(tzinfo=pytz.utc)
    
    curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
    
    # find the last nc file
    lastnc_day = find_last_time('archive/20??/st4_conus.20??????.01h.nc', 'st4_conus.%Y%m%d.01h.nc')
    back_day    = curr_day - timedelta(days=9)
    
    print(f'Time range to download and process: {(lastnc_day+timedelta(days=1)):%Y-%m-%dT%H} to {back_day:%Y-%m-%dT%H}.')
    
    #sys.exit('here')
   
    t = lastnc_day + timedelta(days=1) 
    while t <= back_day:

        print(f'Downloading {t:%Y-%m-%d}')

        # download archive
        fgrb = f'ST4.{t:%Y%m%d}'
        premo = f'https://{ftphost}/{ftppath}/{t:%Y%m}'
        parch = f'archive/{t:%Y}'
        if not os.path.isdir(parch):
            os.system(f'mkdir -p {parch}')

        if not copyold_flag:
            # download
            #cmd = f'wget {premo}/{fgrb} -O {parch}/{fgrb}'
            cmd = f'wget {premo}/{fgrb}.tar -O {parch}/{fgrb}'
            print(cmd); os.system(cmd)

            # process it
            cmd = f'wgrib2 {parch}/{fgrb} | grep "0-1 hour" | head -24 | wgrib2 -i -grib {parch}/st4_conus.{t:%Y%m%d}.01h.grb2 {parch}/{fgrb}'
            print(cmd); os.system(cmd)
            cmd = f'tar -xf {parch}/{fgrb} --wildcards "st4_conus*06h.grb2"; cat st4_conus*06h.grb2 > {parch}/st4_conus.{t:%Y%m%d}.06h.grb2; rm -f st4_conus*06h.grb2'
            print(cmd); os.system(cmd)
        else:
            # copy from old path
            cmd = f'/bin/cp -a {stg4_old}/{t:%Y%m}/st4_conus.{t:%Y%m%d}.0?h.grb2 {parch}/'
            print(cmd); os.system(cmd)
        
        for step in ['01', '06']:
            
            fgrb = f'{parch}/st4_conus.{t:%Y%m%d}.{step}h.grb2'
            fnc = fgrb.replace('grb2', 'nc')
        
            cmd = f'{cdocmd2} {fgrb} | tail -1 | tr -d " "'
            print(cmd)
            ret = subprocess.check_output([cmd], shell=True)
            npix = ret.decode().split(' ')[-1].rstrip()
            fwt = f'stage4_to_0.04deg_weight_{npix}.nc'
            # remap to 0.04 deg
            if os.path.isfile(fwt):
                cmd = f'{cdocmd1},{fwt} {fgrb} {fnc}'
            else:
                cmd = f'{cdocmd3} {fgrb} {fnc}'
            print(cmd); os.system(cmd)
            cmd = f'/bin/rm -f {fgrb}'
            print(cmd); os.system(cmd)


        t = t + timedelta(days=1)
    
    time_finish = time.time()
    print(f'Total download/process time {(time_finish-time_start):.1f} seconds')
    
    os.system(f'/bin/rm -f {lockfile}')

    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
