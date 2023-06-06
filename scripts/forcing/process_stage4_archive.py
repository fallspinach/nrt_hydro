###############################################################################
# Download and process grb2 version of Stage IV archive (not recent 10 days)
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time, yaml, subprocess
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta
from utilities import find_last_time


fconfig = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/config.yaml'
with open(fconfig, 'r') as f:
    config_all = yaml.safe_load(f)
    config     = config_all[config_all['platform']]['forcing']
    base_dir   = config_all[config_all['platform']]['base_dir']

## some setups
workdir  = base_dir + '/forcing/stage4'
lockfile = 'archive.lock'

ftphost  = 'ftp.emc.ncep.noaa.gov'
ftppath  = 'mmb/sref/st2n4.arch'

stg4_start = datetime(2020, 7, 20, 0, 0, 0, 0, pytz.utc)
override_flag = True       # override the old output files or not

cdocmd1 = 'cdo -f nc4 -z zip chname,tp,apcpsfc -remap,latlon_conus_0.04deg.txt' #',stage4_to_0.04deg_weight.nc'
cdocmd2 = 'cdo -s outputtab,value -fldsum -gtc,-20'

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
    
    # find the last nc file
    lastnc_day = find_last_time('archive/20????/st4_conus.20??????.01h.nc', 'st4_conus.%Y%m%d.01h.nc')
    back_day    = curr_day - timedelta(days=9)
    
    print('Time range to download and process: %s to %s.' % ((lastnc_day+timedelta(days=1)).isoformat(), back_day.isoformat()))
    
    #sys.exit('here')
   
    t = lastnc_day + timedelta(days=1) 
    while t <= back_day:

        print(t.strftime('Downloading %Y-%m-%d'))

        # download archive
        fgrb = t.strftime('ST4.%Y%m%d')
        premo = 'ftp://%s/%s/%s' % (ftphost, ftppath, t.strftime('%Y%m'))
        parch = t.strftime('archive/%Y%m')
        if not os.path.isdir(parch):
            os.system('mkdir -p '+parch)
        #cmd = 'wget %s/%s -O %s/%s' % (premo, fgrb, parch, fgrb)
        cmd = 'wget %s/%s.tar -O %s/%s' % (premo, fgrb, parch, fgrb)
        print(cmd); os.system(cmd)

        # process it
        cmd = 'wgrib2 %s/%s | grep "0-1 hour" | head -24 | wgrib2 -i -grib %s/st4_conus.%s.01h.grb2 %s/%s' % (parch, fgrb, parch, t.strftime('%Y%m%d'), parch, fgrb)
        print(cmd); os.system(cmd)
        cmd = 'tar -xf %s/%s --wildcards "st4_conus*06h.grb2"; cat st4_conus*06h.grb2 > %s/st4_conus.%s.06h.grb2; rm -f st4_conus*06h.grb2' % (parch, fgrb, parch, t.strftime('%Y%m%d'))
        print(cmd); os.system(cmd)
        
        for step in ['01', '06']:
            
            fgrb = '%s/st4_conus.%s.%sh.grb2' % (parch, t.strftime('%Y%m%d'), step)
            fnc = fgrb.replace('grb2', 'nc')
        
            cmd = '%s %s | tail -1 | tr -d " "' % (cdocmd2, fgrb)
            print(cmd)
            ret = subprocess.check_output([cmd], shell=True)
            npix = ret.decode().split(' ')[-1].rstrip()
            fwt = 'stage4_to_0.04deg_weight_%s.nc' % (npix)
            # remap to 0.04 deg
            if os.path.isfile(fwt):
                cmd = '%s,%s %s %s' % (cdocmd1, fwt, fgrb, fnc)
            else:
                cmd = '%s %s %s' % (cdocmd3, fgrb, fnc)
            print(cmd); os.system(cmd)
            cmd = '/bin/rm -f %s' % (fgrb)
            print(cmd); os.system(cmd)


        t = t + timedelta(days=1)
    
    time_finish = time.time()
    print('Total download/process time %.1f seconds' % (time_finish-time_start))
    
    os.system('/bin/rm -f '+lockfile)

    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
