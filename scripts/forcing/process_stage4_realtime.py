###############################################################################
# Download and process grb2 version of Stage IV in recent 10 days
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time, yaml, subprocess
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## some setups
workdir  = config['base_dir'] + '/forcing/stage4'
lockfile = 'realtime.lock'

stg4_start = datetime(2022, 11, 30, 0, 0, 0, 0, pytz.utc)
override_flag = True       # override the old output files or not

#stg4_url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/pcpanl/prod/'
stg4_url = 'ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/pcpanl'

cdocmd1 = 'cdo -f nc4 -z zip chname,tp,apcpsfc -remap,latlon_conus_0.04deg.txt' #',stage4_to_0.04deg_weight.nc'
cdocmd3 = 'cdo -f nc4 -z zip chname,tp,apcpsfc -remapcon,latlon_conus_0.04deg.txt' #',stage4_to_0.04deg_weight.nc'
wgetcmd = 'wget -q -r -np -N -nH --cut-dir=5 -R "st4_pr*" -R "st4_ak*" -R "*.gif"'
wgribcmd = 'wgrib2 -undefine out-box -118.1:-118 37:37.1 -csv west_test.tmp'
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
    lastnc_day = find_last_time('realtime/pcpanl.????????/st4_conus.??????????.01h.nc', 'st4_conus.%Y%m%d%H.01h.nc')
    back_day    = curr_time
    
    print('Latest existing data file time %s, update up to %s.' % (lastnc_day.isoformat(), back_day.isoformat()))
    
    t = lastnc_day - timedelta(hours=36)
    while t<back_day-timedelta(hours=1):
        
        steps = ['01']
        if t.hour%6==0:
            steps.append('06')
        if t.hour==12:
            steps.append('24')
            
        for step in steps:
            fgrb = t.strftime('realtime/pcpanl.%Y%m%d/st4_conus.%Y%m%d%H.') + step +'h.grb2'
            fnc  = fgrb.replace('grb2', 'nc')
        
            dgrb = os.path.dirname(fgrb)
            if not os.path.isdir(dgrb):
                os.system('mkdir -p %s' % dgrb)
            
            # download
            cmd = 'wget %s/%s -O %s' % (stg4_url, fgrb.replace('realtime', 'prod'), fgrb)
            print(cmd); os.system(cmd)
            # check whtether CNRFC/NWRFC exist
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
        
        t += timedelta(hours=1)
        
    time_finish = time.time()
    print('Total download/process time %.1f seconds' % (time_finish-time_start))
    
    os.system('/bin/rm -f '+lockfile)
    
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])
