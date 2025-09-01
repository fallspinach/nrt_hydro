''' Download and process GFS forecast fields

Usage:
    python process_gfs_fcst.py [yyyymmddhh]
Default values:
    [yyyymmddhh]: GFS forecast initialization time, default to latest on server
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, yaml, subprocess
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta, UTC
import requests, re
from bs4 import BeautifulSoup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

    
## some setups
workdir  = f'{config["base_dir"]}/forcing/gfs'
lockfile = 'gfs.lock'

gfs_ncep_url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod'
gfs_aws_url  = 'https://noaa-gfs-bdp-pds.s3.amazonaws.com'

grep_cmd = 'grep -e DLWRF -e DSWRF -e "PRES:surface" -e ":TMP:2 m above ground" -e "UGRD:10 m above ground" -e "VGRD:10 m above ground" -e "SPFH:2 m above ground" -e "PRATE:surface:.-" -e "PRATE:surface:..-" -e "PRATE:surface:...-"'
cdocmd = 'cdo -f nc4 -z zip chname,sp,pressfc,\\2t,tmp2m,\\2sh,spfh2m,\\10u,ugrd10m,\\10v,vgrd10m,sdswrf,dswrfsfc,sdlwrf,dlwrfsfc,prate,pratesfc -sellonlatbox,-125,-67,25,53 -sellonlatbox,-180,180,-90,90'
ncocmd = 'ncwa -a height,height_2'

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)
    
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

    # find last file on server
    last_fcst = find_last_gfs_fcst()
    print(f'Last GFS forecast initialized at: {last_fcst:%Y-%m-%dT%H}')

    timeout = 'timeout 10m'

    if len(argv)==1:
        t0 = datetime.strptime(argv[0], '%Y%m%d%H')
        t0 = t0.replace(tzinfo=pytz.utc)
    else:
        t0 = curr_day

    dout = f'0.25deg/{t0:%Y%m%d%H}'
    os.makedirs(dout, exist_ok=True)

    print(f'Retrieving GFS forecast initialized at: {t0:%Y-%m-%dT%H}')

    i = 1
    while i <= 384:

        t = t0 + timedelta(hours=i)

        fullurl = f'{gfs_ncep_url}/gfs.{t0:%Y%m%d}/{t0:%H}/atmos/gfs.t{t0:%H}z.pgrb2.0p25.f{i:03d}'
        fout = f'{dout}/gfs_{t:%Y%m%d%H}.grb2'
        cmd = f'get_inv.pl {fullurl}.idx | {grep_cmd} | get_grib.pl {fullurl} {fout}'
        print(cmd); os.system(f'{timeout} {cmd}')

        # check data integrity
        cmd = f'wgrib2 {fout} | wc -l'
        ret = subprocess.check_output([cmd], shell=True)
        nrecs = ret.decode().split(' ')[-1].rstrip()
        
        if nrecs=='8':
            print(f'{fout} is successfully retrieved.')
            fnc = fout.replace('grb2', 'nc')
            cmd = f'{cdocmd} {fout} {fnc}'
            print(cmd); os.system(cmd)
            cmd = f'{ncocmd} {fnc} {fnc}4'
            print(cmd); os.system(cmd)
            cmd = f'/bin/mv {fnc}4 {fnc}'
            print(cmd); os.system(cmd)
            cmd = f'/bin/rm -f {fout}'
            print(cmd); os.system(cmd)

            if i<120:
                i += 1
            else:
                i += 3
        else:
            print(f'{fout} will be retried shortly.')

        time.sleep(2)

    time_finish = time.time()
    print(f'Total download/process time {time_finish-time_start:.1f} seconds')
    
    os.system(f'/bin/rm -f {lockfile}')
    
    return 0


## find GFS forecast
def find_last_gfs_fcst():

    # find last yyyymmdd
    html_text = requests.get(gfs_ncep_url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    #print(html_text)
    last_ymd = soup.find_all('a', string=re.compile(r'^gfs.[0-9]+'))[-1].string

    # find last hh
    html_text = requests.get(f'{gfs_ncep_url}/{last_ymd}').text
    soup = BeautifulSoup(html_text, 'html.parser')
    last_h = soup.find_all('a', string=re.compile(r'^[0-9]+'))[-1].string

    last_time = datetime.strptime(last_ymd+last_h, 'gfs.%Y%m%d/%H/')

    last_time = last_time.replace(tzinfo=pytz.utc)

    return last_time
    

if __name__ == '__main__':
    main(sys.argv[1:])
