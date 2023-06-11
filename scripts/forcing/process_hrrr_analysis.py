###############################################################################
# Download and process NAM data, up to current day
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time, yaml, subprocess
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta
import requests, re
from bs4 import BeautifulSoup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, base_dir, find_last_time

    
## some setups
workdir  = config['base_dir'] + '/forcing/hrrr'
lockfile = 'hrrr.lock'

hrrr_ncep_url = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/'
hrrr_aws_url  = 'https://noaa-hrrr-bdp-pds.s3.amazonaws.com/'

cdocmd = 'cdo -f nc4 -z zip chname,sp,pressfc,\\2t,tmp2m,\\2sh,spfh2m,\\10u,ugrd10m,\\10v,vgrd10m,dswrf,dswrfsfc,dlwrf,dlwrfsfc,prate,pratesfc -remap,latlon_conus_0.03125deg.txt,hrrr_to_0.03125deg_weight.nc'
ncocmd = 'ncwa -a height,height_2'

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

    back_day = curr_day - timedelta(days=2)

    # find last file in archive
    last_nc = find_last_time('analysis/202?????/hrrr_anal_202???????.nc', 'hrrr_anal_%Y%m%d%H.nc')
    #last_nc = datetime(2021, 2, 22, 0, 0, 0, 0, pytz.utc)

    # find last file on server
    last_anal = find_last_hrrr_anal()
    print('Last HRRR analysis time: %s' % last_anal.isoformat())

    timeout = 'timeout 10m '

    if last_nc + timedelta(hours=1) < back_day:
        t1 = back_day
    else:
        t1 = last_nc + timedelta(hours=1)

    t2 = last_anal

    if len(argv)==2:
        t1 = datetime.strptime(argv[0], '%Y%m%d%H')
        t2 = datetime.strptime(argv[1], '%Y%m%d%H')

    t = t1
    while t <= t2:

        fullurl = hrrr_aws_url + t.strftime('hrrr.%Y%m%d/conus/hrrr.t%Hz.wrfsfcf00.grib2')
        cmd = 'get_inv.pl %s.idx | grep -e DLWRF -e DSWRF | get_grib.pl %s tmp15.grb2' % (fullurl, fullurl)
        print(cmd); os.system(timeout+cmd)
        cmd = 'get_inv.pl %s.idx | grep -e "PRES:surface" | get_grib.pl %s tmp11.grb2' % (fullurl, fullurl)
        print(cmd); os.system(timeout+cmd)
        cmd = 'get_inv.pl %s.idx | grep -e "TMP:2 m"      | get_grib.pl %s tmp12.grb2' % (fullurl, fullurl)
        print(cmd); os.system(timeout+cmd)
        cmd = 'get_inv.pl %s.idx | grep -e "UGRD:10 m" -e "VGRD:10 m" | get_grib.pl %s tmp14.grb2' % (fullurl, fullurl)
        print(cmd); os.system(timeout+cmd)
        cmd = 'get_inv.pl %s.idx | grep -e SPFH           | get_grib.pl %s tmp13.grb2' % (fullurl, fullurl)
        print(cmd); os.system(timeout+cmd)
        cmd = 'cat tmp1?.grb2 > tmp1.grb2; rm -f tmp1?.grb2'
        print(cmd); os.system(cmd)

        t0 = t - timedelta(hours=1)
        fullurl = hrrr_aws_url + t0.strftime('hrrr.%Y%m%d/conus/hrrr.t%Hz.wrfsfcf01.grib2')
        cmd = 'get_inv.pl %s.idx | grep PRATE | get_grib.pl %s tmp0.grb2' % (fullurl, fullurl)
        print(cmd); os.system(timeout+cmd)
        cmd = 'cat tmp1.grb2 tmp0.grb2 > tmp2.grb2'
        print(cmd); os.system(cmd)

        fout = 'analysis/%s/hrrr_anal_%s.grb2' % (t.strftime('%Y%m%d'), t.strftime('%Y%m%d%H'))
        dout = os.path.dirname(fout)
        if not os.path.isdir(dout):
            os.system('mkdir -p '+dout)
        cmd = 'wgrib2 tmp2.grb2 | grep -v "TMP:surface" | wgrib2 -i tmp2.grb2 -grib %s' % (fout)
        print(cmd); os.system(cmd)
        cmd = 'rm -f tmp?.grb2 wget-log*'
        print(cmd); os.system(cmd)
        
        # check data integrity
        cmd = 'wgrib2 %s | wc -l' % (fout)
        ret = subprocess.check_output([cmd], shell=True)
        nrecs = ret.decode().split(' ')[-1].rstrip()
        
        if nrecs=='8':
            print('%s is successfully retrieved.' % (fout))
            fnc = fout.replace('grb2', 'nc')
            cmd = '%s %s %s' % (cdocmd, fout, fnc)
            print(cmd); os.system(cmd)
            cmd = '%s %s %s4' % (ncocmd, fnc, fnc)
            print(cmd); os.system(cmd)
            cmd = '/bin/mv %s4 %s' % (fnc, fnc)
            print(cmd); os.system(cmd)
            cmd = '/bin/rm -f %s' % (fout)
            print(cmd); os.system(cmd)

            t = t + timedelta(hours=1)
        else:
            print('%s will be retried in 1 minute.' % (fout))

        time.sleep(6)

    time_finish = time.time()
    print('Total download/process time %.1f seconds' % (time_finish-time_start))
    
    os.system('/bin/rm -f '+lockfile)
    
    return 0


## find latest hrrr analysis
def find_last_hrrr_anal():

    # find last yyyymmdd
    html_text = requests.get(hrrr_ncep_url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    #print(html_text)
    last_ymd = soup.find_all('a', string=re.compile(r'^hrrr.[0-9]+'))[-1].string

    # find last hh
    html_text = requests.get(hrrr_ncep_url+last_ymd+'conus/').text
    soup = BeautifulSoup(html_text, 'html.parser')
    last_h = soup.find_all('a', string=re.compile(r'^hrrr.t[0-9]+z.wrfnatf00.grib2$'))[-1].string

    last_time = datetime.strptime(last_ymd+last_h, 'hrrr.%Y%m%d/hrrr.t%Hz.wrfnatf00.grib2')

    last_time = last_time.replace(tzinfo=pytz.utc) - timedelta(hours=1)

    return last_time
    

if __name__ == '__main__':
    main(sys.argv[1:])
