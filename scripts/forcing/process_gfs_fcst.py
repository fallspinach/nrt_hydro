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
from utilities import config, find_last_time, replace_brackets

    
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
        t0 = last_fcst

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

    # create GrADS control file
    os.system(f'/bin/cp {dout}/../gfs_fcst.ctl.tpl {dout}/gfs_fcst.ctl')
    t1 = t0 + timedelta(hours=1)
    replace_brackets(f'{dout}/gfs_fcst.ctl', {'START_TIME': f'{t1:%Hz%d%b%Y}'})
    t2 = t0 + timedelta(hours=384)

    modelid = 'nwm_v3'
    domain = 'cnrfc'
    
    # downscale to 0.01 deg
    os.chdir(f'{config["base_dir"]}/forcing/nwm/')
    np = 12
    python_script = '../../scripts/utils/run_grads_in_time_mpi.py'
    grads_script  = '../../scripts/forcing/downscale_gfs_0.01deg.gs'
    [lon1, lon2, lat1, lat2] = config[modelid][domain]['lonlatbox']
    grads_args    = f'../gfs/0.25deg/{t0:%Y%m%d%H}/gfs_fcst.ctl {lon1} {lon2} {lat1} {lat2} ../gfs/0.01deg/{domain}'
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {np} python {python_script} hourly {t1:%Y%m%d%H} {t2:%Y%m%d%H} {grads_script} "{grads_args}"'
    flog = f'../log/dnsc_gfs_{domain}_{t0:%Y%m%H}.txt'
    cmd = f'sbatch -t 00:20:00 --nodes=1 -p {config["part_shared"]} --ntasks-per-node={np} -J dnscgfs --wrap=\'{cmd1}\' -o {flog}'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid1 = jid
    print(f'GFS downscaling ({domain}) job ID is: {jid}')
    
    # mergetime and remap to NWM 1km grid
    os.chdir(f'{config["base_dir"]}/forcing/gfs/')
    os.makedirs(f'1km/{domain}/{t1:%Y}', exist_ok=True)
    os.makedirs(f'1km/{domain}/{t2:%Y}', exist_ok=True)
    np = 17
    python_script = '../../scripts/utils/run_cmd_in_time_mpi.py'
    #cdo -f nc4 -z zip remap,domain/scrip_cnrfc_bilinear.nc,domain/cdo_weights_cnrfc.nc [ -mergetime 0.01deg/cnrfc/2025/202509/20250901??.LDASIN_DOMAIN1 ] 1km/cnrfc/2025/20250901.LDASIN_DOMAIN1.nc; cdo -f nc4 -z zip add 1km/cnrfc/2025/20250901.LDASIN_DOMAIN1.nc domain/xmask0_cnrfc.nc 1km/cnrfc/2025/20250901.LDASIN_DOMAIN1; /bin/rm -f 1km/cnrfc/2025/20250901.LDASIN_DOMAIN1.nc
    cmd0 = f'cdo -f nc4 -z zip remap,domain/scrip_{domain}_bilinear.nc,domain/cdo_weights_{domain}.nc [ -mergetime 0.01deg/{domain}/%Y/%Y%m/%Y%m%d??.LDASIN_DOMAIN1 ] 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1.nc; cdo -f nc4 -z zip add 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1.nc domain/xmask0_{domain}.nc 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1; /bin/rm -f 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1.nc'
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {np} python {python_script} daily {t1:%Y%m%d} {t2:%Y%m%d} "{cmd0}"'
    flog = f'../log/remap_gfs_{domain}_{t0:%Y%m%H}.txt'
    cmd = f'sbatch -d afterok:{jid1} -t 00:20:00 --nodes=1 -p {config["part_shared"]} --ntasks-per-node={np} -J remapgfs --wrap=\'{cmd1}\' -o {flog}'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid2 = jid
    print(f'GFS merging/remapping ({domain}) job ID is: {jid}')
    
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
