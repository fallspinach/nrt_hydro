''' Process West-WRF deterministic forecast data into WRF-Hydro format

Usage:
    mpirun -np [# of procs] python process_wwrf.py [fcst_length] [fcst_date]
Default values:
    [# of procs]: must specify
    [fcst_length]: 10
    [fcst_date]: latest West-WRF deterministic forecast
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, yaml, subprocess
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
from calendar import monthrange
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups
wwrfdir  = f'{config["base_dir"]}/forcing/wwrf'

fcst_init   = 'ecmwf'
fcst_domain = '01'
fcst_length = 10

tmpdir = f'/scratch/{os.getenv("USER")}/{config["node_scratch"]}{os.getenv("SLURM_JOBID")}'

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(wwrfdir)
    
    # keep the time
    time_start = time.time()
    
    # get current UTC time
    curr_time = datetime.now(UTC)
    curr_time = curr_time.replace(tzinfo=pytz.utc)
    
    curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
    
    # figure out the water year
    wy      = curr_day.year if curr_day.month>=9 else curr_day.year-1
    fcst_dir = f'links/NRT/{wy:d}-{wy+1:d}/NRT_{fcst_init}'
    
    # find the latest West-WRF forecast
    latest_day = find_last_time(fcst_dir+'/????????00', '%Y%m%d%H')
    fcst_length = 10
    
    last_day = latest_day + timedelta(days=fcst_length)
    fww = f'{fcst_dir}/{latest_day:%Y%m%d%H}/cf/wrfcf_{fcst_init}_d{fcst_domain}_{last_day:%Y-%m-%d_%H}_00_00.nc'
    ntmp = len(glob(f'{fcst_dir}/{latest_day:%Y%m%d%H}/cf/wrfcf_{fcst_init}_d{fcst_domain}_*_temp.nc'))
    if (not os.path.isfile(fww)) or ntmp>0:
        latest_day -= timedelta(hours=24)
    
    if len(argv)==1:
        latest_day = datetime.strptime(argv[0], '%Y%m%d%H')
        latest_day = latest_day.replace(tzinfo=pytz.utc)
        
    print(f'Latest forecast to process: {latest_day:%Y-%m-%dT%H}.')

    # remap to 0.05 deg
    t1 = latest_day + timedelta(hours=1)
    t2 = latest_day + timedelta(days=fcst_length)
    np = fcst_length
    python_script = '../../scripts/utils/run_cmd_in_time_mpi.py'
    cdocmd = f'cdo -O -f nc4 -z zip remap,domain/latlon_wus_0.05deg.txt,domain/cdo_weights_d01_wus_0.05deg.nc -selname,p_sfc,T_2m,q_2m,LW_d,SW_d,precip_bkt,u_10m,v_10m'
    cmd0 = f'{cdocmd} {fcst_dir}/{latest_day:%Y%m%d%H}/cf/wrfcf_{fcst_init}_d{fcst_domain}_%Y-%m-%d_%H_00_00.nc 0.05deg/%Y/%Y%m/wrfcf_d{fcst_domain}_%Y%m%d%H.nc'
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {np} python {python_script} hourly {t1:%Y%m%d%H} {t2:%Y%m%d%H} "{cmd0}"'
    flog = f'../log/remap_wwrf_0.05deg_{latest_day:%Y%m%d%H}.txt'
    cmd = f'sbatch -t 00:15:00 --nodes=1 -p {config["part_shared"]} --ntasks-per-node={np} -J wwrf005 --wrap=\'{cmd1}\' -o {flog}'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid1 = jid
    print(f'WWRF remapping to 0.05deg job ID is: {jid}')
        
    modelid = 'nwm_v3'
    domain = 'cnrfc'
    
    # downscale to 0.01 deg
    os.chdir(f'{config["base_dir"]}/forcing/nwm/')
    python_script = '../../scripts/utils/run_grads_in_time_mpi.py'
    grads_script  = '../../scripts/forcing/downscale_wwrf_0.01deg.gs'
    [lon1, lon2, lat1, lat2] = config[modelid][domain]['lonlatbox']
    grads_args    = f'../wwrf/0.05deg/wwrf_d01_0.05deg.ctl {lon1} {lon2} {lat1} {lat2} ../wwrf/0.01deg/{domain}'
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {np} python {python_script} hourly {t1:%Y%m%d%H} {t2:%Y%m%d%H} {grads_script} "{grads_args}"'
    flog = f'../log/dnsc_wwrf_{domain}_{latest_day:%Y%m%d%H}.txt'
    cmd = f'sbatch -d afterok:{jid1} -t 00:20:00 --nodes=1 -p {config["part_shared"]} --ntasks-per-node={np} -J dnscwwrf --wrap=\'{cmd1}\' -o {flog}'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid2 = jid
    print(f'WWRF downscaling ({domain}) job ID is: {jid}')
    
    # mergetime and remap to NWM 1km grid
    os.chdir(f'{config["base_dir"]}/forcing/wwrf/')
    os.makedirs(f'1km/{domain}/{t1:%Y}', exist_ok=True)
    os.makedirs(f'1km/{domain}/{t2:%Y}', exist_ok=True)
    python_script = '../../scripts/utils/run_cmd_in_time_mpi.py'
    cmd0 = f'cdo -f nc4 -z zip remap,domain/scrip_{domain}_bilinear.nc,domain/cdo_weights_{domain}.nc [ -mergetime 0.01deg/{domain}/%Y/%Y%m/%Y%m%d??.LDASIN_DOMAIN1 ] 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1.nc; cdo -f nc4 -z zip add 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1.nc domain/xmask0_{domain}.nc 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1; /bin/rm -f 1km/{domain}/%Y/%Y%m%d.LDASIN_DOMAIN1.nc'
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {np} python {python_script} daily {t1:%Y%m%d} {t2:%Y%m%d} "{cmd0}"'
    flog = f'../log/remap_wwrf_{domain}_{latest_day:%Y%m%d%H}.txt'
    cmd = f'sbatch -d afterok:{jid2} -t 00:20:00 --nodes=1 -p {config["part_shared"]} --ntasks-per-node={np} -J rempwwrf --wrap=\'{cmd1}\' -o {flog}'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid3 = jid
    print(f'WWRF merging/remapping ({domain}) job ID is: {jid}')
    
    # aggregate to daily (no monthly at the moment)
    os.makedirs(f'1km_daily/{domain}/{t1:%Y}', exist_ok=True)
    os.makedirs(f'1km_daily/{domain}/{t2:%Y}', exist_ok=True)
    t = t1
    while t <= t2:
        md = monthrange(t.year, t.month)[1]        
        cmd0 = f'cdo -O -f nc4 -z zip seldate,{t1:%Y-%m-%d},{t2:%Y-%m-%d} -delete,timestep=1,{md+2} -daymean -shifttime,-1hour [ -mergetime 1km_hourly/{domain}/{t:%Y/%Y%m}??.LDASIN_DOMAIN1 ] 1km_daily/{domain}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.daily'
        flog = f'../log/agg_wwrf_{domain}_{latest_day:%Y%m%d%H}.txt'
        cmd = f'sbatch -d afterok:{jid3} -t 00:15:00 --nodes=1 -p {config["part_shared"]} --ntasks-per-node=1 -J agg_wwrf --wrap="{cmd0}" -o {flog}'
        print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid = ret.decode().split(' ')[-1].rstrip(); jid3 = jid
        print(f'WWRF forcing aggregation ({domain}, {t:%Y%m}) job ID is: {jid}')
        t += relativedelta(months=1)
        
    time_finish = time.time()
    print('Total processing time %.1f seconds' % (time_finish-time_start))
    
    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
