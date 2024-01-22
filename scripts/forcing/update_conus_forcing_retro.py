import sys, os, math, pytz, time, yaml, subprocess
from glob import glob
import netCDF4 as nc
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

import process_prism
    
## some setups
workdir   = f'{config["base_dir"]}/scripts/forcing'
logdir    = f'{config["base_dir"]}/forcing/log'
prsm_path = f'{config["base_dir"]}/forcing/prism/recent/nc'             # path to PRISM files
nwm_path  = f'{config["base_dir"]}/forcing/nwm'
stnl_path = f'{config["base_dir"]}/forcing/stage4/filled_with_nldas2'   # path to Stage-IV NlDAS-2 merged precip data

prodtype = 'retro'

## main function
def main(argv):
    
    '''main loop'''
    
    # update PRISM data first
    process_prism.main('')
    
    os.chdir(workdir)

    # find the last nc file
    ncfiles = glob(f'{prsm_path}/PRISM_tmean_stable_4kmD2_*.nc')
    ncfiles.sort()
    f = nc.Dataset(ncfiles[-1], 'r')
    last_prsm = datetime.strptime(str(nc.num2date(f['time'][-1], f['time'].units)), '%Y-%m-%d %H:%M:%S')
    last_prsm = last_prsm.replace(tzinfo=pytz.utc)
    f.close()
    first_prsm = datetime(1895, 1, 1, tzinfo=pytz.utc)

    # find last retro forcing in NWM format
    last_prod = find_last_time(f'{nwm_path}/1km/conus/retro/????/????????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1')
    last_stnl = find_last_time(f'{stnl_path}/????/st4nl2_????????.nc', 'st4nl2_%Y%m%d.nc') - timedelta(days=1)
    
    print(f'Last PRISM "recent history" data:  {last_prsm:%Y-%m-%dT%H}')
    print(f'Last StageIV/NLDAS2 merged precip: {last_stnl:%Y-%m-%dT%H}')
    print(f'Last retro forcing file:           {last_prod:%Y-%m-%dT%H}')

    if len(argv)==2:
        t1 = datetime.strptime(argv[0], '%Y%m%d%H')
        t2 = datetime.strptime(argv[1], '%Y%m%d%H')
        t1 = t1.replace(tzinfo=pytz.utc)
        t2 = t2.replace(tzinfo=pytz.utc)
    else:
        t1 = last_prod + timedelta(days=1)
        t2 = last_prsm

    if t1<first_prsm:
        t1 = first_prsm
    if t2>last_prsm:
        t2 = last_prsm
    
    print(f'Processing {prodtype} data from {t1:%Y-%m-%dT%H} to {t2:%Y-%m-%dT%H}.')

    if last_stnl<last_prsm:
        
        print(f'PRISM ({last_prsm:%Y-%m-%dT%H}) is newer than StageIV-NLDAS2 merged ({last_stnl:%Y-%m-%dT%H}) - bring the latter up to date first.')
        
        tt1 = last_stnl + timedelta(days=1)
        tt2 = last_prsm + timedelta(days=1)
        cmd00 = 'sbatch -p shared -n 12'
        cmd11 = 'sbatch -p shared -n 2'
        cmd22 = 'unset SLURM_MEM_PER_NODE; mpirun -np 12 python fill_stage4_with_nldas2.py'
        cmd33 = 'unset SLURM_MEM_PER_NODE; mpirun -np  2 python calc_shifted_daily.py'
        
        # Merge
        cmd = f'{cmd00} -t 00:30:00 -J st4nl2 --wrap="{cmd22} {t1:%Y%m%d} {t2:%Y%m%d} {prodtype}" -o {logdir}/st4nl2_{t1:%Y%m%d}_{t2:%Y%m%d}.txt'; print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid1 = ret.decode().split(' ')[-1].rstrip()
        print(f'StageIV & NLDAS-2 precip merging job ID is: {jid1}')

        dep = f'-d afterok:{jid1}'

    else:
        dep = ''
        
    cmd0 = 'sbatch -p shared -n 12 '
    cmd1 = 'sbatch -p compute -N 1 '
    cmd2 = 'unset SLURM_MEM_PER_NODE; mpirun -np 12 python create_conus_forcing.py'
    cmd3 = 'unset SLURM_MEM_PER_NODE; mpirun -np 12 python mergetime_subset.py'
    
    # retro forcing update
    ndays = (t2+timedelta(days=1)-t1).days
    trun = (datetime(1,1,1)+timedelta(minutes=ndays*4+1)).strftime('%H:%M:%S')
    cmd = f'{cmd0} {dep} -t {trun} -J retrof --wrap="{cmd2} {t1:%Y%m%d%H} {t2:%Y%m%d%H} {prodtype}" -o {logdir}/retrof_{t1:%Y%m%d%H}_{t2:%Y%m%d%H}.txt'; print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid3 = ret.decode().split(' ')[-1].rstrip()
    print(f'Retro forcing job ID is: {jid3}')
    
    # merge hourly files to daily and subset/reproject
    trun = (datetime(1,1,1)+timedelta(minutes=ndays*3+10)).strftime('%H:%M:%S')
    cmd = f'{cmd0} -d afterok:{jid3} -t {trun} -J mergesub --wrap="{cmd3} {t1:%Y%m%d} {t2:%Y%m%d} {prodtype}"  -o {logdir}/mergesub_retro_{t1:%Y%m%d}_{t2:%Y%m%d}.txt'; print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid4 = ret.decode().split(' ')[-1].rstrip()
    print(f'Mergetime and subset retro forcing job ID is: {jid4}')
    
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

