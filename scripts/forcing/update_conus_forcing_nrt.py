''' Update NRT WRF-Hydro forcing

Usage:
    python update_conus_forcing_nrt.py [backdate]
Default values:
    [backdate]: date of last Stage IV archive product on disk
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, math, pytz, time, yaml, subprocess
from glob import glob
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

import process_nldas2
import process_stage4_archive
import process_stage4_realtime
import process_hrrr_analysis

## some setups
workdir   = f'{config["base_dir"]}/scripts/forcing'
logdir    = f'{config["base_dir"]}/forcing/log'
stg4_path = f'{config["base_dir"]}/forcing/stage4'                      # path to Stage IV files
nld2_path = f'{config["base_dir"]}/forcing/nldas2/NLDAS_FORA0125_H.2.0' # path to NLDAS-2 archive folder
hrrr_path = f'{config["base_dir"]}/forcing/hrrr/analysis'               # path to HRRR analysis
out_path  = f'{config["base_dir"]}/forcing/nwm/1km'

globus_path = 'm3pan@skyriver.ucsd.edu:Hydro/wrf_hydro/forcing/nwm/conus'

prodtype = 'nrt'

## main function
def main(argv):
    
    '''main loop'''

    # update all external data first
    if len(argv)==0:
        
        print('Updating NLDAS2 data archive ...')
        process_nldas2.main('')
    
        print('Updating Stage IV non-realtime data archive ...')
        process_stage4_archive.main('')
    
        print('Updating Stage IV realtime data archive ...')
        process_stage4_realtime.main('')
    
        print('Updating HRRR Analysis data archive ...')
        process_hrrr_analysis.main('')
        
    else:
        
        t0 = datetime.strptime(argv[0], '%Y%m%d')
        t0 = t0.replace(tzinfo=pytz.utc)
        
    os.chdir(workdir)

    last_st4a = find_last_time(f'{stg4_path}/archive/202?/ST4.20??????', 'ST4.%Y%m%d') + timedelta(hours=23)
    last_st4r = find_last_time(f'{stg4_path}/realtime/pcpanl.????????/st4_conus.??????????.01h.nc', 'st4_conus.%Y%m%d%H.01h.nc')
    last_nld2 = find_last_time(f'{nld2_path}/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.020.nc')
    last_hrrr = find_last_time(f'{hrrr_path}/202?????/hrrr_anal_202???????.nc', 'hrrr_anal_%Y%m%d%H.nc')
    
    print(f'Last Stage-IV archive:  {last_st4a:%Y-%m-%dT%H}')
    print(f'Last Stage-IV realtime: {last_st4r:%Y-%m-%dT%H}')
    print(f'Last NLDAS-2 data:      {last_nld2:%Y-%m-%dT%H}')
    print(f'Last HRRR analysis:     {last_hrrr:%Y-%m-%dT%H}')
    
    cmd0 = 'sbatch -A cwp101 -p cw3e-shared --nodes=1 --ntasks-per-node=12 --mem=30G'
    cmd1 = 'sbatch -A cwp101 -p cw3e-shared --nodes=1 --ntasks-per-node=6 --mem=32G'
    cmd2 = 'unset SLURM_MEM_PER_NODE SLURM_MEM_PER_CPU; mpirun -np 12 python create_conus_forcing.py'
    cmd3 = 'unset SLURM_MEM_PER_NODE SLURM_MEM_PER_CPU; mpirun -np 6 python mergetime_subset.py'
        
    # NLDAS-2 + Stage-IV archive update
    if len(argv)==0:
        t1 = last_st4a - timedelta(hours=47)
    else:
        t1 = t0
    t2 = last_st4a
    cmd = f'{cmd0} -t 01:30:00 -J nld2st4a --wrap="{cmd2} {t1:%Y%m%d%H} {t2:%Y%m%d%H} {prodtype}" -o {logdir}/nld2st4a_{t1:%Y%m%d%H}_{t2:%Y%m%d%H}.txt'; print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid1 = ret.decode().split(' ')[-1].rstrip()
    print(f'NLDAS-2 + StageIV archive forcing job ID is: {jid1}')

    # NLDAS-2 + Stage-IV realtime until end of NLDAS-2
    t1 = last_st4a + timedelta(hours=1); t2 = last_nld2
    cmd = f'{cmd0} -t 01:00:00 -J nld2st4r --wrap="{cmd2} {t1:%Y%m%d%H} {t2:%Y%m%d%H} {prodtype}" -o {logdir}/nld2st4r_{t1:%Y%m%d%H}_{t2:%Y%m%d%H}.txt'; print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid2 = ret.decode().split(' ')[-1].rstrip()
    print(f'NLDAS-2 + StageIV realtime forcing job ID is: {jid2}')

    # HRRR + Stage-IV until end of HRRR analysis
    t1 = last_nld2 + timedelta(hours=1); t2 = last_hrrr
    cmd = f'{cmd0} -t 01:00:00 -J hrrrst4r --wrap="{cmd2} {t1:%Y%m%d%H} {t2:%Y%m%d%H} {prodtype}" -o {logdir}/hrrrst4r_{t1:%Y%m%d%H}_{t2:%Y%m%d%H}.txt'; print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid3 = ret.decode().split(' ')[-1].rstrip()
    print(f'HRRR + StageIV realtime forcing job ID is: {jid3}')
    
    # merge hourly files to daily and subset/reproject
    if len(argv)==0:
        t1 = last_st4a - timedelta(hours=47)
    else:
        t1 = t0
    t2 = last_hrrr
    cmd = f'{cmd1} -d afterok:{jid1}:{jid2}:{jid3} -t 04:00:00 -J mergesub --wrap="{cmd3} {t1:%Y%m%d} {t2:%Y%m%d} {prodtype}"  -o {logdir}/mergesub_{t1:%Y%m%d}_{t2:%Y%m%d}.txt'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid4 = ret.decode().split(' ')[-1].rstrip()
    print(f'Mergetime and subset forcing job ID is: {jid4}')

    # aggregate hourly forcing data to daily and monthly and subset them
    nmons = 1 if t1.month==t2.month else 2
    nmem  = 25*nmons
    cmd5 = f'sbatch -A cwp101 -p cw3e-shared --nodes=1 --ntasks-per-node={nmons:d} --mem={nmem:d}G'
    cmd6 = f'unset SLURM_MEM_PER_NODE SLURM_MEM_PER_CPU; mpirun -np {nmons} python aggregate_forcing_nrt.py'
    cmd = f'{cmd5} -d afterok:{jid4} -t 02:00:00 -J aggreg --wrap="{cmd6} {t1:%Y%m} {t2:%Y%m}"  -o {logdir}/aggreg_{t1:%Y%m%d}_{t2:%Y%m%d}.txt'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid5 = ret.decode().split(' ')[-1].rstrip()
    print(f'Aggregate forcing job ID is: {jid5}')

    # rsync a copy to skyriver for Globus share
    rcmd = f'rsync -a {out_path}/conus/{prodtype}/{t1:%Y}/{t1:%Y%m}* {globus_path}/{prodtype}/{t1:%Y}/'
    if t1.year!=t2.year or t1.month!=t2.month:
        rcmd += f'; rsync -a {out_path}/conus/{prodtype}/{t2:%Y}/{t2:%Y%m}* {globus_path}/{prodtype}/{t2:%Y}/'
    cmd = f'sbatch -A cwp101 -p cw3e-shared --nodes=1 --ntasks-per-node=1 -d afterok:{jid1}:{jid2}:{jid3}:{jid4} -t 02:00:00 -J rsync --wrap="{rcmd}" -o {logdir}/rsync_{t1:%Y%m%d}_{t2:%Y%m%d}.txt'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid6 = ret.decode().split(' ')[-1].rstrip()
    print(f'Job ID to rsync NRT forcing to skyriver Globus share is: {jid6}')
    
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

