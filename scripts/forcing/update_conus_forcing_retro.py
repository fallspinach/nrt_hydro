import sys, os, math, pytz, time, yaml, subprocess
from glob import glob
import netCDF4 as nc
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

import process_prism
    
## some setups
workdir   = config['base_dir'] + '/scripts/forcing'
logdir    = config['base_dir'] + '/forcing/log'
prsm_path = config['base_dir'] + '/forcing/prism/recent/nc'             # path to PRISM files
nwm_path  = config['base_dir'] + '/forcing/nwm'
stnl_path = config['base_dir'] + '/forcing/stage4/filled_with_nldas2'   # path to Stage-IV NlDAS-2 merged precip data

prodtype = 'retro'

## main function
def main(argv):
    
    '''main loop'''
    
    # update PRISM data first
    #process_prism.main('')
    
    os.chdir(workdir)

    # find the last nc file
    ncfiles = glob(prsm_path+'/PRISM_tmean_stable_4kmD2_*.nc')
    ncfiles.sort()
    f = nc.Dataset(ncfiles[-1], 'r')
    last_prsm = datetime.strptime(str(nc.num2date(f['time'][-1], f['time'].units)), '%Y-%m-%d %H:%M:%S')
    last_prsm = last_prsm.replace(tzinfo=pytz.utc)
    f.close()
    first_prsm = datetime(1981, 1, 1, tzinfo=pytz.utc)

    # find last retro forcing in NWM format
    last_prod = find_last_time(nwm_path+'/1km/conus/????/????????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1')
    last_stnl = find_last_time(stnl_path+'/????/st4nl2_????????.nc', 'st4nl2_%Y%m%d.nc') - timedelta(days=1)
    
    print('Last PRISM "recent history" data:  %s' % (last_prsm.isoformat()))
    print('Last StageIV/NLDAS2 merged precip: %s' % (last_stnl.isoformat()))
    print('Last retro forcing file:           %s' % (last_prod.isoformat()))

    if len(argv)==2:
        t1 = datetime.strptime(argv[0], '%Y%m%d')
        t2 = datetime.strptime(argv[1], '%Y%m%d')
        t1 = t1.replace(tzinfo=pytz.utc)
        t2 = t2.replace(tzinfo=pytz.utc)
    else:
        t1 = last_prod + timedelta(days=1)
        t2 = last_prsm

    if t1<first_prsm:
        t1 = first_prsm
    if t2>last_prsm:
        t2 = last_prsm
    
    print('Processing %s data from %s to %s.' % (prodtype, t1.isoformat(), t2.isoformat()))

    if last_stnl<last_prsm:
        
        print('PRISM (%s) is newer than StageIV-NLDAS2 merged (%s) - bring the latter up to date first.' % (last_prsm.isoformat(), last_stnl.isoformat()))
        
        tt1 = last_stnl + timedelta(days=1)
        tt2 = last_prsm + timedelta(days=1)
        cmd00 = 'sbatch -A cwp101 -p shared -n 12'
        cmd11 = 'sbatch -A cwp101 -p shared -n 2'
        cmd22 = 'unset SLURM_MEM_PER_NODE; mpirun -np 12 python fill_stage4_with_nldas2.py'
        cmd33 = 'unset SLURM_MEM_PER_NODE; mpirun -np  2 python calc_shifted_daily.py'
        
        # Merge
        cmd = '%s -t 00:30:00 -J st4nl2 --wrap="%s %s %s %s" -o %s/st4nl2_%s_%s.txt' % (cmd00, cmd22, t1.strftime('%Y%m%d'),
                t2.strftime('%Y%m%d'), prodtype, logdir, t1.strftime('%Y%m%d'), t2.strftime('%Y%m%d')); print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid1 = ret.decode().split(' ')[-1].rstrip()
        print('StageIV & NLDAS-2 precip merging job ID is: '+jid1)

        # Shifted daily average
        cmd = '%s -d afterok:%s -t 00:10:00 -J shiftdai --wrap="%s %s %s %s" -o %s/shifted_daily_%s_%s.txt' % (cmd11, jid1, cmd33, t1.strftime('%Y'),
                t2.strftime('%Y'), prodtype, logdir, t1.strftime('%Y'), t2.strftime('%Y')); print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid2 = ret.decode().split(' ')[-1].rstrip()
        print('Shifted daily averaging job ID is: '+jid2)

        dep = '-d afterok:%s' % jid2

    else:
        dep = ''
        
    cmd0 = 'sbatch -A cwp101 -p shared -n 12 '
    cmd1 = 'sbatch -A cwp101 -p compute -N 1 '
    cmd2 = 'unset SLURM_MEM_PER_NODE; mpirun -np 12 python create_conus_forcing.py'
    cmd3 = 'mpirun -np 12 python mergetime_subset.py'
    
    # NLDAS-2 + Stage-IV archive update
    cmd = '%s %s -t 00:40:00 -J retrof --wrap="%s %s %s %s" -o %s/retrof_%s_%s.txt' % (cmd0, dep, cmd2, t1.strftime('%Y%m%d%H'),
            t2.strftime('%Y%m%d%H'), prodtype, logdir, t1.strftime('%Y%m%d%H'), t2.strftime('%Y%m%d%H')); print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid3 = ret.decode().split(' ')[-1].rstrip()
    print('Retro forcing job ID is: '+jid3)
    
    # merge hourly files to daily and subset/reproject
    cmd = '%s -d afterok:%s -t 02:20:00 -J mergesub --wrap="%s %s %s %s"  -o %s/mergesub_retro_%s_%s.txt' % (cmd1, jid3, cmd3,
            t1.strftime('%Y%m%d'), t2.strftime('%Y%m%d'), prodtype, logdir, t1.strftime('%Y%m%d%H'), t2.strftime('%Y%m%d%H')); print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid4 = ret.decode().split(' ')[-1].rstrip()
    print('Mergetime and subset retro forcing job ID is: '+jid4)
    
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

