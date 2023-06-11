import sys, os, math, pytz, time, yaml, subprocess
from glob import glob
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time
    
## some setups
workdir   = config['base_dir'] + '/scripts/forcing'
logdir    = config['base_dir'] + '/forcing/log'
stg4_path = config['base_dir'] + '/forcing/stage4'                      # path to Stage IV files
nld2_path = config['base_dir'] + '/forcing/nldas2/NLDAS_FORA0125_H.002' # path to NLDAS-2 archive folder
hrrr_path = config['base_dir'] + '/forcing/hrrr/analysis'               # path to HRRR analysis


## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)

    last_st4a = find_last_time(stg4_path+'/archive/202???/ST4.20??????', 'ST4.%Y%m%d') + timedelta(hours=23)
    last_st4r = find_last_time(stg4_path+'/realtime/pcpanl.????????/st4_conus.??????????.01h.nc', 'st4_conus.%Y%m%d%H.01h.nc')
    last_nld2 = find_last_time(nld2_path+'/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.002.nc')
    last_hrrr = find_last_time(hrrr_path+'/202?????/hrrr_anal_202???????.nc', 'hrrr_anal_%Y%m%d%H.nc')
    
    print('Last Stage-IV archive:  %s' % (last_st4a.isoformat()))
    print('Last Stage-IV realtime: %s' % (last_st4r.isoformat()))
    print('Last NLDAS-2 data:      %s' % (last_nld2.isoformat()))
    print('Last HRRR analysis:     %s' % (last_hrrr.isoformat()))
    
    #cmd0 = 'sbatch --export=NONE -A cwp101 -p shared -n 12 '
    #cmd1 = 'sbatch --export=NONE -A cwp101 -p compute -N 1 '
    cmd0 = 'sbatch -A cwp101 -p shared -n 12 '
    cmd1 = 'sbatch -A cwp101 -p compute -N 1 '
    cmd2 = 'unset SLURM_MEM_PER_NODE; mpirun -np 12 python create_conus_forcing.py'
    cmd3 = 'mpirun -np 12 python mergetime_subset.py'
    
    # NLDAS-2 + Stage-IV archive update
    t1 = last_st4a - timedelta(hours=47); t2 = last_st4a
    cmd = cmd0 + '-t 00:40:00 -J nld2st4a --wrap="%s %s %s" -o %s/nld2st4a_%s_%s.txt' % (cmd2, t1.strftime('%Y%m%d%H'),
            t2.strftime('%Y%m%d%H'), logdir, t1.strftime('%Y%m%d%H'), t2.strftime('%Y%m%d%H')); print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid1 = ret.decode().split(' ')[-1].rstrip()
    print('NLDAS-2 + StageIV archive forcing job ID is: '+jid1)

    # NLDAS-2 + Stage-IV realtime until end of NLDAS-2
    t1 = last_st4a + timedelta(hours=1); t2 = last_nld2
    cmd = cmd0 + '-t 00:40:00 -J nld2st4r --wrap="%s %s %s" -o %s/nld2st4r_%s_%s.txt' % (cmd2, t1.strftime('%Y%m%d%H'),
            t2.strftime('%Y%m%d%H'), logdir, t1.strftime('%Y%m%d%H'), t2.strftime('%Y%m%d%H')); print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid2 = ret.decode().split(' ')[-1].rstrip()
    print('NLDAS-2 + StageIV realtime forcing job ID is: '+jid2)

    # HRRR + Stage-IV until end of HRRR analysis
    t1 = last_nld2 + timedelta(hours=1); t2 = last_hrrr
    cmd = cmd0 + '-t 00:40:00 -J hrrrst4r --wrap="%s %s %s" -o %s/hrrrst4r_%s_%s.txt' % (cmd2, t1.strftime('%Y%m%d%H'),
            t2.strftime('%Y%m%d%H'), logdir, t1.strftime('%Y%m%d%H'), t2.strftime('%Y%m%d%H')); print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid3 = ret.decode().split(' ')[-1].rstrip()
    print('HRRR + StageIV realtime forcing job ID is: '+jid3)
    
    # merge hourly files to daily and subset/reproject
    t1 = last_st4a - timedelta(hours=47); t2 = last_hrrr
    cmd = cmd1 + '-d afterok:%s:%s:%s -t 02:20:00 -J mergesub --wrap="%s %s %s"  -o %s/mergesub_%s_%s.txt' % (jid1, jid2, jid3,
            cmd3, t1.strftime('%Y%m%d'), t2.strftime('%Y%m%d'), logdir, t1.strftime('%Y%m%d%H'), t2.strftime('%Y%m%d%H')); print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid4 = ret.decode().split(' ')[-1].rstrip()
    print('Mergetime and subset forcing job ID is: '+jid4)
    
    # plot/update images for web
    #t1 = last_st4a - timedelta(hours=47); t2 = last_hrrr
    #cmd = 'sbatch run_plot_conus_forcing.sh %s %s' % (t1.strftime('%Y%m%d%H'), t2.strftime('%Y%m%d%H'))

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

