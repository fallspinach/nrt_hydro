import sys, os, pytz, time, subprocess
from glob import glob
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups

## main function
def main(argv):

    '''main loop'''
    curr_time = datetime.utcnow()
    curr_time = curr_time.replace(tzinfo=pytz.utc)

    if len(argv)>=1:
        domain = argv[0]
    else:
        domain = 'cnrfc'
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/run'
    
    if len(argv)>=3:
        t1 = datetime.strptime(argv[1], '%Y%m%d')
        t2 = datetime.strptime(argv[2], '%Y%m%d')
    else:
        curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
        t1 = curr_day - timedelta(days=11)
        t2 = find_last_time(f'{workdir}/../forcing/1km_hourly/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') - timedelta(days=1)
        if t2 > curr_day - timedelta(days=1):
            t2 = curr_day - timedelta(days=1)
    
    ndays = (t2+timedelta(days=1)-t1).days
    nmons = round(ndays/30.5)
    if nmons==0:
        nmons = 1

    print(f'Current day is {curr_time:%Y-%m-%d}, running model from {t1:%Y-%m-%d} to {t2:%Y-%m-%d} ({ndays:d} days).') 

    tpn = config['cores_per_node']
    config_dom = config['wrf_hydro'][domain]
    minperday = config_dom['minperday']
    trun1 = datetime(1,1,1)+timedelta(minutes=ndays*minperday+30)
    trun = f'{trun1.day-1}-{trun1:%H:%M:%S}'
    nnodes    = config_dom['nnodes']
    nprocs    = config_dom['nprocs']
    partition = config_dom['partition']
    modules   = config['modules']

    # single shared node case
    if nprocs<tpn:
        tpn = nprocs

    os.chdir(workdir)
    #os.system('ln -s ../../../shared/tables/*.TBL .')

    if ndays>25:
        rst_hr = -99999
        rst_mn = -99999
    else:
        rst_hr = 24
        rst_mn = 1440

    for ftpl in glob('../../../shared/nrt/*.tpl'):

        #f = ftpl.strip('.tpl')
        f = os.path.basename(ftpl).replace('.tpl', '')
        #print(ftpl, f)
        os.system(f'/bin/cp {ftpl} {f}')
        os.system(f'sed -i "s/<DOMAIN>/{domain}/g; s/<DOM>/{domain[:2]}/g; s/<STARTYEAR>/{t1.year:d}/g; s/<STARTMONTH>/{t1.month:02d}/g; s/<STARTDAY>/{t1.day:02d}/g; s/<NDAYS>/{ndays:d}/g; s/<SBATCHTIME>/{trun}/g; s/<RSTHOURS>/{rst_hr:d}/g; s/<RSTMINUTES>/{rst_mn:d}/g; s/<NNODES>/{nnodes:d}/g; s/<NPROCS>/{nprocs:d}/g; s/<PARTITION>/{partition}/g; s/<TPN>/{tpn:d}/g; s#<MODULES>#{modules}#g" {f}')

    ret = subprocess.check_output(['sbatch run_wrf_hydro.sh'], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip()
    print(f'WRF-Hydro job ID is: {jid}')
    
    os.chdir(f'{config["base_dir"]}/scripts/wrf_hydro')
    nc_shared   = nmons
    mem_shared  = 12*nc_shared
    part_shared = config_dom["partition"].replace("compute", "shared")
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {nc_shared} python merge_fix_time_nrt.py'
    cmd = f'sbatch -d afterok:{jid} -t 02:00:00 --nodes=1 -p {part_shared} --ntasks-per-node={nc_shared:d} --mem={mem_shared}G -A cwp101 -J mf{t1:%Y%m} --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m}" -o {workdir}/log/mergefixtime_{t1:%Y%m}_{t2:%Y%m}.txt'
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip()
    print('Merging/percentile calculation job ID is: '+jid)
    
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        