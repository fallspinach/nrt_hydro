import sys, os, pytz, time, subprocess
from glob import glob
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    y1 = int(argv[1])
    y2 = int(argv[2])

    jid1 = argv[3] if len(argv)>3 else ''

    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/run'
    tpn = config['cores_per_node']
    config_dom = config['wrf_hydro'][domain]
    minperday = config_dom['minperday']
    nnodes    = config_dom['nnodes']
    nprocs    = config_dom['nprocs']
    partition = config_dom['partition']
    modules   = config['modules']
    
    # single shared node case
    if nprocs<tpn:
        tpn = nprocs

    for y in range(y1, y2+1):

        t1 = datetime(y,  1,  1)
        t2 = datetime(y, 12, 31)

        ndays = (t2+timedelta(days=1)-t1).days
        nmons = round(ndays/30.5)
        trun1 = datetime(1,1,1)+timedelta(minutes=ndays*minperday+30)
        trun = f'{trun1.day-1}-{trun1:%H:%M:%S}'

        os.system(f'mkdir -p {workdir}/{t1:%Y}')
        os.system(f'mkdir -p {workdir}/../output/1km_daily/{t1:%Y}')
        os.chdir(f'{workdir}/{t1:%Y}')
        os.system('ln -s ../../../../shared/tables/*.TBL .')

        rst_hr = -99999
        rst_mn = -99999

        for ftpl in glob('../../../../shared/retro/*.tpl'):

             #f = ftpl.strip('.tpl')
            f = os.path.basename(ftpl).replace('.tpl', '')
            #print(ftpl, f)
            os.system(f'/bin/cp {ftpl} {f}')
            os.system(f'sed -i "s/<DOMAIN>/{domain}/g; s/<DOM>/{domain[:2]}/g; s/<STARTYEAR>/{t1.year:d}/g; s/<STARTMONTH>/{t1.month:02d}/g; s/<STARTDAY>/{t1.day:02d}/g; s/<NDAYS>/{ndays:d}/g; s/<SBATCHTIME>/{trun}/g; s/<RSTHOURS>/{rst_hr:d}/g; s/<RSTMINUTES>/{rst_mn:d}/g; s/<NNODES>/{nnodes:d}/g; s/<NPROCS>/{nprocs:d}/g; s/<PARTITION>/{partition}/g; s/<TPN>/{tpn:d}/g; s#<MODULES>#{modules}#g" {f}')

        dep = '' if jid1=='' else f'-d afterok:{jid1}'
        ret = subprocess.check_output([f'sbatch {dep} run_wrf_hydro.sh'], shell=True)
        jid1 = ret.decode().split(' ')[-1].rstrip()
        print(f'WRF-Hydro job ID for year {y} is: {jid1}')
        time.sleep(2)
    
        os.chdir(f'{config["base_dir"]}/scripts/wrf_hydro')
        nc_shared   = nmons
        mem_shared  = 12*nc_shared
        part_shared = config_dom["partition"].replace("compute", "shared")
        cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {nc_shared} python merge_fix_time_retro.py'
        cmd = f'sbatch -d afterok:{jid1} -t 02:00:00 --nodes=1 -p {part_shared} --ntasks-per-node={nc_shared:d} --mem={mem_shared}G -A cwp101 -J mf{t1:%Y%m} --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m}" -o {workdir}/log/mergefixtime_{t1:%Y%m}_{t2:%Y%m}.txt'
        ret = subprocess.check_output([cmd], shell=True)
        jid2 = ret.decode().split(' ')[-1].rstrip()
        print(f'Merging/percentile calculation job ID for year {y} is: {jid2}')
        time.sleep(2)
    
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
