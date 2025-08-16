''' Run WRF-Hydro in retrospective time horizon for multiple years

Usage:
    python run_retro_yearly.py [domain] [yyyy1] [yyyy2]
Default values:
    must spcify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, subprocess
from glob import glob
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time, replace_brackets

## some setups

modelid = 'nwm_v3'

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    y1 = int(argv[1])
    y2 = int(argv[2])

    jid1 = argv[3] if len(argv)>3 else ''

    workdir = f'{config["base_dir"]}/{modelid}/{domain}/retro/run'
    tpn = config['cores_per_node']
    config_dom = config[modelid][domain]
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
        os.system(f'mkdir -p {workdir}/../output/1km_hourly/{t1:%Y}')
        os.system(f'mkdir -p {workdir}/../output/1km_monthly/{t1:%Y}')
        os.chdir(f'{workdir}/{t1:%Y}')
        os.system('ln -nfs ../../../../shared/tables/*.TBL .')

        rst_hr = -99999
        rst_mn = -99999

        for ftpl in glob('../../../../shared/retro/*.tpl'):

            f = os.path.basename(ftpl).replace('.tpl', '')
            os.system(f'/bin/cp {ftpl} {f}')
            replace_brackets(f, {'DOMAIN': domain,        'DOM': domain[:2],         'STARTYEAR': f'{t1:%Y}',     'STARTMONTH': f'{t1:%m}', 'STARTDAY': f'{t1:%d}',
                                 'NDAYS':  f'{ndays:d}',  'RSTHOURS': f'{rst_hr:d}', 'RSTMINUTES': f'{rst_mn:d}', 'SBATCHTIME': trun,       'NNODES': f'{nnodes:d}',
                                 'NPROCS': f'{nprocs:d}', 'PARTITION': partition,    'TPN': f'{tpn:d}',           'MODULES': modules})
            
            if (not config_dom['lake']) and f=='hydro.namelist':
                replace_brackets(f, {'outlake  = 1': 'outlake  = 0', 'route_lake_f': '!route_lake_f'}, False)

        # check restart files
        frestart = f'../../restart/RESTART.{t1:%Y%m%d}00_DOMAIN1'
        if not os.path.isfile(frestart):
            if y==y1 and jid1=='':
                print(f'{frestart} not found. Please make sure it is available before running year {y1} and later.')
                return 1
        frestart = f'../../restart/HYDRO_RST.{t1:%Y-%m-%d}_00:00_DOMAIN1'
        if not os.path.isfile(frestart):
            if y==y1 and jid1=='':
                print(f'{frestart} not found. Please make sure it is available before running year {y1} and later.')
                return 1
    
        # check forcing files
        t = t1
        while t<=t2+timedelta(days=1):
            fforcing = f'../../forcing/1km_hourly/{t1:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            if not os.path.isfile(fforcing):
                print(f'{fforcing} not found, quitting now.')
                return 1
            t += timedelta(days=1)

        dep = '' if jid1=='' else f'-d afterok:{jid1}'
        ret = subprocess.check_output([f'sbatch {dep} run_{modelid}.sh'], shell=True)
        jid1 = ret.decode().split(' ')[-1].rstrip()
        print(f'WRF-Hydro job ID for year {y} is: {jid1}')
        time.sleep(2)
    
        os.chdir(f'{config["base_dir"]}/scripts/{modelid}')
        nc_shared   = nmons
        mem_shared  = 12*nc_shared
        part_shared = config_dom["partition"].replace("compute", "shared")
        cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {nc_shared} python merge_fix_time_retro.py'
        cmd = f'sbatch -d afterok:{jid1} -t 04:00:00 --nodes=1 -p {part_shared} --ntasks-per-node={nc_shared:d} --mem={mem_shared}G -A cwp101 -J mf{t1:%Y%m} --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m}" -o {workdir}/log/mergefixtime_{t1:%Y%m}_{t2:%Y%m}.txt'
        ret = subprocess.check_output([cmd], shell=True)
        jid2 = ret.decode().split(' ')[-1].rstrip()
        print(f'Merging/percentile calculation job ID for year {y} is: {jid2}')
        time.sleep(2)
    
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
