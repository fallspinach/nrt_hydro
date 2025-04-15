''' Run WRF-Hydro in Near Real Time (NRT)

Usage:
    python run_nrt.py [domain] [date_start] [date_end]
Default values:
    [domain]: "cnrfc"
    [date_start]: 11 days before current time
    [date_end]: date of most recent full-day NRT forcing
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, subprocess
from glob import glob
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time, replace_brackets

## separate the WRF-Hydro simulation from post-processing

def run_wrf_hydro(domain, t1, t2, xland='rfcs', dep=-1):

    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/run'
    ndays = (t2+timedelta(days=1)-t1).days
    nmons = round(ndays/30.5)
    if nmons==0:
        nmons = 1

    print(f'Running model from {t1:%Y-%m-%d} to {t2:%Y-%m-%d} ({ndays:d} days).') 

    tpn = config['cores_per_node']
    config_dom = config['wrf_hydro'][domain]
    minperday = config_dom['minperday']
    if domain=='conus':
        trun1 = datetime(1,1,1)+timedelta(minutes=ndays*minperday+50)
    else:
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

    if domain=='conus':
        frun = f'run_wrf_hydro_{xland}.sh'
        fnml = f'namelist.hrldas.{xland}'
        fhyd = f'hydro.namelist.{xland}'
    else:
        frun = 'run_wrf_hydro.sh'
    
    for ftpl in glob('../../../shared/nrt/*.tpl'):

        f = os.path.basename(ftpl).replace('.tpl', '')
        if domain=='conus':
            if f=='run_wrf_hydro.sh':
                f = frun
            elif f=='namelist.hrldas':
                f = fnml
            elif f=='hydro.namelist':
                f = fhyd
                
        os.system(f'/bin/cp {ftpl} {f}')
        replace_brackets(f, {'DOMAIN': domain,        'DOM': domain[:2],         'STARTYEAR': f'{t1:%Y}',     'STARTMONTH': f'{t1:%m}', 'STARTDAY': f'{t1:%d}',
                             'NDAYS':  f'{ndays:d}',  'RSTHOURS': f'{rst_hr:d}', 'RSTMINUTES': f'{rst_mn:d}', 'SBATCHTIME': trun,       'NNODES': f'{nnodes:d}',
                             'NPROCS': f'{nprocs:d}', 'PARTITION': partition,    'TPN': f'{tpn:d}',           'MODULES': modules})
        
        if (not config_dom['lake']) and f[:14]=='hydro.namelist':
            replace_brackets(f, {'outlake  = 1': 'outlake  = 0', 'route_lake_f': '!route_lake_f'}, False)

        if domain=='conus' and f==frun:
            print(f'Adding cmd to {frun} to set XLAND to xland={xland}.')
            target = f'{config["base_dir"]}/wrf_hydro/{domain}/domain/wrfinput_conus.nc'
            cmd1 = f'ln -nfs wrfinput_{xland}_trim.nc {target}'
            cmd2 = f'ln -nfs {fnml} namelist.hrldas'
            cmd3 = f'ln -nfs {fhyd} hydro.namelist'
            replace_brackets(f, {'module list': f'{cmd1}; {cmd2}; {cmd3}', '.out': f'_{xland}.out'}, False)

    # check restart files
    frestart = f'../restart/RESTART.{t1:%Y%m%d}00_DOMAIN1'
    if not os.path.isfile(frestart):
        print(f'{frestart} not found, quitting now.')
        return 1
    frestart = f'../restart/HYDRO_RST.{t1:%Y-%m-%d}_00:00_DOMAIN1'
    if not os.path.isfile(frestart):
        print(f'{frestart} not found, quitting now.')
        return 1
    
    # check forcing files
    t = t1
    while t<=t2:
        fforcing = f'../forcing/1km_hourly/{t1:%Y/%Y%m%d}.LDASIN_DOMAIN1'
        if not os.path.isfile(fforcing):
            print(f'{fforcing} not found, quitting now.')
            return 1
        t += timedelta(days=1)

    if dep==-1:
        ret = subprocess.check_output([f'sbatch {frun}'], shell=True)
    else:
        ret = subprocess.check_output([f'sbatch -d afterok:{dep} {frun}'], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip()
    print(f'WRF-Hydro job ID is: {jid}')

    return jid


## main function
def main(argv):

    '''main loop'''
    curr_time = datetime.utcnow()
    curr_time = curr_time.replace(tzinfo=pytz.utc)

    print(f'Current day is {curr_time:%Y-%m-%d}.')

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
            
    t1 = t1.replace(tzinfo=pytz.utc)
    t2 = t2.replace(tzinfo=pytz.utc)

    config_dom = config['wrf_hydro'][domain]
    
    ndays = (t2+timedelta(days=1)-t1).days
    nmons = round(ndays/30.5)
    if nmons==0:
        nmons = 1

    nldas2_path = f'{config["base_dir"]}/forcing/nldas2/NLDAS_FORA0125_H.2.0' # path to NLDAS-2 archive folder
    last_nldas2 = find_last_time(f'{nldas2_path}/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.020.nc')
    last_nldas2 -= timedelta(hours=last_nldas2.hour)

    if domain=='conus':
        if t1<last_nldas2:
            if t2>=last_nldas2:
                print(f'CONUS domain and NLDAS-2 ends (on {last_nldas2:%Y-%m-%d}) between t1={t1:%Y-%m-%d} and t2={t2:%Y-%m-%d}. Split the simulation into two separate ones.')
                jid = run_wrf_hydro(domain, t1,          last_nldas2-timedelta(days=1), xland='rfcs')
                jid = run_wrf_hydro(domain, last_nldas2, t2,                            xland='hrrr', dep=jid)
            else:
                print(f'CONUS domain and NLDAS-2 ends (on {last_nldas2:%Y-%m-%d}) later than t2={t2:%Y-%m-%d}. RFCS mask for entire period.')
                jid = run_wrf_hydro(domain, t1, t2, xland='rfcs')
        else:
            print(f'CONUS domain and NLDAS-2 ends (on {last_nldas2:%Y-%m-%d}) earlier than t1={t1:%Y-%m-%d}. HRRR mask for entire period.')
            jid = run_wrf_hydro(domain, t1, t2, xland='hrrr')
    else:
        jid = run_wrf_hydro(domain, t1, t2)
    
    os.chdir(f'{config["base_dir"]}/scripts/wrf_hydro')
    nc_shared   = nmons
    mem_shared  = 12*nc_shared if domain!='conus' else 25*nc_shared
    part_shared = config_dom["partition"].replace("compute", "shared")
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {nc_shared} python merge_fix_time_nrt.py'
    flog = f'{workdir}/log/mergefixtime_{t1:%Y%m}_{t2:%Y%m}.txt'
    cmd = f'sbatch -d afterok:{jid} -t 02:00:00 --nodes=1 -p {part_shared} --ntasks-per-node={nc_shared:d} --mem={mem_shared}G -A cwp101 -J mf{t1:%Y%m} --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m}" -o {flog}'
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid1 = jid
    print(f'Merging/percentile calculation job ID is: {jid}')

    cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/plot_nrt.py'
    flog = f'{workdir}/log/plot_{t1:%Y%m}_{t2:%Y%m}.txt'
    cmd = f'sbatch -d afterok:{jid1} -t 00:40:00 --nodes=1 -p {part_shared} --ntasks-per-node=1 -A cwp101  --mem=10G -J plotday --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m}" -o {flog}'
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid2 = jid
    print(f'Daily plotting job ID is: {jid}')
    cmd = f'sbatch -d afterok:{jid2} -t 00:40:00 --nodes=1 -p {part_shared} --ntasks-per-node=1 -A cwp101  --mem=10G -J plotmon --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m} monthly" -o {flog}.monthly'
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid3 = jid
    print(f'Monthly plotting job ID is: {jid}')
    
    if domain in ['cnrfc']:
        
        cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/extract_rivers_nrt.py'
        flog = f'{workdir}/log/extract_rivers_{t1:%Y%m}_{t2:%Y%m}.txt'
        cmd = f'sbatch -d afterok:{jid1} --nodes=1 --ntasks-per-node=1 -t 00:20:00 -p cw3e-shared -A cwp101 --mem=30G -J "exrivnrt" --wrap="{cmd1} {domain}" -o {flog}'
        #print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid = ret.decode().split(' ')[-1].rstrip()
        print(f'River extraction will run with job ID: {jid}')

    elif domain in ['basins24']:

        cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np 6 python {config["base_dir"]}/scripts/wrf_hydro/extract_average_nrt.py'
        flog = f'{workdir}/log/extract_averages_{t1:%Y%m}_{t2:%Y%m}.txt'
        cmd = f'sbatch -d afterok:{jid3} --nodes=1 --ntasks-per-node=6 -t 00:20:00 -p cw3e-shared -A cwp101 --mem=12G -J "exavgnrt" --wrap="{cmd1} {domain} 202405 {t2:%Y%m}" -o {flog}'
        #print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid3 = ret.decode().split(' ')[-1].rstrip()
        print(f'Basin averages extraction will run with job ID: {jid3}')

        cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/extract_points_nrt.py'
        flog = f'{workdir}/log/extract_points_{t1:%Y%m}_{t2:%Y%m}.txt'
        cmd = f'sbatch -d afterok:{jid1} --nodes=1 --ntasks-per-node=1 -t 00:20:00 -p cw3e-shared -A cwp101 --mem=2G -J "expntnrt" --wrap="{cmd1} {domain} 202405 {t2:%Y%m}" -o {flog}'
        #print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid3 = ret.decode().split(' ')[-1].rstrip()
        print(f'Snow sites extraction will run with job ID: {jid3}')

        cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/extract_b120_nrt.py'
        flog = f'{workdir}/log/extract_b120_{t1:%Y%m}_{t2:%Y%m}.txt'
        cmd = f'sbatch -d afterok:{jid1} --nodes=1 --ntasks-per-node=1 -t 00:20:00 -p cw3e-shared -A cwp101 --mem=2G -J "ex120nrt" --wrap="{cmd1} {domain} 202405 {t2:%Y%m}" -o {flog}'
        #print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid3 = ret.decode().split(' ')[-1].rstrip()
        print(f'B-120 simulated flow extraction will run with job ID: {jid3}')

    elif domain in ['conus']:

        for huclev in [8, 10]:
            cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/extract_huc_nrt.py'
            flog = f'{workdir}/log/extract_huc{huclev}_{t1:%Y%m}_{t2:%Y%m}.txt'
            cmd = f'sbatch -d afterok:{jid3} --nodes=1 --ntasks-per-node=1 -t 01:20:00 -p cw3e-shared -A cwp101 --mem=5G -J "exhucnrt" --wrap="{cmd1} {domain} 202410 {t2:%Y%m} {huclev}" -o {flog}'
            #print(cmd)
            ret = subprocess.check_output([cmd], shell=True)
            jid4 = ret.decode().split(' ')[-1].rstrip()
            print(f'HUC{huclev} basin averages extraction will run with job ID: {jid4}')
            
        cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/extract_gauges_nrt_conus.py'
        flog = f'{workdir}/log/extract_gauges_{t1:%Y%m}_{t2:%Y%m}.txt'
        cmd = f'sbatch -d afterok:{jid3} --nodes=1 --ntasks-per-node=1 -t 00:30:00 -p cw3e-shared -A cwp101 --mem=5G -J "extgage" --wrap="{cmd1} {domain} 202410 {t2:%Y%m}" -o {flog}'
        #print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid4 = ret.decode().split(' ')[-1].rstrip()
        print(f'USGS gage time series extraction will run with job ID: {jid4}')

        for subdomain in ['cbrfc']:
            
            # subset subdomain from conus
            cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/subset_output.py'
            flog = f'{workdir}/log/subset_{subdomain}_{t1:%Y%m}_{t2:%Y%m}.txt'
            cmd = f'sbatch -d afterok:{jid1} -t 00:40:00 --nodes=1 -p {part_shared} --ntasks-per-node=1 -A cwp101 --mem=5G -J subset{subdomain} --wrap="{cmd1} {subdomain} {domain} {t1:%Y%m} {t2:%Y%m} nrt" -o {flog}'
            ret = subprocess.check_output([cmd], shell=True)
            jid5 = ret.decode().split(' ')[-1].rstrip()
            print(f'Subsetting {subdomain} job ID is: {jid5}')

            # plot subdomain
            cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/plot_nrt.py'
            flog = f'{workdir}/log/plot_{subdomain}_{t1:%Y%m}_{t2:%Y%m}.txt'
            cmd = f'sbatch -d afterok:{jid5} -t 00:40:00 --nodes=1 -p {part_shared} --ntasks-per-node=1 -A cwp101  --mem=6G -J plotday --wrap="{cmd1} {subdomain} {t1:%Y%m} {t2:%Y%m}" -o {flog}'
            ret = subprocess.check_output([cmd], shell=True)
            jid6 = ret.decode().split(' ')[-1].rstrip()
            print(f'Daily plotting for subdomain {subdomain} job ID is: {jid6}')
            cmd = f'sbatch -d afterok:{jid5} -t 00:10:00 --nodes=1 -p {part_shared} --ntasks-per-node=1 -A cwp101  --mem=6G -J plotmon --wrap="{cmd1} {subdomain} {t1:%Y%m} {t2:%Y%m} monthly" -o {flog}.monthly'
            ret = subprocess.check_output([cmd], shell=True)
            jid7 = ret.decode().split(' ')[-1].rstrip()
            print(f'Monthly plotting for subdomain {subdomain} job ID is: {jid7}')

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
