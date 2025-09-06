''' Run WRF-Hydro short-range forecast driven by deterministic weather forecast

Usage:
    python run_fcst_det.py [domain] [weather_fcst] [date_start] [date_end] 
Default values:
    [domain]: "cnrfc"
    [weather_fcst]: wwrf, gfs, etc.
    [date_start]: most recent date of NRT model state
    [date_end]: last West-WRF full-day forecast (deterministic at the moment)
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, subprocess
from glob import glob
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time, replace_brackets

## some setups
modelid = 'nwm_v3'

## main function
def main(argv):

    '''main loop'''

    if len(argv)>=1:
        domain = argv[0]
    else:
        domain = 'cnrfc'

    wfcst = argv[1]
    
    workdir   = f'{config["base_dir"]}/{modelid}/{domain}/fcst/{wfcst}/run'

    curr_time = datetime.now(UTC)
    curr_time = curr_time.replace(tzinfo=pytz.utc)

    if len(argv)>=4:
        t1 = datetime.strptime(argv[2], '%Y%m%d')
        t2 = datetime.strptime(argv[3], '%Y%m%d')
        t1 = t1.replace(tzinfo=pytz.utc)
        t2 = t2.replace(tzinfo=pytz.utc)
    else:
        t1 = find_last_time(f'{config["base_dir"]}/{modelid}/{domain}/nrt/restart/RESTART.????????00_DOMAIN1', 'RESTART.%Y%m%d00_DOMAIN1')
        t2 = find_last_time(f'{config["base_dir"]}/{modelid}/{domain}/fcst/{wfcst}/forcing/1km_hourly/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') - timedelta(days=1)
        
    os.chdir(workdir)
    rlast = f'../../../nrt/restart/RESTART.{t1:%Y%m%d}00_DOMAIN1'
    llast = f'../restart/RESTART.{t1:%Y%m%d}00_DOMAIN1'
    if not os.path.islink(llast):
        cmd = f'ln -s {rlast} {llast}'
        os.system(cmd)
    rlast = f'../../../nrt/restart/HYDRO_RST.{t1:%Y-%m-%d}_00:00_DOMAIN1'
    llast = f'../restart/HYDRO_RST.{t1:%Y-%m-%d}_00:00_DOMAIN1'
    llast2 = f'../restart/HYDRO_RST.{t1:%Y-%m-%d}_00:00_DOMAIN1'
    if not os.path.islink(llast2):
        cmd = f'ln -s {rlast} {llast}'
        os.system(cmd)
    

    ndays  = (t2+timedelta(days=1)-t1).days
    ndays  = ndays if ndays>0 else 0
    
    trun  = (datetime(1,1,1)+timedelta(minutes=ndays*2+13)).strftime("%H:%M:%S")
    
    tpn = config['cores_per_node']
    config_dom = config[modelid][domain]
    minperday = config_dom['minperday']
    
    truntmp = datetime(1,1,1)+timedelta(minutes=ndays*minperday+30)
    trun = f'{truntmp.day-1}-{truntmp:%H:%M:%S}'
    
    nnodes    = config_dom['nnodes']
    nprocs    = config_dom['nprocs']
    partition = config_dom['partition']
    modules   = config['modules']
    
    # single shared node case
    if nprocs<tpn:
        tpn = nprocs

    print(f'Current day is {curr_time:%Y-%m-%d}, running {wfcst}-driven deterministic model from {t1:%Y-%m-%d} to {t2:%Y-%m-%d} ({ndays} days).')

    # don't save any restart files
    rst_hr = -99999
    rst_mn = -99999

    os.chdir(workdir)
    
    for freq in ['hourly', 'daily']:
        os.system(f'/bin/rm -f ../output/1km_{freq}/{t1:%Y}/*')
        os.system(f'/bin/rm -f ../output/1km_{freq}/{t2:%Y}/*')
        os.makedirs(f'../output/1km_{freq}/{t1:%Y}', exist_ok=True)
        os.makedirs(f'../output/1km_{freq}/{t2:%Y}', exist_ok=True)

    for ftpl in glob(f'../../../../shared/fcst/{wfcst}/*.tpl'):

        f = os.path.basename(ftpl).replace('.tpl', '')
        os.system('/bin/cp %s %s' % (ftpl, f))
        replace_brackets(f, {'DOMAIN': domain, 'DOM': domain[:2], 'STARTYEAR': f'{t1:%Y}', 'STARTMONTH': f'{t1:%m}', 'STARTDAY': f'{t1:%d}', 'ENDYEAR': f'{t2:%Y}', 
                            'NDAYS':  f'{ndays}', 'RSTHOURS': f'{rst_hr}', 'RSTMINUTES': f'{rst_mn}', 'SBATCHTIME': trun, 'NNODES': f'{nnodes}', 
                            'NPROCS': f'{nprocs}', 'PARTITION': partition, 'TPN': f'{tpn}', 'MODULES': modules})
        if (not config_dom['lake']) and f=='hydro.namelist':
            replace_brackets(f, {'outlake  = 1': 'outlake  = 0', 'route_lake_f': '!route_lake_f'}, False)

    cmd = f'sbatch -J wh_{modelid} run_{modelid}.sh'
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip()
    print(f'WRF-Hydro/{wfcst.upper()} will run with job ID: {jid}')

    # merge output files and aggregate to daily
    os.chdir(f'{config["base_dir"]}/scripts/{modelid}')
    nc_shared   = 1 if t1.month==t2.month else 2
    mem_shared  = 8*nc_shared if domain!='conus' else 25*nc_shared
    part_shared = config_dom["partition"].replace("compute", "shared")
    cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np {nc_shared} python merge_aggregate.py'
    flog = f'{workdir}/log/merge_aggre_{t1:%Y%m}_{t2:%Y%m}.txt'
    cmd = f'sbatch -d afterok:{jid} -t 02:00:00 --nodes=1 -p {part_shared} --ntasks-per-node={nc_shared:d} --mem={mem_shared}G -J mf{wfcst}{t1:%Y%m} --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m} fcst/{wfcst}" -o {flog}'
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid1 = jid
    print(f'Merging/percentile calculation job ID is: {jid}')

    # extract time series over basins
    cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/{modelid}/extract_basin.py'
    flog = f'{workdir}/log/extract_basin_{t1:%Y%m}_{t2:%Y%m}.txt'
    cmd = f'sbatch -d afterok:{jid1} --nodes=1 --ntasks-per-node=1 -t 00:20:00 -p {part_shared} --mem=13G -J "exbas{wfcst}" --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m} fcst/{wfcst}" -o {flog}'
    #print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip()
    print(f'Basin time series extraction will run with job ID: {jid}')

    cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/{modelid}/plot_forcing_output.py'
    flog = f'{workdir}/log/plot_{wfcst}_{t1:%Y%m}_{t2:%Y%m}.txt'
    cmd = f'sbatch -d afterok:{jid1} -t 00:40:00 --nodes=1 -p {part_shared} --ntasks-per-node=1 --mem=10G -J plot{wfcst} --wrap="{cmd1} {domain} {t1:%Y%m} {t2:%Y%m} fcst/{wfcst}" -o {flog}'
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip(); jid2 = jid
    print(f'Daily plotting job ID is: {jid}')
    
    if True:
        return

    cmd1 = f'unset SLURM_MEM_PER_NODE; python {config["base_dir"]}/scripts/wrf_hydro/extract_rivers_wwrf.py'
    flog = f'{workdir}/log/extract_rivers_{t1:%Y%m}_{t2:%Y%m}_{ens:02d}.txt'
    cmd = f'sbatch -d afterok:{jid} --nodes=1 --ntasks-per-node=1 -t 00:20:00 -p cw3e-shared -A cwp101 -J "exriwwrf" --wrap="{cmd1} {domain} {t1:%Y%m%d} {t2:%Y%m%d} {ens} {ens}" -o {flog}'
    print(cmd)
    ret = subprocess.check_output([cmd], shell=True)
    jid = ret.decode().split(' ')[-1].rstrip()
    print(f'River extraction will run for ensemble #{ens:02d} with job ID: {jid}')

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
