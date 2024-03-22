import sys, os, pytz, time, subprocess
from glob import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups

## main function
def main(argv):

    '''main loop'''

    if len(argv)>=1:
        domain = argv[0]
    else:
        domain = 'cnrfc'
    
    workdir   = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/wwrf/run'

    curr_time = datetime.utcnow()
    curr_time = curr_time.replace(tzinfo=pytz.utc)

    if len(argv)>=5:
        t1 = datetime.strptime(argv[1], '%Y%m%d')
        t2 = datetime.strptime(argv[2], '%Y%m%d')
        t1 = t1.replace(tzinfo=pytz.utc)
        t2 = t2.replace(tzinfo=pytz.utc)
        ens1 = int(argv[3])
        ens2 = int(argv[4])
    else:
        t1 = find_last_time(f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/restart/RESTART.????????00_DOMAIN1', 'RESTART.%Y%m%d00_DOMAIN1')
        t2 = find_last_time(f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/wwrf/forcing/01/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') - timedelta(days=1)
        ens1 = 41
        ens2 = 41
        
    t22 = find_last_time(f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/wwrf/forcing/41/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') - timedelta(days=1)

    os.chdir(workdir)
    rlast = f'../../../nrt/restart/RESTART.{t1:%Y%m%d}00_DOMAIN1'
    llast = f'../restart/RESTART.{t1:%Y%m%d}00_DOMAIN1'
    if not os.path.islink(llast):
        cmd = f'ln -s {rlast} {llast}'
        os.system(cmd)
    rlast = f'../../../nrt/restart/HYDRO_RST.{t1:%Y-%m-%d}_00\:00_DOMAIN1'
    llast = f'../restart/HYDRO_RST.{t1:%Y-%m-%d}_00\:00_DOMAIN1'
    llast2 = f'../restart/HYDRO_RST.{t1:%Y-%m-%d}_00:00_DOMAIN1'
    if not os.path.islink(llast2):
        cmd = f'ln -s {rlast} {llast}'
        os.system(cmd)
    

    ndays  = (t2+timedelta(days=1)-t1).days
    ndays  = ndays if ndays>0 else 0
    ndays2 = (t22+timedelta(days=1)-t1).days
    
    trun  = (datetime(1,1,1)+timedelta(minutes=ndays*2+13)).strftime("%H:%M:%S")
    trun2 = (datetime(1,1,1)+timedelta(minutes=ndays2*2+13)).strftime("%H:%M:%S")
    
    tpn = config['cores_per_node']
    config_dom = config['wrf_hydro'][domain]
    minperday = config_dom['minperday']
    
    truntmp = datetime(1,1,1)+timedelta(minutes=ndays*minperday+30)
    trun = f'{truntmp.day-1}-{truntmp:%H:%M:%S}'
    truntmp = datetime(1,1,1)+timedelta(minutes=ndays2*minperday+30)
    trun2 = f'{truntmp.day-1}-{truntmp:%H:%M:%S}'
    
    nnodes    = config_dom['nnodes']
    nprocs    = config_dom['nprocs']
    partition = config_dom['partition']
    modules   = config['modules']
    
    # single shared node case
    if nprocs<tpn:
        tpn = nprocs

    print(f'Current day is {curr_time:%Y-%m-%d}, running ensemble model from {t1:%Y-%m-%d} to {t2:%Y-%m-%d} ({ndays} days) and deterministic model to {t22:%Y-%m-%d} ({ndays2} days).')

    if ndays>31:
        rst_hr = -99999
        rst_mn = -99999
    else:
        rst_hr = 24
        rst_mn = 1440

    #if True:
    #    return 1
 
    for ens in range(ens1, ens2+1):

        os.chdir(f'{workdir}/{ens:02d}')

        if ens == 41:
            t2 = t22
            ndays = ndays2
            trun  = trun2

        for ftpl in glob('../../../../../shared/fcst/wwrf/*.tpl'):

            f = os.path.basename(ftpl).replace('.tpl', '')
            os.system('/bin/cp %s %s' % (ftpl, f))
            os.system(f'sed -i "s/<DOMAIN>/{domain}/g; s/<DOM>/{domain[:2]}/g; s/<STARTYEAR>/{t1.year:d}/g; s/<STARTMONTH>/{t1.month:02d}/g; s/<STARTDAY>/{t1.day:02d}/g; s/<ENDYEAR>/{t2.year:d}/g; s/<NDAYS>/{ndays:d}/g; s/<SBATCHTIME>/{trun}/; s/<RSTHOURS>/{rst_hr:d}/; s/<RSTMINUTES>/{rst_mn:d}/; s/<ENS>/{ens:02d}/g; s/<NNODES>/{nnodes:d}/g; s/<NPROCS>/{nprocs:d}/g; s/<PARTITION>/{partition}/g; s/<TPN>/{tpn:d}/g; s#<MODULES>#{modules}#g" {f}')

        cmd = f'sbatch -J ww{ens:02d} run_wrf_hydro.sh'
        ret = subprocess.check_output([cmd], shell=True)
        jid = ret.decode().split(' ')[-1].rstrip()
        print(f'Ensemble #{ens:02d} will run with job ID: {jid}')
        
        cmd1 = f'unset SLURM_MEM_PER_NODE; mpirun -np 1 python {config["base_dir"]}/scripts/wrf_hydro/merge_fix_time_wwrf.py'
        flog = f'{workdir}/log/mergefixtime_{t1:%Y%m%d}_{t2:%Y%m%d}_{ens:02d}.txt'
        cmd = f'sbatch -d afterok:{jid} --nodes=1 --ntasks-per-node=1 --mem=20G -t 01:00:00 -p cw3e-shared -A cwp101 -J pp{ens:02d} --wrap="{cmd1} {domain} {t1:%Y%m%d} {t2:%Y%m%d} {ens} {ens} wwrf" -o {flog}'
        print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid = ret.decode().split(' ')[-1].rstrip()
        print(f'Mergetime will run for ensemble #{ens:02d} with job ID: {jid}')

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

        
