''' Run West-WRF + ESP ensemble forecast

    (1) to set up forcing links:
        python run_esp_wwrf.py [domain] [fcst_start] [fcst_end] [fcst_update]
        
    (2) to run WRF-Hydro ensemble simulations after the forcing links have been set up:
        python run_esp_wwrf.py [domain] [fcst_start] [fcst_end] [fcst_update] [ens1] [ens2]
        
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, subprocess
import netCDF4 as nc
from glob import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time, replace_brackets

## setup links
def setup_links(domain, t1, t2, tupdate):

    '''setup links for ensemble forecast'''
    
    yclim1 = config['wrf_hydro'][domain]['climrange'][0]
    yclim2 = config['wrf_hydro'][domain]['climrange'][1]
    nens = yclim2 - yclim1 + 1
    
    # create forcing links
    forcedir = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/esp_wwrf/forcing/{t1.year:d}-{t2.year:d}'
    if not os.path.isdir(forcedir):

        print('Creating forcing links ...')
        os.system(f'mkdir {forcedir}')
        os.chdir(forcedir)

        ens = 1
        for yclim in range(yclim1, yclim2+1):
            if yclim!=t1.year:

                os.system(f'mkdir -p {ens:02d}/{t1.year:d}')

                if t1.year==t2.year:
                    tclim1 = datetime(yclim, 1, 1)
                    tclim2 = datetime(yclim, 12, 31)
                else:
                    tclim1 = datetime(yclim, 8, 1)
                    tclim2 = datetime(yclim+1, 6, 1)
                    os.system(f'mkdir -p {ens:02d}/{t2.year:d}')

                tclim = tclim1
                while tclim<=tclim2:

                    tforc = tclim + relativedelta(years=t1.year-yclim)
                    fforc = f'{ens:02d}/{tforc:%Y/%Y%m%d}.LDASIN_DOMAIN1'

                    fclim = f'../../../retro/{tclim:%Y/%Y%m%d}.LDASIN_DOMAIN1'

                    # handle extra 2/29 - skip it
                    if tclim.year%4==0 and tforc.year%4!=0 and tclim.month==2 and tclim.day==29:
                        pass
                    else:
                        cmd = f'ln -s {fclim} {fforc}'
                        #print(cmd)
                        os.system(cmd)

                    # handle missing 2/29 - set it to 2/28
                    if tclim.year%4!=0 and tforc.year%4==0 and tclim.month==2 and tclim.day==28:
                        fforc = f'{ens:02d}/{tforc:%Y/%Y%m29}.LDASIN_DOMAIN1'
                        cmd = f'ln -s {fclim} {fforc}'
                        os.system(cmd)

                    tclim += timedelta(days=1)

                ens += 1
                
    # link WestWRF ensemble forecasts
    print('Creating links to West-WRF ensemble forecasts.')
    os.chdir(forcedir)
    if t1.month<1 or t1.month>12:
        print('  Oct 1 to Mar 31 season, using WWRF ensemble forecast')
        tlast = find_last_time(f'../NRT_ens/[012]?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') - timedelta(days=1)
        max_lead = 7
    else:
        print('  Apr 1 - Sep 30 season, using WWRF deterministic forecast')
        tlast = find_last_time(f'../NRT_ens/{nens}/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') - timedelta(days=1)
        # don't go beyond 7-day forecasts (for makeup runs that are executed later)
        max_lead = 5 # avoid overkilling the ensemble spread by the 10-day deterministic forecast
            
    if tlast > tupdate+timedelta(days=max_lead-1):
        tlast = tupdate+timedelta(days=max_lead-1)
    print(f'Last ensemble forecast date is: {tlast:%Y-%m-%d}; forecast update date is {tupdate:%Y-%m-%d}')
    for ens in range(1, nens+1):
        tforc = tupdate
        #while tforc<t1+timedelta(days=7):
        while tforc<=tlast:
            if t1.month<1 or t1.month>12:
                fww   = f'../NRT_ens/{ens:02d}/{tforc:%Y%m%d}.LDASIN_DOMAIN1'
            else:
                fww   = f'../NRT_ens/{nens}/{tforc:%Y%m%d}.LDASIN_DOMAIN1'
            fforc = f'{ens:02d}/{tforc:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            #print(f'{forcedir}/{fww}')
            if os.path.isfile(fww):
                ftemp = nc.Dataset(fww, 'r')
                if ftemp['time'].size==24:
                    os.system(f'rm -f {fforc}')
                    os.system(f'ln -s ../../{fww} {fforc}')
                    if ens==1:
                        print(f'{tforc:%Y-%m-%d} is found in West-WRF ensemble #1.')
                else:
                    print(f'{tforc:%Y-%m-%d} is found in West-WRF ensemble #{ens:02d} but has fewer than 24 time steps.')
                ftemp.close()
            else:
                if ens==1:
                    print(f'{tforc:%Y-%m-%d} is not found in West-WRF ensemble #1.')
            tforc += timedelta(days=1)
                
    # link NRT monitor forcing
    print('Creating links to NRT forcing.')
    tlast = find_last_time('../nrt/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') - timedelta(days=1)
    if tlast > tupdate:
        tlast = tupdate
    print(f'Use NRT monitor forcing date until: {tlast:%Y-%m-%d}')
    for ens in range(1, nens+1):
        tforc = t1
        while tforc<=tlast:
            fww   = f'../nrt/{tforc:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            fforc = f'{ens:02d}/{tforc:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            #print(forcedir+'/'+fww)
            if os.path.isfile(fww):
                os.system(f'rm -f {fforc}')
                os.system(f'ln -s ../../{fww} {fforc}')
            tforc += timedelta(days=1)
        
    return 0
 
def run_ensemble(domain, t1, t2, tupdate, ens1, ens2):

    '''run WRF-Hydro ensemble simulations'''
        
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/esp_wwrf/run'
    ndays = (t2+timedelta(days=1)-t1).days

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

    if ndays>31:
        rst_hr = -99999
        rst_mn = -99999
    else:
        rst_hr = 24
        rst_mn = 1440
        
    for ens in range(ens1, ens2+1):

        os.chdir(f'{workdir}/{ens:02d}')

        # create output dir
        os.system(f'mkdir -p ../../output/init{t1:%Y%m%d}_update{tupdate:%Y%m%d}/{ens:02d}') 

        for ftpl in glob('../../../../../shared/fcst/esp_wwrf/*.tpl'):

            f = os.path.basename(ftpl).replace('.tpl', '')
            os.system(f'/bin/cp {ftpl} {f}')
            replace_brackets(f, {'DOMAIN': domain, 'DOM': domain[:2], 'STARTYEAR': f'{t1:%Y}', 'STARTMONTH': f'{t1:%m}', 'STARTDAY': f'{t1:%d}', 'ENDYEAR': f'{t2:%Y}', 
                            'NDAYS':  f'{ndays}', 'RSTHOURS': f'{rst_hr}', 'RSTMINUTES': f'{rst_mn}', 'ENS': f'{ens:02d}', 'SBATCHTIME': trun, 'NNODES': f'{nnodes}', 
                            'NPROCS': f'{nprocs}', 'UPDATEYMD': f'{tupdate:%Y%m%d}', 'PARTITION': partition, 'TPN': f'{tpn}', 'MODULES': modules})
            if (not config_dom['lake']) and f=='hydro.namelist':
                replace_brackets(f, {'outlake  = 1': 'outlake  = 0', 'route_lake_f': '!route_lake_f'}, False)

        cmd = f'sbatch -J espww{ens:02d} run_wrf_hydro.sh'
        ret = subprocess.check_output([cmd], shell=True)
        jid = ret.decode().split(' ')[-1].rstrip()
        print(f'Ensemble #{ens:02d} will run with job ID: {jid}')

        cmd1 = f'python {config["base_dir"]}/scripts/wrf_hydro/merge_fix_time_ens.py  {domain} {t1:%Y%m%d} {t2:%Y%m%d} {tupdate:%Y%m%d} {ens:d} {ens:d} esp_wwrf'
        cmd2 = f'python {config["base_dir"]}/scripts/wrf_hydro/extract_average_ens.py {domain} {t1:%Y%m%d} {t2:%Y%m%d} {tupdate:%Y%m%d} {ens:d} {ens:d} esp_wwrf'
        if domain=='basins24':
            cmd12 = f'{cmd1}; {cmd2}'
        else:
            cmd12 = cmd1
        flog = f'{workdir}/log/log_mergefixtime_{t1:%Y%m%d}-{t2:%Y%m%d}_{ens:02d}.txt'
        cmd = f'sbatch -d afterok:{jid} --nodes=1 --ntasks-per-node=2 --mem=30G -t 01:30:00 -p cw3e-shared -A cwp101 -J pp{ens:02d} --wrap="{cmd12}" -o {flog}'
        #cmd = f'sbatch --nodes=1 --ntasks-per-node=2 --mem=30G -t 01:30:00 -p cw3e-shared -A cwp101 -J pp{ens:02d} --wrap="{cmd2}" -o {flog}'
        print(cmd)
        ret = subprocess.check_output([cmd], shell=True)
        jid = ret.decode().split(' ')[-1].rstrip()
        print(f'Mergetime will run for ensemble #{ens:02d} with job ID: {jid}')

    return 0


if __name__ == '__main__':
    
    argv = sys.argv[1:]
    
    domain = argv[0]
    t1 = datetime.strptime(argv[1], '%Y%m%d')
    t2 = datetime.strptime(argv[2], '%Y%m%d')
    t1 = t1.replace(tzinfo=pytz.utc)
    t2 = t2.replace(tzinfo=pytz.utc)
    tupdate = datetime.strptime(argv[3], '%Y%m%d')
    tupdate = tupdate.replace(tzinfo=pytz.utc)

    if len(argv)>4:
        ens1 = int(argv[4])
        ens2 = int(argv[5])
        run_ensemble(domain, t1, t2, tupdate, ens1, ens2)
    else:
        setup_links(domain, t1, t2, tupdate)        
