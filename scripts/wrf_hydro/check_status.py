''' Check the status of the entire system

Usage:
    python check_status.py [update_gcloud]
Default values:
    must specify all (empty for not updating gcloud)
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, subprocess, csv
from glob import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time, find_last_time2

## some setups
domain = 'cnrfc'
domain1 = 'basins24'
domain2 = 'conus'
stage4_path = f'{config["base_dir"]}/forcing/stage4'  # path to Stage IV files
nldas2_path = f'{config["base_dir"]}/forcing/nldas2/NLDAS_FORA0125_H.2.0' # path to NLDAS-2 archive folder
hrrran_path = f'{config["base_dir"]}/forcing/hrrr/analysis' # path to HRRR analysis
fnwmrt_path = f'{config["base_dir"]}/forcing/nwm/1km/{domain}/nrt'
fnwmpr_path = f'{config["base_dir"]}/forcing/nwm/1km/{domain}/retro'
fnwmbc_path = f'{config["base_dir"]}/forcing/nwm/1km/{domain}/retro'
fwwdet_path = f'{config["base_dir"]}/forcing/wwrf/NRT/2022-2023/NRT_ecwmf/{domain}'
fwwens_path = f'{config["base_dir"]}/forcing/wwrf/NRT/2022-2023/NRT_ens/{domain}/01'

whmoni_path = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/1km_daily'
whrean_path = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/1km_daily'
whwwrf_path = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/wwrf/output/41'
whespw_path = f'{config["base_dir"]}/wrf_hydro/{domain1}/fcst/esp_wwrf/output'
scamod_path = f'{config["base_dir"]}/obs/modis/nc'

#fnstatus = f'{config["base_dir"]}/wrf_hydro/{domain}/web/imgs/monitor/system_status.csv'
fnstatus = f'{config["base_dir"]}/web/data/system_status.csv'

## main function
def main(argv):

    '''main loop'''

    curr_time = datetime.utcnow()
    curr_time = curr_time.replace(tzinfo=pytz.utc)
    curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
    curr_month= curr_day - timedelta(days=curr_day.day-1)

    wy_start = curr_month - relativedelta(months=6)
    
    last_stage4a = find_last_time(f'{stage4_path}/archive/202?/ST4.20??????', 'ST4.%Y%m%d') + timedelta(hours=23)
    last_stage4r = find_last_time(f'{stage4_path}/realtime/pcpanl.????????/st4_conus.??????????.01h.nc', 'st4_conus.%Y%m%d%H.01h.nc')
    last_nldas2  = find_last_time(f'{nldas2_path}/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.020.nc')
    last_hrrran  = find_last_time(f'{hrrran_path}/202?????/hrrr_anal_202???????.nc', 'hrrr_anal_%Y%m%d%H.nc')
    last_fnwmrt  = find_last_time(f'{fnwmrt_path}/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1')
    last_fnwmpr  = find_last_time(f'{fnwmpr_path}/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') + timedelta(hours=23)
    last_fnwmbc  = find_last_time(f'{fnwmbc_path}/202?/202?????.LDASIN_DOMAIN1', '%Y%m%d.LDASIN_DOMAIN1') + timedelta(hours=23)
    
    last_whrean  = find_last_time(f'{whrean_path}/202?/202???.CHRTOUT_DOMAIN1', '%Y%m.CHRTOUT_DOMAIN1') + relativedelta(months=1) - timedelta(hours=1)
    last_whmoni  = find_last_time(f'{whmoni_path}/202?????00.CHRTOUT_DOMAIN1', '%Y%m%d%H.CHRTOUT_DOMAIN1') - timedelta(hours=1)
    last_whespw1, last_whespwu = find_last_time2(f'{whespw_path}/init202?????_update202?????', 'init%Y%m%d', 'update%Y%m%d', '_')
    if not os.path.isdir(f'{whespw_path}/init{last_whespw1:%Y%m%d}_update{last_whespwu:%Y%m%d}/basins'):
        last_whespw1 -= relativedelta(months=1)
    last_whespw2 = last_whespw1 + relativedelta(months=6) - timedelta(days=1)
    if last_whespw1.month==1:
        last_whespw2 = last_whespw1 + relativedelta(months=7) - timedelta(days=1)
    elif last_whespw1.month==12:
        last_whespw2 = last_whespw1 + relativedelta(months=8) - timedelta(days=1)
        
    fcstfiles = glob(f'{whwwrf_path}/????????-????????.CHRTOUT_DOMAIN1')
    fcstfiles.sort()
    lastfcst = os.path.basename(fcstfiles[-1]).split('.')[0]
    last_whwwrf1  = datetime.strptime(lastfcst.split('-')[0], '%Y%m%d'); last_whwwrf1 = last_whwwrf1.replace(tzinfo=pytz.utc)
    last_whwwrf2  = datetime.strptime(lastfcst.split('-')[1], '%Y%m%d'); last_whwwrf2 = last_whwwrf2.replace(tzinfo=pytz.utc) + timedelta(hours=23)
    
    last_scamod   = find_last_time(scamod_path+'/MOD10A1.A202????.nc', 'MOD10A1.A%Y%j.nc')
    
    datastreams = ['Stage-IV Archive', 'Stage-IV Realtime', 'NLDAS-2', 'HRRR Analysis', 'Forcing NRT', 'Forcing PR', 'Forcing Retro',
                   'WRF-Hydro Retro', 'WRF-Hydro NRT', 'ESP-WWRF Fcst', 'WWRF Fcst', 'MODIS Snow', 'Current']
    datastart = [    wy_start,     wy_start,    wy_start,    wy_start,    wy_start,    wy_start,    wy_start,    wy_start,    wy_start, last_whespw1, last_whwwrf1,    wy_start, curr_time]
    dataend   = [last_stage4a, last_stage4r, last_nldas2, last_hrrran, last_fnwmrt, last_fnwmpr, last_fnwmbc, last_whrean, last_whmoni, last_whespw2, last_whwwrf2, last_scamod, curr_time]
    
    dtfmt = '%Y-%m-%d %H:00:00'
    
    print(f'This update:       {curr_time:{dtfmt}}')
    
    for i,d in enumerate(datastreams):
        print(f'{d:22s}: {datastart[i]:{dtfmt}} to {dataend[i]:{dtfmt}}')
    print(f'Last ESP-WWRF-CCA Forecast Update: {last_whespwu:{dtfmt}}')
    
    with open(fnstatus, 'w') as csvfile:
        if True:
            swriter = csv.writer(csvfile, delimiter=',')
            swriter.writerow(datastreams)
            swriter.writerow(datastart)
            swriter.writerow(dataend)
        else:
            swriter = csv.DictWriter(csvfile, delimiter=',', fieldnames=['ID', 'Data Stream', 'Start', 'End'])
            swriter.writeheader()
            for i,d in enumerate(datastreams):
                swriter.writerow({'ID': i, 'Data Stream': d, 'Start': f'{datastart[i]:{dtfmt}}', 'End': f'{datastart[i]:{dtfmt}}'})

    ## sync data to web app folders

    # old separated dash apps
    if False:
        if last_whmoni.month>=10:
            last_riv_moni = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/rivers/CHRTOUT_{last_whmoni:%Y}01-{last_whmoni:%Y%m}.daily.db'
        else:
            last_riv_moni = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/rivers/CHRTOUT_{last_whmoni.year-1:d}10-{last_whmoni:%Y%m}.daily.db'
        
        last_riv_fcst = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/wwrf/output/41/CHRTOUT_{last_whwwrf1:%Y%m%d}-{last_whwwrf2:%Y%m%d}.daily.db'
    
        river_data_dir = f'{config["base_dir"]}/wrf_hydro/{domain}/web/cw3e-water-panel-gcloud/data/nrt/rivers'
        cmd = f'rsync -a {last_riv_moni} {last_riv_fcst} {river_data_dir}/'
        print(cmd); os.system(cmd)

    # new centralized web apps
    
    # cnrfc
    cmd = f'rsync -a {config["base_dir"]}/obs/cdec/fnf {config["base_dir"]}/obs/cdec/snow_* {config["base_dir"]}/web/data/cnrfc/cdec/'
    print(cmd); os.system(cmd)
    cmd = f'rsync -a {config["base_dir"]}/wrf_hydro/basins24/nrt/output/basins/*ed {config["base_dir"]}/wrf_hydro/basins24/nrt/output/basins/sites {config["base_dir"]}/web/data/cnrfc/nrt/'
    print(cmd); os.system(cmd)
    if last_whmoni.month>=10:
        last_riv_moni = f'{config["base_dir"]}/wrf_hydro/cnrfc/nrt/output/rivers/CHRTOUT_{last_whmoni:%Y}01-{last_whmoni:%Y%m}.daily.t.csv.gz'
    else:
        last_riv_moni = f'{config["base_dir"]}/wrf_hydro/cnrfc/nrt/output/rivers/CHRTOUT_{last_whmoni.year-1:d}10-{last_whmoni:%Y%m}.daily.t.csv.gz'
    last_riv_fcst = f'{config["base_dir"]}/wrf_hydro/cnrfc/fcst/wwrf/output/41/CHRTOUT_{last_whwwrf1:%Y%m%d}-{last_whwwrf2:%Y%m%d}.daily.t.csv.gz'
    cmd = f'rsync -a {last_riv_moni} {last_riv_fcst} {config["base_dir"]}/web/data/cnrfc/nrt/rivers/'
    print(cmd); os.system(cmd)
    
    # conus
    cmd = f'rsync -a {config["base_dir"]}/wrf_hydro/conus/nrt/output/basins/huc* {config["base_dir"]}/web/data/conus/nrt/'
    print(cmd); os.system(cmd)
    
    # cbrfc
    for h in ['huc8', 'huc10']:
        cmd = f'rsync -a {config["base_dir"]}/wrf_hydro/conus/nrt/output/basins/{h}/1[45]*  {config["base_dir"]}/wrf_hydro/conus/nrt/output/basins/{h}/160[123]* {config["base_dir"]}/web/data/cbrfc/nrt/{h}/'
        print(cmd); os.system(cmd)
    
    if len(argv)>0:
        if argv[0]=='update_gcloud':
            #os.chdir(f'{config["base_dir"]}/wrf_hydro/{domain}/web')
            os.chdir(f'{config["base_dir"]}/web')
            os.system('gcloud storage rsync imgs gs://cw3e-water-panel.appspot.com/imgs --recursive')
            os.system('gcloud storage rsync data gs://cw3e-water-panel.appspot.com/data --recursive')
            if False:
                os.chdir(f'{config["base_dir"]}/wrf_hydro/{domain1}/web/dash')
                os.system('gcloud app deploy -q --project=cw3e-water-panel')
                os.chdir(f'{config["base_dir"]}/wrf_hydro/{domain2}/web/dash')
                os.system('gcloud app deploy -q --project=cw3e-water-supply')
    
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
