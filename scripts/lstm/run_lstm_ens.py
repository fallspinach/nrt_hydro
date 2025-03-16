''' Run LSTM post-processing for ensemble forecast

Usage:
    python run_lstm_ens.py [domain] [fcst_start] [fcst_end] [fcst_update] [fcst_type]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import sys, os, pytz, time, subprocess
from glob import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time, replace_brackets

import s5_p1_predict

t1_hist = datetime(1979, 10, 1)
t2_hist = datetime(2024, 9, 30)

## main function
def main(argv):

    domain = argv[0]

    t1 = datetime.strptime(argv[1], '%Y%m%d')
    t2 = datetime.strptime(argv[2], '%Y%m%d')
    tupdate = datetime.strptime(argv[3], '%Y%m%d')
    
    workdir  = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{argv[4]}/output/init{t1:%Y%m%d}_update{tupdate:%Y%m%d}'
    inputdir = f'{config["base_dir"]}/wrf_hydro/{domain}/lstm'
    histdir  = f'{inputdir}/hist.dyn.input'
    fcstdir  = f'{workdir}/basins/averaged'
    tmpdir   = f'{workdir}/basins/lstm_tmp'
    obsdir   = f'{inputdir}/csv_cdec'
    outdir   = f'{workdir}/basins/lstm'

    namls = pd.read_csv(f'{inputdir}/stn.names.24.txt', header=None, names=['num', 'id', 'name'], dtype={'num': int, 'id': str, 'name': str})
    #print(namls)
    
    os.chdir(workdir)
    nens = len(glob('??'))

    if not os.path.isdir(f'{tmpdir}/01'):
        allens = ' '.join([f'{tmpdir}/{e:02d}' for e in range(1, nens+1)])
        os.system(f'mkdir -p {allens}')
    if not os.path.isdir(outdir):
        os.system(f'mkdir -p {outdir}')

    # concatennate historical and ensemble forecast data
    print('Concatennate historical and ensemble forecast data...')
    for num,id in zip(namls['num'], namls['id']):
        df_hist = pd.read_csv(f'{histdir}/{id}.{t1_hist:%Y%m}-{t2_hist:%Y%m}.dyn.csv')
        for ens in range(1, nens+1):
            df_ens = pd.read_csv(f'{fcstdir}/{ens:02d}/{id}_monthly.csv')
            df_ens['FNF'] = 9999.0
            df_ens.rename(columns={'Date': 'indx'}, inplace=True)
            df_tmp = pd.concat([df_hist, df_ens], ignore_index=True)
            df_tmp.to_csv(f'{tmpdir}/{ens:02d}/{num}.csv', index=False)
    
    # lstm predictions
    for ens in range(1, nens+1):
        config_lstm = config['wrf_hydro'][domain]['lstm']
        config_lstm['INPUT'] = {'basin_listf':   f'{inputdir}/basin_24_list.txt',
                                'dynamic_dir':   f'{tmpdir}/{ens:02d}/',
                                'static_inputs': f'{inputdir}/basin_24_stable_vars.txt',
                                'bs_name_ls':    f'{inputdir}/stn.names.24.txt',
                                'output_dir':    f'{tmpdir}/{ens:02d}/',
                                'savemodel_dir': f'{inputdir}/'}
        # shift Tpredc to a new 48-month period that ends at t2
        t1_pred = datetime.strptime(config_lstm['TEST_PARA']['Tpredc'][0], '%Y%m%d')
        t2_pred = datetime.strptime(config_lstm['TEST_PARA']['Tpredc'][1], '%Y%m%d')
        monoff  = round((t2-t2_pred).days/30)
        t2_pred = t2
        t1_pred += relativedelta(months=monoff)
        config_lstm['TEST_PARA']['Tpredc'][0] = f'{t1_pred:%Y%m%d}'
        config_lstm['TEST_PARA']['Tpredc'][1] = f'{t2_pred:%Y%m%d}'
        s5_p1_predict.main([config_lstm])

    # force the end date to the preset tvalid period
    # t2 = datetime.strptime(config_lstm['TEST_PARA']['Tpredc'][1], '%Y%m%d')
    
    # merge ensembles and calculate percentiles
    index2 = []; t = t1; nmons = 0
    while t<t2:
        index2.append(f'{t:%Y-%m-%d}')
        if t.month==4:
            mapr = nmons
        if t.month==7:
            mjul = nmons
        nmons += 1
        t += relativedelta(months=1)
    index2.append(f'{t2:%Y}-07-31')
    print(f'Number of months in forecast = {nmons}, April is {mapr}th month and July is {mjul}th month (counting from 0)')
    header = [f'Ens{e:02d}' for e in range(1,nens+1)] + ['Exc10','Exc50','Exc90','Pav10','Pav50','Pav90','Avg']
    print('Merge ensembles and calculate percentiles...')
    for num,id in zip(namls['num'], namls['id']):
        
        #### cal obs long-term avg 1979-2020
        fnf       = pd.read_csv(f'{obsdir}/{id}.csv')
        fnf.index = pd.to_datetime(fnf['Date'])
        fnf       = fnf['1979':'2020']
        flowavg   = fnf['FNF'].groupby(fnf.index.month).mean()
        flowuse   = np.array(flowavg.iloc[t1.month-1:t2.month])
        #print(flowuse)
        #print(flowavg)

        mcol = len(header)
        rec  = np.zeros((nmons+1,mcol))-999.

        #### record ensemble members
        for ens in range(nens):
            flw = pd.read_csv(f'{tmpdir}/{ens+1:02d}/{num}.predicted-flow.{t1_pred:%Y%m%d}-{t2:%Y%m%d}.csv')
            flw.index = pd.to_datetime(flw['date'])
            rec[0:nmons,ens] = np.array(flw['flow'].loc[f'{t1:%Y%m%d}':f'{t2:%Y%m%d}'])
            rec[nmons,ens]   = np.sum(np.array(flw['flow'].loc[f'{t2:%Y}0401':f'{t2:%Y}0731']))

        #### calculate p10, p50, p90, avg
        rec[:,nens]   = np.quantile(rec[:,0:nens], 0.9, axis=1)
        rec[:,nens+1]   = np.quantile(rec[:,0:nens], 0.5, axis=1)
        rec[:,nens+2]   = np.quantile(rec[:,0:nens], 0.1, axis=1)
        rec[0:nmons,mcol-1] = flowuse[:]
        print(flowuse)
        rec[nmons,mcol-1]   = np.sum(flowuse[mapr:mjul+1])
        rec[:,nens+3]   = np.divide(rec[:,nens],   rec[:,mcol-1])*100 ## percentage of avg
        rec[:,nens+4]   = np.divide(rec[:,nens+1], rec[:,mcol-1])*100
        rec[:,nens+5]   = np.divide(rec[:,nens+2], rec[:,mcol-1])*100
        dout = pd.DataFrame(rec, columns=header)
        dout.index = pd.to_datetime(index2)
        #dout = dout[f'{t1:%Y%m%d}':f'{t2:%Y%m%d}']
        dout = dout[f'{t1:%Y%m%d}':]
        dout.to_csv(f'{outdir}/{id}_{t1:%Y%m%d}-{t2:%Y%m%d}.csv',float_format="%.3f", index_label='Date')

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
