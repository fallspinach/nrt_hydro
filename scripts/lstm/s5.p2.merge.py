import sys
import os
import numpy as np
import pandas as pd

################################################
################# Header Ends ##################
################################################

##### output time index
year  = sys.argv[1]
datep = sys.argv[2]
stmon = sys.argv[3]

index = [year+'-'+str(mon).zfill(2)+'-'+'16' for mon in range(1,8)]
index.append(year+'-07-31')
index = pd.to_datetime(index)

##### pre-defined inputs
nams  = pd.read_csv('/home/mxiao/proj/b120.lstm.postpro/s5.gen.flow/stn.names.txt', names=['nums','ids','nams'])
flowd = "/home/mxiao/proj/b120.lstm.postpro/s5.gen.flow/flow.out/"
outd  = "/home/mxiao/proj/b120.lstm.postpro/s5.gen.flow/esp.out.merged/"+year+datep+"/"
obsd  = "/home/mxiao/proj/b120.lstm.postpro/s5.gen.flow/csv_cdec/"
latest_fnf = "/expanse/nfs/cw3e/cwp101/nrt_hydro/wrf_hydro/cnrfc/obs/cdec/fnf/" # FNF_monthly_SIS.csv
latest_fnf = "/expanse/nfs/cw3e/cwp101/nrt_hydro/obs/cdec/fnf/" # FNF_monthly_SIS.csv


##### header for csv output
header = ['Ens'+str(x).zfill(2) for x in range(1,43)]
for x in ['Exc10','Exc50','Exc90','Pav10','Pav50','Pav90','Avg']:
    header.append(x)

##### loop over stns
for i in range(24):
    numi = i+1
    idi  = nams['ids'].iloc[i]
    print(numi, idi)

    #### cal obs long-term avg 1979-2020
    fnf       = pd.read_csv(obsd+idi+'.csv')
    fnf.index = pd.to_datetime(fnf['Date'])
    fnf       = fnf['1979':'2020']
    flowavg   = fnf['FNF'].groupby(fnf.index.month).mean()
    flowuse   = np.array(flowavg.iloc[0:7])

    #### read latest obs record
    latefnf       = pd.read_csv(latest_fnf+'FNF_monthly_'+idi+'.csv')
    latefnf.index = pd.to_datetime(latefnf['Date'])
    latefnf       = latefnf[year+'0401':year+'0731']
    print(latefnf)
    obsapr = latefnf.loc[:,'Flow'].iloc[0]
    obsmay = latefnf.loc[:,'Flow'].iloc[1]


    rec  = np.zeros((8,49))-999.
    #### record ensemble members
    for ens in range(42):
        flw = pd.read_csv(flowd+'/'+str(ens+1).zfill(2)+\
            '/'+str(numi)+'.predicted-flow.20211001-20220930.csv')
        flw.index = pd.to_datetime(flw['date'])
        rec[0:7,ens] = np.array(flw['flow'].loc['20220101':'20220731'])
        rec[7,ens]   = np.sum(np.array(flw['flow'].loc['20220401':'20220731']))

    #### replace Apr with preivous obsapr
    rec[3,:]   = np.zeros((1,49))+obsapr #
    #### replace May with preivous obsapr
    rec[4,:]   = np.zeros((1,49))+obsmay #

#    print(rec)
#    abc

    #### record ensemble members
    for ens in range(42):
        rec[7,ens]   = np.sum(np.array(rec[3:7,ens]))


    #### calculate p10, p50, p90, avg
    rec[:,42]   = np.quantile(rec[:,0:42], 0.9, axis=1)
    rec[:,43]   = np.quantile(rec[:,0:42], 0.5, axis=1)
    #print(rec[:,43])
    #rec[:,43]   = np.mean(rec[:,0:42], axis=1)
    #print(rec[:,43])
    rec[:,44]   = np.quantile(rec[:,0:42], 0.1, axis=1)
    rec[0:7,48] = flowuse[:]
    rec[7,48]   = np.sum(flowuse[3:7])
    rec[:,45]   = np.divide(rec[:,42], rec[:,48])*100 ## percentage of avg
    rec[:,46]   = np.divide(rec[:,43], rec[:,48])*100
    rec[:,47]   = np.divide(rec[:,44], rec[:,48])*100
    dout = pd.DataFrame(rec, columns=header)
    dout.index = index
    dout = dout['20240401':'20240731']
    dout.to_csv(outd+idi+'_'+year+stmon+'01-'+year+'0731.csv',\
         float_format="%.3f", index_label='Date')

