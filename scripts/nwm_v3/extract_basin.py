''' Extract CNRFC basin averaged quantities from WRF-Hydro

Usage:
    python extract_basin.py [domain] [yyyymm1] [yyyymm2] [retro|nrt|fcst/xxx]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import sys, os, pytz, time
import netCDF4 as nc
from glob import glob
import pandas as pd
import numpy as np
import xarray as xr
from scipy import ndimage
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

modelid = 'nwm_v3'

def extract_chrt(domain, df_basins, fchrt):
    
    # ==== USER INPUTS ====
    #variables_to_sum = ["q_lateral", "qBucket", "qSfcLatRunoff", "qBtmVertRunoff"]  # variables to sum
    variables_to_sum = ["qBucket", "qSfcLatRunoff", "q_lateral", "qBtmVertRunoff"]  # variables to sum
    id_column = "feature_id"                           # column name in CSVs
    frac_column = "fraction"                            # column name in CSVs
    list_path = f'{config["base_dir"]}/{modelid}/{domain}/domain/basin_reach_list_fraction'    # folder containing your CSV files
    # ======================


    # 1) Open once, load variables to memory for speed
    ds = xr.open_dataset(fchrt)
    ds_mem = ds[variables_to_sum].load()

    # Assume a common feature_id coordinate across variables
    dataset_ids = ds.coords["feature_id"].values

    # 2) Process each CSV list
    combined_all = []
    for basin in df_basins['Basin']:
        
        df = pd.read_csv(f'{list_path}/{basin}.csv')

        # Coerce types; drop rows with missing IDs; fill NaN fractions with 0
        df = df[[id_column, frac_column]].copy()
        df = df.dropna(subset=[id_column])
        df[frac_column] = pd.to_numeric(df[frac_column], errors="coerce").fillna(0.0)
        # If there are duplicate feature_id rows, aggregate their fractions
        df = df.groupby(id_column, as_index=True)[frac_column].sum()

        # Clip fractions to [0,1] to be safe
        df = df.clip(lower=0.0, upper=1.0)

        csv_ids = df.index.values.astype(dataset_ids.dtype, copy=False)

        # Intersect with dataset IDs and get indices for alignment
        common_ids, idx_csv, idx_ds = np.intersect1d(
            csv_ids, dataset_ids, return_indices=True
        )

        # Aligned weights
        weights = df.values[idx_csv]  # fractions aligned to common_ids

        # Build a weight DataArray on feature_id for broadcasting over time
        w_da = xr.DataArray(
            weights,
            dims=["feature_id"],
            coords={"feature_id": common_ids},
            name="weights",
        )

        combined = None

        # 3) Weighted sum for each variable (all data already in memory)
        for var in variables_to_sum:
            if var not in ds_mem.data_vars:
                print(f"Variable '{var}' not found; skipping.")
                continue

            # Select only the common feature_ids and broadcast weights
            var_sel = ds_mem[var].sel(feature_id=common_ids, drop=True)

            # Multiply by weights and sum
            var_wsum = (var_sel * w_da).sum(dim="feature_id")

            # Give the DataArray a name before converting
            var_wsum.name = var

            # Convert to DataFrame
            var_df = var_wsum.to_dataframe().reset_index()

            # Merge across variables on 'time'
            combined = var_df if combined is None else combined.merge(var_df, on="time")

        # 4) Save one CSV per list with all variables’ weighted sums
        # convert m^3 to m^3/s
        combined['qBtmVertRunoff'] = combined['qBtmVertRunoff']/3600.
        combined.rename(columns={'time': 'Date'}, inplace=True)
        combined_all.append(combined)

    return combined_all

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]

    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    ptype = argv[3]

    fbasin = nc.Dataset(f'{config["base_dir"]}/{modelid}/{domain}/domain/{domain.upper()}_Basins_ID_lcc.nc', 'r')
    basin_data = fbasin['basin_id'][:]
    fbasin.close()

    df_basins = pd.read_csv(f'{config["base_dir"]}/{modelid}/{domain}/domain/{domain.upper()}_Basins_ID_lcc.csv')
    
    workdir = f'{config["base_dir"]}/{modelid}/{domain}/{ptype}/output'
    os.chdir(workdir)

    t = t1
    while t<=t2:

        print(f'{t:%Y-%m-%d}')

        # daily NoahMP output
        print('  daily NoahMP output')
        fin = nc.Dataset(f'1km_daily/{t:%Y/%Y%m}.LDASOUT_DOMAIN1.daily', 'r')
        ntimes = fin['time'].size
        if t==t1:
            tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-%d') for i in range(ntimes)]
        else:
            tstamps.extend([nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-%d') for i in range(ntimes)])
        #print(tstamps)
        for i in range(ntimes):
            data_swe = np.squeeze(fin['SNEQV'][i, :, :])
            data_sm  = np.squeeze(fin['SOIL_M'][i, :, 0, :]*0.05 + fin['SOIL_M'][i, :, 1, :]*0.15 + fin['SOIL_M'][i, :, 2, :]*0.3 + fin['SOIL_M'][i, :, 3, :]*0.5)
            if t==t1 and i==0:
                basin_data[data_swe.mask] = 0
                basin_ids = np.unique(basin_data)
                basin_ids = basin_ids[basin_ids!=0]
                basin_means_swe = ndimage.mean(data_swe, labels=basin_data, index=basin_ids)
                basin_means_sm  = ndimage.mean(data_sm,  labels=basin_data, index=basin_ids)
            else:
                basin_means_swe = np.vstack((basin_means_swe, ndimage.mean(data_swe, labels=basin_data, index=basin_ids)))
                basin_means_sm  = np.vstack((basin_means_sm,  ndimage.mean(data_sm,  labels=basin_data, index=basin_ids)))
        fin.close()

        # daily forcing
        print('  daily forcing')
        fin = nc.Dataset(f'../forcing/1km_daily/{t:%Y/%Y%m}.LDASIN_DOMAIN1.daily', 'r')
        for i in range(ntimes):
            data_p = np.squeeze(fin['RAINRATE'][i, :, :]*86400)
            data_t = np.squeeze(fin['T2D'][i, :, :]-273.15)
            if t==t1 and i==0:
                basin_means_p = ndimage.mean(data_p, labels=basin_data, index=basin_ids)
                basin_means_t = ndimage.mean(data_t, labels=basin_data, index=basin_ids)
            else:
                basin_means_p = np.vstack((basin_means_p, ndimage.mean(data_p, labels=basin_data, index=basin_ids)))
                basin_means_t = np.vstack((basin_means_t, ndimage.mean(data_t, labels=basin_data, index=basin_ids)))
        fin.close()

        # daily routing output
        print('  daily routing output')
        fin = f'1km_daily/{t:%Y/%Y%m}.CHRTOUT_DOMAIN1.daily'
        basin_sums = extract_chrt(domain, df_basins, fin)
        if t==t1:
            basin_sums_all = basin_sums
        else:
            for i in range(len(basin_sums_all)):
                basin_sums_all[i] = pd.concat([basin_sums_all[i], basin_sums[i]])

        if ptype.split('/')[0]!='fcst':
            
            # monthly NoahMP output
            print('  monthly NoahMP output')
            if t==t1:
                tstamps_mon = [t.strftime('%Y-%m-%d')]
            else:
                tstamps_mon.append(t.strftime('%Y-%m-%d'))
            fin = nc.Dataset(f'1km_monthly/{t:%Y/%Y%m}.LDASOUT_DOMAIN1.monthly', 'r')
            data_swe_mon = np.squeeze(fin['SNEQV'][0, :, :])
            data_sm_mon  = np.squeeze(fin['SOIL_M'][0, :, 0, :]*0.05 + fin['SOIL_M'][0, :, 1, :]*0.15 + fin['SOIL_M'][0, :, 2, :]*0.3 + fin['SOIL_M'][0, :, 3, :]*0.5)
            if t==t1:
                basin_means_swe_mon = ndimage.mean(data_swe_mon, labels=basin_data, index=basin_ids)
                basin_means_sm_mon  = ndimage.mean(data_sm_mon,  labels=basin_data, index=basin_ids)
            else:
                basin_means_swe_mon = np.vstack((basin_means_swe_mon, ndimage.mean(data_swe_mon, labels=basin_data, index=basin_ids)))
                basin_means_sm_mon  = np.vstack((basin_means_sm_mon,  ndimage.mean(data_sm_mon,  labels=basin_data, index=basin_ids)))
            fin.close()
        
            # monthly forcing
            print('  monthly forcing')
            fin = nc.Dataset(f'../forcing/1km_monthly/{t:%Y/%Y%m}.LDASIN_DOMAIN1.monthly', 'r')
            md = monthrange(t.year, t.month)[1]
            data_p_mon = np.squeeze(fin['RAINRATE'][0, :, :]*86400*md)
            data_t_mon = np.squeeze(fin['T2D'][0, :, :]-273.15)
            if t==t1:
                basin_means_p_mon = ndimage.mean(data_p_mon, labels=basin_data, index=basin_ids)
                basin_means_t_mon = ndimage.mean(data_t_mon, labels=basin_data, index=basin_ids)
            else:
                basin_means_p_mon = np.vstack((basin_means_p_mon, ndimage.mean(data_p_mon, labels=basin_data, index=basin_ids)))
                basin_means_t_mon = np.vstack((basin_means_t_mon, ndimage.mean(data_t_mon, labels=basin_data, index=basin_ids)))
            fin.close()
        
            # monthly routing output
            print('  monthly routing output')
            fin = f'1km_monthly/{t:%Y/%Y%m}.CHRTOUT_DOMAIN1.monthly'
            basin_sums_mon = extract_chrt(domain, df_basins, fin)
            if t==t1:
                basin_sums_mon_all = basin_sums_mon
            else:
                for i in range(len(basin_sums_mon_all)):
                    basin_sums_mon_all[i] = pd.concat([basin_sums_mon_all[i], basin_sums_mon[i]])
        
        # hourly routing output
        print('  hourly routing output')
        fin = f'1km_hourly/{t:%Y/%Y%m}.CHRTOUT_DOMAIN1'
        basin_sums_hr = extract_chrt(domain, df_basins, fin)
        if t==t1:
            basin_sums_hr_all = basin_sums_hr
        else:
            for i in range(len(basin_sums_hr_all)):
                basin_sums_hr_all[i] = pd.concat([basin_sums_hr_all[i], basin_sums_hr[i]])
        
        t += relativedelta(months=1)

    basin_means_swe = np.atleast_2d(basin_means_swe)
    basin_means_sm  = np.atleast_2d(basin_means_sm)
    basin_means_p   = np.atleast_2d(basin_means_p)
    basin_means_t   = np.atleast_2d(basin_means_t)
    
    if ptype.split('/')[0]!='fcst':
        freqs = ['hourly', 'daily', 'monthly']
        basin_means_swe_mon = np.atleast_2d(basin_means_swe_mon)
        basin_means_sm_mon  = np.atleast_2d(basin_means_sm_mon)
        basin_means_p_mon   = np.atleast_2d(basin_means_p_mon)
        basin_means_t_mon   = np.atleast_2d(basin_means_t_mon)
    else:
        freqs = ['hourly', 'daily']
            
    # output dir
    outds = {}
    for freq in freqs:
        outds[freq] = f'basins/{freq}'
        if ptype=='retro':
            outds[freq] += f'/{t1:%Y%m}-{t2:%Y%m}'
        os.makedirs(outds[freq], exist_ok=True)

    print(f'Writing data: total={basin_ids.size} basins')

    dfs = {}
    for j in range(basin_ids.size):
        basin_name = df_basins.loc[df_basins['ID']==basin_ids[j], 'Basin'].to_list()[0] 

        if j%10==0:
            print(f'    {j+1}th: {basin_name}')
        # daily
        dfs['daily'] = pd.DataFrame({'Date': pd.to_datetime(tstamps, format='%Y-%m-%d')})
        dfs['daily']['SWE']   = basin_means_swe[:, j]
        dfs['daily']['SMTOT'] = basin_means_sm[:, j]
        dfs['daily']['T2D']   = basin_means_t[:, j]
        dfs['daily']['PREC']  = basin_means_p[:, j]
        dfs['daily']['RUNOFF']= basin_sums_all[j]['qBucket'].to_numpy() + basin_sums_all[j]['qSfcLatRunoff'].to_numpy()

        if ptype.split('/')[0]!='fcst':
            # monthly
            dfs['monthly'] = pd.DataFrame({'Date': pd.to_datetime(tstamps_mon, format='%Y-%m-%d')})
            dfs['monthly']['SWE']   = basin_means_swe_mon[:, j]
            dfs['monthly']['SMTOT'] = basin_means_sm_mon[:, j]
            dfs['monthly']['T2D']   = basin_means_t_mon[:, j]
            dfs['monthly']['PREC']  = basin_means_p_mon[:, j]
            dfs['monthly']['RUNOFF']= basin_sums_mon_all[j]['qBucket'].to_numpy() + basin_sums_mon_all[j]['qSfcLatRunoff'].to_numpy()

        # hourly
        basin_sums_hr_all[j]['RUNOFF'] = basin_sums_hr_all[j]['qBucket'] + basin_sums_hr_all[j]['qSfcLatRunoff']
        dfs['hourly'] = basin_sums_hr_all[j]

        for freq in freqs:
            fnout = f'{outds[freq]}/{basin_name}_{freq}.csv.gz'
            dfs[freq].set_index('Date', inplace=True)
            
            if ptype=='nrt' and os.path.isfile(fnout):
                df0 = pd.read_csv(fnout, parse_dates=True, index_col='Date')
                dfs[freq] = pd.concat([df0, dfs[freq]])
                dfs[freq] = dfs[freq].loc[~dfs[freq].index.duplicated(keep='last')]

            if freq=='hourly':
                dfs[freq].to_csv(fnout, compression='gzip', float_format='%.4f')
            else:
                dfs[freq].to_csv(fnout, compression='gzip', float_format='%.4f', date_format='%Y-%m-%d')
        
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
