import numpy  as np
import pandas as pd
import xarray as xr


def read_stc_inputs(config):
    df_static  = pd.read_csv(config['INPUT']['static_inputs'], index_col=0) 
    #### remove unnecessary variables
    stc_raw_ls = list(df_static)
    stc_list   = config['TEST_PARA']['stc_var_list']
    for varf in stc_raw_ls:
        if varf in stc_list:
            continue
        else: 
            df_static.drop(columns=[varf], inplace=True)
    [n_stn, n_stc_var] = np.shape(df_static.to_numpy())
    return df_static, n_stc_var



def read_dyn_inputs(config):
    with open(config['INPUT']['basin_listf']) as f:
        basinlist = [line.rstrip() for line in f]
    forcdir = config['INPUT']['dynamic_dir']
    dfls = []
    for bi in basinlist:
        tmpd = pd.read_csv(forcdir+bi+'.csv')
        dindex = pd.to_datetime(tmpd['indx'])
        #### remove unnecessary variables
        dyn_raw_ls = list(tmpd)
        dyn_list   = config['TEST_PARA']['dyn_var_list']
        for varf in dyn_raw_ls:
            if varf not in dyn_list:
                tmpd.drop(columns=[varf], inplace=True)
        tmpd.index = dindex
        dfls.append(tmpd.to_xarray())
    ds_all = xr.concat(dfls,  'id') 
    ds_all = ds_all.assign_coords(id=('id', np.arange(len(dfls)) ) )
    ds_all = ds_all.rename({'indx': 'time'})

    [n_dyn_var, n_stn, n_step] = np.shape(ds_all.to_array())
    return ds_all, [n_dyn_var, n_stn]

def read_dyn_inputs1(config):
    with open(config['INPUT']['basin_listf']) as f:
        basinlist = [line.rstrip() for line in f]
    forcdir = config['INPUT']['dynamic_dir']
    dfls = []
    for bi in basinlist:
        tmpd = pd.read_csv(forcdir+bi+'.csv')
        dindex = pd.to_datetime(tmpd['indx'])
        #### remove unnecessary variables
        dyn_raw_ls = list(tmpd)
        dyn_list   = config['TEST_PARA']['dyn_var_list1']
        for varf in dyn_raw_ls:
            if varf not in dyn_list:
                tmpd.drop(columns=[varf], inplace=True)
        tmpd.index = dindex
        dfls.append(tmpd.to_xarray())
    ds_all = xr.concat(dfls,  'id') 
    ds_all = ds_all.assign_coords(id=('id', np.arange(len(dfls)) ) )
    ds_all = ds_all.rename({'indx': 'time'})

    [n_dyn_var, n_stn, n_step] = np.shape(ds_all.to_array())
    return ds_all, [n_dyn_var, n_stn]


def read_flow_obs(config):
    with open(config['INPUT']['basin_listf']) as f:
        basinlist = [line.rstrip() for line in f]
    forcdir = config['INPUT']['dynamic_dir']
    dfls = []
    for bi in basinlist:
        tmpd = pd.read_csv(forcdir+bi+'.csv')
        dindex = pd.to_datetime(tmpd['indx'])
        #### remove unnecessary variables
        dyn_raw_ls = list(tmpd)
        dyn_list   = config['TEST_PARA']['target_var']
        for varf in dyn_raw_ls:
            if varf not in dyn_list:
                tmpd.drop(columns=[varf], inplace=True)
        tmpd.index = dindex
        dfls.append(tmpd.to_xarray())
    ds_all = xr.concat(dfls,  'id') 
    ds_all = ds_all.assign_coords(id=('id', np.arange(len(dfls)) ) )
    ds_all = ds_all.rename({'indx': 'time'})

    return ds_all

