import numpy  as np
import pandas as pd
import xarray as xr
import torch

'''
Functions that implements the normalizing and reverse process.
If the variables belongs to flow and precipitation, apply
log transform as in equation (11) in Feng et al.(2020).
https://doi.org/10.1029/2019WR026793
'''

def norm_dyn(data_ori, config, t_train, t_out):
    data = data_ori.copy(deep=True)
    log_list   = config['TEST_PARA']['log_var_list']
    inputs  = data.sel(time=slice(t_train[0], t_train[1]))
    outputs = data.sel(time=slice(t_out[0]  , t_out[1]))
    for vari in inputs.keys(): ### loop over all variables
        xtmp = inputs[vari]
        xout = outputs[vari]
        if vari in log_list:
            xtmp = np.log10(np.sqrt(xtmp)+0.1) ### eqt(11) in Feng et al.
            xout = np.log10(np.sqrt(xout)+0.1) ### eqt(11) in Feng et al.
        avg = xtmp.mean()
        std = xtmp.std()

        if std<0.001: ### from hydroDL2.0
            std = 1
        xout = (xout-avg) / std
        outputs[vari][:] = xout[:]
    return outputs


def norm_stc(data_ori, config):
    inputs = data_ori.to_xarray()
    log_list   = config['TEST_PARA']['log_var_list']
    for vari in inputs.keys(): ### loop over all variables
        if vari != 'id':
            xtmp = inputs[vari]
            if vari in log_list:
                xtmp = np.log10(np.sqrt(xtmp)+0.1) ### eqt(11) in Feng et al.
            avg = xtmp.mean()
            std = xtmp.std()
            if std<0.001: ### from hydroDL2.0
                std = 1
            xtmp = (xtmp-avg) / std
            inputs[vari][:] = xtmp[:]
    return inputs


def trans_to_flow(y_in, data_ori, config, t_train):
    target = config['TEST_PARA']['target_var'][0]
    y_out = y_in.detach().numpy()

    xtmp = data_ori[target].sel(time=slice(t_train[0], t_train[1]))
    xtmp = np.log10(np.sqrt(xtmp)+0.1) ### eqt(11) in Feng et al.
    avg = np.float64(xtmp.mean())
    std = np.float64(xtmp.std())

    y_out = y_out*std + avg
    y_final = np.power(np.power(10, y_out)-0.1, 2)

    return y_final[:,:,0]
