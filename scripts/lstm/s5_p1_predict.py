import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import torch, random
import numpy as np
import pandas as pd
import xarray as xr
from monflowpred import utils, normalize, core
from monflowpred.train import TrainLSTM
from torch.utils.data import DataLoader
from matplotlib import pyplot as plt

def cal_rmse(pred, obs):
    err = np.array(pred) - np.array(obs)
    rmse = np.sqrt(np.mean(np.power(err,2)))
    return(rmse)


def cal_nse(pred, obs):
    errup = np.power(np.array(pred) - np.array(obs),2)
    errdn = np.power(np.array(obs)- np.mean(np.array(obs)), 2)
    nse = 1-np.sum(errup)/np.sum(errdn)
    return(nse)

def cal_kge(pred, obs):
    cc   = np.corrcoef(pred, obs)[0,1]
    pavg = np.mean(pred)
    pstd = np.std(pred)
    oavg = np.mean(obs)
    ostd = np.std(obs)
    kge  = 1-np.sqrt( np.power(1-cc,2)+ np.power(pavg/oavg-1,2)+np.power(pstd/ostd-1,2) )
    return(kge)

def plt_mon(pred, qsim, obs, outf, lbs):
    rmseml = cal_rmse(pred, obs)
    nse = cal_nse(pred, obs)

    rmsesim = cal_rmse(qsim, obs)
    nsesim = cal_nse(qsim, obs)

    ccml   = np.corrcoef(pred, obs)[0,1]
    kgeml  = cal_kge(pred, obs)

    print(nse, nsesim, ccml, lbs)

    fig = plt.figure(figsize=(7.5,3.55))
    plt.plot(obs,  'k--', label='OBS, '+lbs)
    plt.plot(pred, 'r', label='LSTM (RSME='+"{:.2f}".format(rmseml)+', NSE='+"{:.2f}".format(nse) + ")")

    plt.plot(qsim, 'c', label='WRF-hyd (RSME='+"{:.2f}".format(rmsesim)+', NSE='+"{:.2f}".format(nsesim) + ")")
    plt.ylabel('Monthly FLow (Kaf)')
    plt.xticks()
    plt.legend(framealpha=0)
    plt.savefig(outf, dpi=180)
    plt.close()

def main(argv):
    
    # Fix random seed (Yuan's version of hydroDL)
    seedid = 111111
    random.seed(seedid)
    torch.manual_seed(seedid)
    np.random.seed(seedid)
    torch.cuda.manual_seed(seedid)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    ################################################
    ################# Header Ends ##################
    ################################################

    

    config = argv[0]
    output_dir = config['INPUT']['output_dir']

    ###### Read static inputs
    print('Reading static parameters...')
    df_stc, n_stc_var = utils.read_stc_inputs(config)
    df_stc_ori = df_stc.copy(deep=True)

    ###### Read dynamic inputs and natural flow (target)
    print('Reading dynamic inputs for each basin...')
    xr_dyn_rec, [n_dyn_var, n_stn] = utils.read_dyn_inputs(config)
    flow_rec = utils.read_flow_obs(config)
    xr_dyn_ori = xr_dyn_rec.copy(deep=True)

    ###### Adjust units, prec to mm
    #xr_dyn_rec['prec'][:] = xr_dyn_rec['prec'][:]*3600*24  ### adjust prcp to daily
    #df_stc_ori['p_mean'][:] = df_stc_ori['p_mean'][:]*3600*24  ### adjust prcp to daily
    kaf_2_mm3 = 1.2335e15

    ###### For each station, divide flow with (area*mean_p)
    for ni in range(n_stn):
        areai = df_stc_ori['size'].iloc[ni]
        pavgi = df_stc_ori['p_mean'].iloc[ni]
        tmpc1 = kaf_2_mm3 ### Kaf to mm^3
        tmpc2 = 10 ** 6   ### m^2 to mm^2
        tmpc3 = 1.
        flow_rec.loc[dict(id=ni)] = tmpc1/(tmpc2*tmpc3) * flow_rec.sel(id=ni) / (areai*pavgi)

    ###### Normalize data, avg and std of the training data are
    ###### also applied in the verifcation and prediction process
    print('Normalize the inputs based on stats of the training period...')
    t_train = config['TEST_PARA']['Ttrain']
    t_valid = config['TEST_PARA']['Tvalid']
    t_valid = config['TEST_PARA']['Tpredc']
    in_tmp  = normalize.norm_dyn(xr_dyn_rec, config,  t_train,  t_valid)
    out_tmp = normalize.norm_dyn(flow_rec,   config,  t_train,  t_valid)
    stc_tmp = normalize.norm_stc(df_stc, config)
    stc_epd = stc_tmp.expand_dims(dim={"time": in_tmp.sizes['time']}, axis=1)

    ###### Convert data to pytorch Dataset Class
    train_all = xr.merge([in_tmp, stc_epd, out_tmp])
    target   = config['TEST_PARA']['target_var']
    features = [xs for xs in train_all.keys() if xs not in target]
    print(target)
    print(features)

    train_dataset = core.seqDataset(
        train_all,
        target=target,
        features=features,
        seq_len=12
    )
    train_loader = DataLoader(train_dataset, batch_size=1, shuffle=False)


    ###### Train model and save at selected epoch
    nx = n_dyn_var + n_stc_var
    ny = 1
    nt = int(config['HYPER_PARA']['rho'])

    lossfunc = torch.nn.MSELoss()
    lossfunc = core.RMSE_Loss()

    nepoch      =  config['HYPER_PARA']['nepoch']
    saveEpoch   =  config['HYPER_PARA']['EPOCHsave']
    saveFolder  =  config['INPUT']['savemodel_dir']
    ## saveFolder  =  './saved.model.with.swe/'
    #saveFolder  =  './saved.model/'

    epoch_use = 350

    if 2>1:
        modelFile = os.path.join(
            saveFolder, "model_Ep" + str(epoch_use) + ".pt"
            )
        model = torch.load(modelFile)
        loss_ttl = 0.
        ####### main loop over data
        outrec = []
        for (batch_idx, batch) in enumerate(train_loader):
            xi = batch[0][0,:,:,:]
            yi = batch[1][0,:,:,:]
            model_out  = model(xi)
            outrec.append(normalize.trans_to_flow(model_out, flow_rec, config, t_train) )

    y_predict = np.concatenate((outrec[:]), axis=0)


    ###### flow mulitply by area and pavg
    for ni in range(n_stn):
        areai = df_stc_ori['size'].iloc[ni]
        pavgi = df_stc_ori['p_mean'].iloc[ni]
        flow_rec.loc[dict(id=ni)] = flow_rec.sel(id=ni) * (areai) * (tmpc2*tmpc3/tmpc1) * pavgi
        y_predict[:,ni] = y_predict[:,ni] * (areai) * (tmpc2*tmpc3/tmpc1) * pavgi

    obs = flow_rec.sel(time=slice(t_valid[0], t_valid[1]))
    qsim = xr_dyn_ori['Qsim'].sel(time=slice(t_valid[0], t_valid[1]))

    nams = pd.read_csv(config['INPUT']['bs_name_ls'], names=['nums','ids','nams'])

    output_dir = config['INPUT']['output_dir']

    for bi in range(config['HYPER_PARA']['batch_size']):
        predbi = pd.Series(y_predict[:,bi])
        #predbi.index = pd.date_range(t_valid[0], t_valid[1], freq='ME')
        predbi.index = pd.date_range(t_valid[0], t_valid[1], freq='M')
        obsbi = pd.Series(obs.isel(id=bi).to_array()[0,:])
        #obsbi.index = pd.date_range(t_valid[0], t_valid[1], freq='ME')
        obsbi.index = pd.date_range(t_valid[0], t_valid[1], freq='M')

        qsimbi = pd.Series(qsim.isel(id=bi)[:])
        #qsimbi.index = pd.date_range(t_valid[0], t_valid[1], freq='ME')
        qsimbi.index = pd.date_range(t_valid[0], t_valid[1], freq='M')


        predbi.to_csv(output_dir+'/'+str(bi+1)+'.predicted-flow.'+t_valid[0]+'-'+t_valid[1]+'.csv',index_label='date', header=['flow'])

if __name__ == '__main__':
    main(sys.argv[1:])
