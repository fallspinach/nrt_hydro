import numpy as np
import pandas as pd
import torch
import time
import os

def TrainLSTM(
    model,
    dataloader,
    lossFunc,
    config
):
    nepoch      =  int(config['HYPER_PARA']['nepoch'])
    saveEpoch   =  int(config['HYPER_PARA']['EPOCHsave'])
    saveFolder  =  config['INPUT']['savemodel_dir']

    optim = torch.optim.Adadelta(model.parameters())
    #optim = torch.optim.Adam(model.parameters(), lr=0.001)
    #optim = torch.optim.SGD(model.parameters(), lr=0.01)
    #optim = torch.optim.RMSprop(model.parameters(), lr=0.001)

    ####### loop over epoch
    for iepoch in np.arange(1, nepoch + 1):

        t0 = time.time()
        loss_ttl = 0.
        model.train()
        optim.zero_grad()

        ####### main loop over train_data for each epoch
        countmx = 0
        for (batch_idx, batch) in enumerate(dataloader):
            xi = batch[0][0,:,:,:]
            yi = batch[1][0,:,:,:]
            model_out  = model(xi)
            loss = lossFunc(model_out, yi.float())
            loss.backward()
            optim.step()
            optim.zero_grad()
            loss_ttl = loss_ttl + loss.item()
            countmx = countmx + 1

        loss_ttl = loss_ttl / countmx
        loss_str = "Epoch {} Loss {:.3f} time {:.2f}".format(
            iepoch, loss_ttl, time.time() - t0
        )
        print(loss_str)
        if saveFolder is not None:
            if iepoch % saveEpoch == 0:
                # save model
                modelFile = os.path.join(
                    saveFolder, "model_Ep" + str(iepoch) + ".pt"
                )
                torch.save(model, modelFile)
    # return model

