import pandas as pd
import pandas_datareader.data as pdr

from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from pmdarima.arima import auto_arima

import datetime
import itertools
import warnings
warnings.filterwarnings('ignore')

def get_data(df_init):
    df = df_init.set_index(keys='date')

    # train,test split
    train_data, test_data = train_test_split(df, test_size=0.3, shuffle=False)
    dt=datetime.datetime.today()

    data_idx=[]
    for i in range(10):
        delta = datetime.timedelta(days = i)
        dtnew = dt + delta
        data_idx.append(str(dtnew))
    
    # ARIMA 모델 생성
    p = range(0, 2)
    d = range(1, 3)
    q = range(0, 2)
    pdq = list(itertools.product(p, d, q))

    AIC = []
    for i in pdq :
        model = ARIMA(train_data['Close'].values, order=(i))
        model_fit = model.fit()
        print(f'ARIMA pdq : {i} >> AIC : {round(model_fit.aic, 2)}')
        AIC.append(round(model_fit.aic, 2))

    optim = [(pdq[i], j) for i, j in enumerate(AIC) if j == min(AIC)]
    print(optim)

    model = ARIMA(train_data['Close'].values, order=optim[0][0])
    model_fit = model.fit()    

    # 예측값 생성
    pred = model_fit.get_forecast(len(test_data) +10)
    pred_val = pred.predicted_mean

    pred_index = list(test_data.index)
    for i in data_idx:
        pred_index.append(i)
    
    print(pred_val)



get_data(var)