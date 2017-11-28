import warnings
warnings.filterwarnings("ignore")

import sys

import pandas as pd
import datetime
from statsmodels.tsa.arima_model import ARIMA

# FORECASTS THE UNIT SALES FOR ONE ITEM/STORE COMBINATION
# Usage example: 'python src/arima_per_item_forecast.py ./splitter/train.csv ./splitter/test.csv 1503844 47 ./arima_forecast/'
if __name__ == "__main__":
    pathToTrainingSetFile = sys.argv[1]
    pathToValidationSetFile = sys.argv[2]
    itemNumber = sys.argv[3]
    storeNumber = sys.argv[4]
    predictionsOutputPath = sys.argv[5]
    predictionsOutputFile = predictionsOutputPath + 'predictions_' + itemNumber + '_' + storeNumber + '.csv'

    train=pd.read_csv(pathToTrainingSetFile)
    train['date']=train.date.apply(lambda x:datetime.datetime.strptime(x, '%Y-%m-%d'))
    ts=train.loc[(train['store_nbr'] == storeNumber) & (train['item_nbr'] == itemNumber), ['date', 'unit_sales']].set_index('date')
    ts=ts.unit_sales.astype('float')

    test=pd.read_csv(pathToValidationSetFile)
    test['date']=test.date.apply(lambda x:datetime.datetime.strptime(x, '%Y-%m-%d'))
    ts_test=test.loc[(train['store_nbr'] == storeNumber) & (train['item_nbr'] == itemNumber), ['date', 'unit_sales']].set_index('date')
    ts_test=ts_test.unit_sales.astype('float')

    pdq=(4,0,4)
    model = ARIMA(ts, order = pdq, freq='W')
    model_fit = model.fit(disp=False,method='css',maxiter=100)

    history = [x for x in ts]
    predictions = list()

    print('Starting the ARIMA predictions...')
    print('\n')
    for t in range(len(ts_test)):
        try:
            model = ARIMA(history, order = pdq, freq='W');
            model_fit = model.fit(disp=0);
            output = model_fit.forecast()
            yhat = output[0]
            predictions.append(float(yhat))
            obs = ts_test[t]
            history.append(obs)
            print('all good...')
        except:
            print('Caught an exception. Decrease p and/or q if you see this too often')
            pass
    print('Predictions finished.\n')

    predictions_series = pd.Series(predictions, index = ts_test.index)

    predictions_series.to_csv(predictionsOutputFile, index=False)

