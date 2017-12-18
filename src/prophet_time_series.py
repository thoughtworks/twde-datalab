import pandas as pd
from fbprophet import Prophet
import sys
import os
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))

import evaluation

def load_data():
    filename = "splitter/train.csv"
    print("Loading data from {}".format(filename))
    train = pd.read_csv(filename, parse_dates=['date'])

    filename = 'splitter/validation.csv'
    print("Loading data from {}".format(filename))
    validate = pd.read_csv(filename, parse_dates=['date'])

    return train, validate


def fill_missing_date(df, total_dates):
    idx = df.iloc[-1:,0].values[0]
    for d in set(total_dates)-set(df['date'].unique()):
        idx+=1
        df.loc[idx, ['date', 'item_nbr', 'store_nbr']]= [pd.to_datetime(d), int(df.iloc[0]['item_nbr']), int(df.iloc[0]['store_nbr'])]
    return df


def get_predictions_for_test_good(test_good, train):
    total_dates = train['date'].unique()
    result = pd.DataFrame(columns=['id', 'unit_sales'])
    problem_pairs = []
    for name, y in test_good.groupby(['item_nbr', 'store_nbr']):
        item_nbr=name[0]
        store_nbr = name[1]
        df = train[(train.item_nbr==item_nbr)&(train.store_nbr==store_nbr)]
        print("item_nbr :",item_nbr,"store_nbr :", store_nbr, "df :", df.shape, df['date'].max())
        CV_SIZE = 16 #if you make it bigger, fill missing dates in cv with 0 if any
        TRAIN_SIZE = 365
        total_dates = train['date'].unique()
        df = fill_missing_date(df, total_dates)
        df = df.sort_values(by=['date'])
        X = df[-TRAIN_SIZE:]
        print('Train on: {}'.format(X.shape))
        X = X[['date','unit_sales']]
        X.columns = ['ds', 'y']
        m = Prophet(yearly_seasonality=True)
        try:
            m.fit(X)
        except ValueError:
            print("problem for this item store pair")
            problem_pairs.append((item_nbr, store_nbr))
            continue
        future = m.make_future_dataframe(periods=CV_SIZE)
        pred = m.predict(future)
        data = pred[['ds','yhat']].tail(CV_SIZE)
        data = pred[['ds','yhat']].merge(y, left_on='ds', right_on='date')
        data['unit_sales'] = data['yhat'].fillna(0).clip(0, 999999)
        result = result.append(data[['id', 'unit_sales']])
        print("result", result.shape)
    return (result, problem_pairs)


def write_predictions_and_score(validation_score, model, columns_used):
    key = "time_series"
    if not os.path.exists(key):
        os.makedirs(key)

    filename = './{}/score_and_metadata.csv'.format(key)
    print("Writing to {}".format(filename))
    score = pd.DataFrame({'estimate': [validation_score], 'columns_used': [columns_used]})
    score.to_csv(filename, index=False)

    print("Done deciding with trees")


def main():
    train, validate = load_data()

    print("Not predicting returns...")
    train.loc[train['unit_sales']<0, 'unit_sales']=0
    validate.loc[validate['unit_sales']<0, 'unit_sales']=0

    print("train.dtypes +++++++++++++++>>>")
    print(train.dtypes)

    validation_predictions, problem_pairs_part = get_predictions_for_test_good(validate, train)

    print("Calculating estimated error")
    validation_score = evaluation.nwrmsle(validation_predictions, validate['unit_sales'], validate['perishable'])

    write_predictions_and_score(validation_score, 0, train.columns)

    print("Decision tree analysis done with a validation score (error rate) of {}.".format(validation_score))


if __name__ == "__main__":
    main()
