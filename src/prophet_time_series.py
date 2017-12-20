import pandas as pd
from fbprophet import Prophet
import sys
import os
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))

import evaluation

def load_data():
    filename = "data/splitter/train.csv"
    print("Loading data from {}".format(filename))
    train = pd.read_csv(filename, parse_dates=['date'])

    filename = 'data/splitter/validation.csv'
    print("Loading data from {}".format(filename))
    validate = pd.read_csv(filename, parse_dates=['date'])

    return train, validate


def fill_missing_date(df, total_dates):
    idx = df.iloc[-1:,0].values[0]
    for d in set(total_dates)-set(df['date'].unique()):
        idx+=1
        df.loc[idx, ['date', 'item_nbr', 'store_nbr']]= [pd.to_datetime(d), int(df.iloc[0]['item_nbr']), int(df.iloc[0]['store_nbr'])]
    return df


def get_predictions(validate, train):
    total_dates = train['date'].unique()
    result = pd.DataFrame(columns=['id', 'unit_sales'])
    problem_pairs = []
    example_items = [510052, 1503899, 2081175, 1047674, 215327, 1239746, 765520, 1463867, 1010755, 1473396]
    store47examples = validate.loc[(validate.store_nbr == 47) & (validate.item_nbr.isin(example_items))]
    print("ONLY PREDICTING ITEMS {} IN STORE NO. 47!".format(example_items))
    for name, y in store47examples.groupby(['item_nbr']):
    # for name, y in validate.groupby(['item_nbr', 'store_nbr']):
        item_nbr=int(name)
        store_nbr = 47
        df = train[(train.item_nbr==item_nbr)&(train.store_nbr==store_nbr)]
        CV_SIZE = 16 #if you make it bigger, fill missing dates in cv with 0 if any
        TRAIN_SIZE = 365
        total_dates = train['date'].unique()
        df = fill_missing_date(df, total_dates)
        df = df.sort_values(by=['date'])
        X = df[-TRAIN_SIZE:]
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
    return (result, problem_pairs)


def write_predictions_and_score(validation_score, model, columns_used):
    key = "time_series"
    if not os.path.exists("data/" + key):
        os.makedirs("data/" + key)

    filename = 'data/{}/score_and_metadata.csv'.format(key)
    print("Writing to {}".format(filename))
    score = pd.DataFrame({'estimate': [validation_score], 'columns_used': [columns_used]})
    score.to_csv(filename, index=False)


def main():
    train, validate = load_data()

    print("Not predicting returns...")
    train.loc[train['unit_sales']<0, 'unit_sales']=0
    validate.loc[validate['unit_sales']<0, 'unit_sales']=0

    validation_predictions, problem_pairs_part = get_predictions(validate, train)

    preds_sorted = validation_predictions.sort_values(by=['id'])
    subset_for_validation = validate[validate.id.isin(validation_predictions['id'])].sort_values(by=['id'])

    print("Calculating estimated error")
    validation_score = evaluation.nwrmsle(preds_sorted['unit_sales'].values, subset_for_validation['unit_sales'].values, subset_for_validation['perishable'].values)

    write_predictions_and_score(validation_score, 0, train.columns)

    print("Times series analysis done with a validation score (error rate) of {}.".format(validation_score))


if __name__ == "__main__":
    main()
