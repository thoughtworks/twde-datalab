import pandas as pd
import datetime
import sys
import os
import boto3
from sklearn.preprocessing import LabelEncoder
from sklearn.externals import joblib
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))
from sklearn import tree
import evaluation


def load_data(s3resource, s3client, s3bucket):
    # dataset = "sample_data_path"  # for running on a very small sample of data
    dataset = "latest"
    latestContents = s3client.get_object(Bucket='twde-datalab', Key='splitter/{}'.format(dataset))['Body']
    latestSplitter = latestContents.read().decode('utf-8').strip()

    filename = 'train.hdf'
    key = "splitter/{}/{}".format(latestSplitter, filename)
    print("Loading {} data from {}".format(filename, key))
    s3resource.Bucket(s3bucket).download_file(key, filename)
    train = pd.read_hdf(filename)

    filename = 'test.hdf'
    key = "splitter/{}/{}".format(latestSplitter, filename)
    print("Loading {} data from {}".format(filename, key))
    s3resource.Bucket(s3bucket).download_file(key, filename)
    validate = pd.read_hdf(filename)

    print("Loading test data from merger/testBigTable.hdf")
    dataset = "latest"
    latestContents = s3client.get_object(Bucket='twde-datalab', Key='merger/{}'.format(dataset))['Body']
    latestMerger = latestContents.read().decode('utf-8').strip()

    filename = 'bigTestTable.hdf'
    key = "merger/{}/{}".format(latestMerger, filename)
    print("Loading {} data from {}".format(filename, key))
    s3resource.Bucket(s3bucket).download_file(key, filename)
    test = pd.read_hdf(filename)

    return train, validate, test


def join_tables(train, validate, test):
    print("Joining tables for consistent encoding")
    return train.append(validate).append(test).drop('date', axis=1)


def encode_categorical_columns(df):
    obj_df = df.select_dtypes(include=['object', 'bool']).copy().fillna('-1')
    lb = LabelEncoder()
    for col in obj_df.columns:
        df[col] = lb.fit_transform(obj_df[col])
    return df


def encode(train, validate, test):
    print("Encoding categorical variables")
    train_ids = train.id
    validate_ids = validate.id
    test_ids = test.id

    joined = join_tables(train, validate, test)

    encoded = encode_categorical_columns(joined.fillna(-1))

    validate = encoded[encoded['id'].isin(validate_ids)]
    train = encoded[encoded['id'].isin(train_ids)]
    test = encoded[encoded['id'].isin(test_ids)]
    return train, validate, test


def make_model(train):
    print("Creating decision tree model")
    train_dropped = train.drop('unit_sales', axis=1)
    target = train['unit_sales']

    clf = tree.DecisionTreeRegressor()
    clf = clf.fit(train_dropped, target)
    return clf


def overwrite_unseen_prediction_with_zero(preds, train, validate):
    cols_item_store = ['item_nbr', 'store_nbr']
    cols_to_use = validate.columns.drop('unit_sales') if 'unit_sales' in validate.columns else validate.columns
    validate_train_joined = pd.merge(validate[cols_to_use], train, on=cols_item_store, how='left')
    unseen = validate_train_joined[validate_train_joined['unit_sales'].isnull()]
    validate['preds'] = preds
    validate.loc[validate.id.isin(unseen['id_x']), 'preds'] = 0
    preds = validate['preds'].tolist()
    return preds


def make_predictions(clf, validate, train):
    print("Making prediction on validation data")
    validate_dropped = validate.drop('unit_sales', axis=1).fillna(-1)
    validate_preds = clf.predict(validate_dropped)
    validate_preds = overwrite_unseen_prediction_with_zero(validate_preds, train, validate)
    return validate_preds


def write_predictions_and_score_to_s3(s3resource, s3client, s3bucket, test_predictions, validation_score, model, timestamp, test, columns_used):
    key = "decision_tree/{timestamp}".format(timestamp=timestamp.isoformat())

    s3client.put_object(Body=timestamp.isoformat(), Bucket=s3bucket, Key='decision_tree/latest')

    key = "decision_tree/{}".format(timestamp.isoformat())
    filename = 'submission.csv'
    print("Writing to s3://{}/{}/{}".format(s3bucket, key, filename))
    print("predictions length: {}, test length: {}".format(len(test_predictions), test.count()))
    test['unit_sales'] = test_predictions
    # test_predictions = pd.DataFrame({'unit_sales': test_predictions})
    # predictions = test.join(test_predictions)[['id', 'unit_sales']]
    # predictions.loc[predictions['unit_sales'] < 0, 'unit_sales'] = 0
    test['unit_sales'] = test['unit_sales'].round().astype(int)
    test.to_csv(filename)
    s3resource.Bucket(s3bucket).upload_file(filename, '{key}/{filename}'.format(key=key, filename=filename))

    key = "decision_tree/{}".format(timestamp.isoformat())
    filename = 'model.pkl'
    print("Writing to s3://{}/{}/{}".format(s3bucket, key, filename))
    joblib.dump(model, filename)
    model.to_pickle(filename)
    s3resource.Bucket(s3bucket).upload_file(filename, '{key}/{filename}'.format(key=key, filename=filename))

    key = "decision_tree/{}".format(timestamp.isoformat())
    filename = 'score_and_metadata.csv'
    print("Writing to s3://{}/{}/{}".format(s3bucket, key, filename))
    timediff = (datetime.datetime.now() - timestamp).total_seconds() / 60
    score = pd.DataFrame({'runtime_minutes': [timediff], 'estimate': [validation_score], 'columns_used': [columns_used]})
    score.to_csv(filename)
    s3resource.Bucket(s3bucket).upload_file(filename, '{key}/{filename}'.format(key=key, filename=filename))

    print("Done. Time elapsed (minutes): {timediff}".format(timediff=timediff))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sample", help="Use sample data? true | false", type=str)

    sample = False
    args = parser.parse_args()
    if args.sample == 'true':
        sample = True

    s3resource = boto3.resource('s3')
    s3client = boto3.client('s3')

    timestamp = datetime.datetime.now()
    s3bucket = "twde-datalab"

    original_train, original_validate, original_test = load_data(s3resource, s3client, s3bucket)
    train, validate, test = encode(original_train, original_validate, original_test)
    model = make_model(train)
    validation_predictions = make_predictions(model, validate, train)

    print("Calculating estimated error")
    validation_score = evaluation.nwrmsle(validation_predictions, validate['unit_sales'], validate['perishable'])

    test_predictions = make_predictions(model, test, train)

    write_predictions_and_score_to_s3(s3resource, s3client, s3bucket, test_predictions, validation_score, model, timestamp, original_test, original_train.columns)
