import pandas as pd
import datetime
import sys
import os
import io
import boto3
import joblib
from io import StringIO
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))
from sklearn import tree
import preprosessing_for_tree
import evaluation


def load_data(s3, s3bucket):
    # dataset = "sample_data_path"  # for running on a very small sample of data
    dataset = "latest"
    latestContents = s3.get_object(Bucket='twde-datalab', Key='splitter/{}'.format(dataset))['Body']
    latest = latestContents.read().decode('utf-8').strip()

    trainPath = "splitter/{}/train.csv".format(latest)
    print("Loading train data from {}".format(trainPath))
    trainObj = s3.get_object(Bucket=s3bucket, Key=trainPath)
    trainBody = trainObj['Body']
    train_csv_string = trainBody.read().decode('utf-8')
    train = pd.read_csv(StringIO(train_csv_string))

    validatePath = "splitter/{}/test.csv".format(latest)
    print("Loading validate data from {}".format(validatePath))
    validateObj = s3.get_object(Bucket=s3bucket, Key=validatePath)
    validateBody = validateObj['Body']
    validate_csv_string = validateBody.read().decode('utf-8')
    validate = pd.read_csv(StringIO(validate_csv_string))

    dataset = "latest"
    latestContents = s3.get_object(Bucket='twde-datalab', Key='merger/{}'.format(dataset))['Body']
    latest = latestContents.read().decode('utf-8').strip()

    print("Loading test data from raw/test.csv")
    testPath = "merger/{}/bigTestTable.csv".format(latest)
    testObj = s3.get_object(Bucket=s3bucket, Key=testPath)
    testBody = testObj['Body']
    test_csv_string = testBody.read().decode('utf-8')
    test = pd.read_csv(StringIO(test_csv_string))

    return train, validate, test


def join_tables(train, validate, test):
    print("Joining tables for consistent encoding")
    return train.append(validate).append(test)


def encode(train, validate, test):
    print("Encoding categorical variables")
    train_ids = train.id
    validate_ids = validate.id
    test_ids = test.id

    joined = join_tables(train, validate, test)

    encoded = preprosessing_for_tree.preprocess_for_trees(joined).fillna(-1)

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


def make_predictions(clf, validate):
    print("Making prediction on validation data")
    validate_dropped = validate.drop('unit_sales', axis=1).fillna(-1)
    validate_preds = clf.predict(validate_dropped)
    return validate_preds


def write_predictions_and_score_to_s3(s3, s3bucket, test_predictions, validation_score, model, timestamp, test, columns_used):
    key = "decision_tree/{timestamp}".format(timestamp=timestamp.isoformat())

    s3.put_object(Body=timestamp.isoformat(), Bucket=s3bucket, Key='decision_tree/latest')

    filename = 'submission.csv'
    print("Writing to s3://{}/{}/{}".format(s3bucket, key, filename))
    test_predictions = pd.DataFrame({'unit_sales': test_predictions})
    predictions = test.join(test_predictions)[['id', 'unit_sales']]
    predictions.loc[predictions['unit_sales'] < 0, 'unit_sales'] = 0
    predictions['unit_sales'] = predictions['unit_sales'].round().astype(int)

    csv_buffer = io.StringIO()
    predictions.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=s3bucket, Key='{key}/{filename}'.format(key=key, filename=filename), Body=csv_buffer.getvalue())

    filename = 'model.pkl'
    print("Writing model as pickle to s3://{}/{}/{}".format(s3bucket, key, filename))
    joblib.dump(model, './model.pkl')
    s3.put_object(Bucket=s3bucket, Key='{key}/'.format(key=key), Body='./model.pkl')

    filename = 'score_and_metadata.csv'
    print("Writing to s3://{}/{}/{}".format(s3bucket, key, filename))
    timediff = (datetime.datetime.now() - timestamp).total_seconds() / 60
    score = pd.DataFrame({'runtime_minutes': [timediff], 'estimate': [validation_score], 'columns_used': [columns_used]})

    csv_buffer = io.StringIO()
    score.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=s3bucket, Key='{key}/{filename}'.format(key=key, filename=filename), Body=csv_buffer.getvalue())

    print("Done. Time elapsed (minutes): {timediff}".format(timediff=timediff))


if __name__ == "__main__":

    timestamp = datetime.datetime.now()
    s3bucket = "twde-datalab"
    s3 = boto3.client('s3')

    original_train, original_validate, original_test = load_data(s3, s3bucket)
    train, validate, test = encode(original_train, original_validate, original_test)
    model = make_model(train)
    validation_predictions = make_predictions(model, validate)

    print("Calculating estimated error")
    validation_score = evaluation.nwrmsle(validation_predictions, validate['unit_sales'], validate['perishable'])

    test_predictions = make_predictions(model, test)

    write_predictions_and_score_to_s3(s3, s3bucket, test_predictions, validation_score, model, timestamp, original_test, original_train.columns)
