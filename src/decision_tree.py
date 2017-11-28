import pandas as pd
import datetime
import sys
import os
import s3fs
import boto3
import s3io
import joblib
from io import StringIO
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))
from sklearn import tree
import preprosessing_for_tree
import evaluation


def load_data(s3, s3bucket):
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

    print("Loading test data from raw/test.csv")
    testPath = "raw/test.csv"
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
    aws_akid = os.environ['AWS_ID']
    aws_seckey = os.environ['AWS_SECRET']

    key = "decision_tree/{timestamp}".format(timestamp=timestamp.isoformat())
    s3path = "s3://{s3bucket}/{key}/".format(s3bucket=s3bucket, key=key)

    fs = s3fs.S3FileSystem(key=aws_akid, secret=aws_seckey)

    s3.put_object(Body=timestamp.isoformat(), Bucket=s3bucket, Key='decision_tree/latest')

    print("Writing test_predictions to {}".format(s3path))
    test_predictions = pd.DataFrame({'unit_sales': test_predictions})
    predictions = test.join(test_predictions).drop(['date', 'store_nbr', 'item_nbr', 'onpromotion'], axis=1)
    bytes_to_write = predictions.to_csv(None, index=False).encode()
    with fs.open(s3path + 'test_predictions.csv', 'wb') as f:
       f.write(bytes_to_write)

    print("Writing model as pickle to {}".format(s3path))
    compress = ('gzip', 3)
    credentials = dict(
        aws_access_key_id=aws_akid,
        aws_secret_access_key=aws_seckey,
    )
    with s3io.open('s3://{0}/{1}'.format(s3bucket, key), mode='w', **credentials) as s3_file:
        joblib.dump(model, s3_file, compress=compress)

    print("Writing validation_score and metadata to {}".format(s3path))
    timediff = (datetime.datetime.now() - timestamp).total_seconds() / 60
    bytes_to_write = pd.DataFrame({'runtime_minutes': [timediff], 'estimate': [validation_score], 'columns_used': [columns_used]}).to_csv(None, index=False).encode()
    with fs.open(s3path + 'validation_score.csv', 'wb') as f:
        f.write(bytes_to_write)
    print("Done. Time elapsed (minutes): {timediff}".format(timediff=timediff))

    # To open this pickle:
    # with s3io.open('s3://{0}/{1}'.format(bucket, key), mode='r', **credentials) as s3_file:
    #     obj_reloaded = joblib.load(s3_file)


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
