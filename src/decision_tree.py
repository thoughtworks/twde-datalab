import pandas as pd
import datetime
import sys
import os
import s3fs
import boto3
from io import StringIO
sys.path.append(os.path.join('..', 'src'))
sys.path.append(os.path.join('src'))
from sklearn import tree
import preprosessing_for_tree
import evaluation


def load_data(s3, s3bucket):

    latestContents = s3.get_object(Bucket='twde-datalab', Key='splitter/latest')['Body']
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


def write_predictions_and_score_to_s3(s3, s3bucket, test_predictions, validation_score, model):
    aws_akid = os.environ['AWS_ID']
    aws_seckey = os.environ['AWS_SECRET']

    timestamp = datetime.datetime.now().isoformat()
    s3path = "s3://{s3bucket}/decision_tree/{timestamp}/".format(s3bucket=s3bucket, timestamp=timestamp)

    fs = s3fs.S3FileSystem(key=aws_akid, secret=aws_seckey)

    s3.put_object(Body=timestamp, Bucket=s3bucket, Key='decision_tree/latest')

    print("Writing test_predictions to {}".format(s3path))
    bytes_to_write = pd.DataFrame({'unit_sales': test_predictions}).to_csv(None, index=False).encode()
    with fs.open(s3path + 'test_predictions.csv', 'wb') as f:
       f.write(bytes_to_write)

    print("Writing validation_score to {}".format(s3path))
    bytes_to_write = pd.DataFrame({'estimate': [validation_score]}).to_csv(None, index=False).encode()
    with fs.open(s3path + 'validation_score.csv', 'wb') as f:
        f.write(bytes_to_write)


if __name__ == "__main__":
    s3bucket = "twde-datalab"
    s3 = boto3.client('s3')

    train, validate, test = load_data(s3, s3bucket)
    train, validate, test = encode(train, validate, test)
    model = make_model(train)
    validation_predictions = make_predictions(model, validate)

    print("Calculating estimated error")
    validation_score = evaluation.nwrmsle(validation_predictions, validate['unit_sales'], validate['perishable'])

    test_predictions = make_predictions(model, test)

    write_predictions_and_score_to_s3(s3, s3bucket, test_predictions, validation_score, model)
