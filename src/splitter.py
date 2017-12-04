import pandas as pd
import boto3
import datetime
import numpy as np


def get_validation_period(latest_date_train):
    # we want from wednesday to thursday (encoded as int 3) for a 15 day period
    offset = (latest_date_train.weekday() - 3) % 7
    end_of_validation_period = latest_date_train - pd.DateOffset(days=offset)
    begin_of_validation_period = end_of_validation_period - pd.DateOffset(days=15)
    return (begin_of_validation_period, end_of_validation_period)


def split_validation_train_by_validation_period(train, validation_begin_date, validation_end_date):
    train_validation = train[(train['date'] >= validation_begin_date) & (train['date'] <= validation_end_date)]
    train_train = train[train['date'] < validation_begin_date]
    return train_train, train_validation


def move_items_from_train_to_validation(train, validation, items_to_remove):
    train2 = train[~train.item_nbr.isin(items_to_remove)]
    validation_to_add = train[train.item_nbr.isin(items_to_remove)]
    validation2 = validation.append(validation_to_add)
    return train2, validation2


def move_random_items_from_train_to_validation(train, validation, num_items_to_remove):
    train_items = train['item_nbr'].unique()
    items_to_remove = np.random.choice(train_items, num_items_to_remove)
    train2, validation2 = move_items_from_train_to_validation(train, validation, items_to_remove)
    print("Moved {} items from train data to test data".format(num_items_to_remove))
    print("train data: {} -> {} rows".format(train.shape[0], train2.shape[0]))
    print("validation data: {} -> {} rows".format(validation.shape[0], validation2.shape[0]))
    return train2, validation2


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

    s3bucket = "twde-datalab"

    dataset = "latest"
    if sample:
        dataset = "sample_data_path"

    latestContents = s3client.get_object(Bucket=s3bucket, Key='merger/{}'.format(dataset))['Body']
    latest = latestContents.read().decode('utf-8').strip()
    print("Latest folder: {}".format(latest))
    print("Dataset value: {}".format(dataset))

    s3bigTablePath = "merger/{latest}/bigTable2016-2017.hdf".format(latest=latest)
    filename = 'bigTable2016-2017.hdf'

    print("Downloading latest bigTable from {}".format(s3bigTablePath))
    s3resource.Bucket(s3bucket).download_file(s3bigTablePath, filename)
    train = pd.read_hdf(filename)

    train['date'] = pd.to_datetime(train['date'], format="%Y-%m-%d")

    latest_date = train['date'].max()

    begin_of_validation, end_of_validation = get_validation_period(latest_date)

    print("Splitting data between {} and {}".format(begin_of_validation, end_of_validation))
    train_train, train_validation = split_validation_train_by_validation_period(train, begin_of_validation,
                                                                                end_of_validation)

    timestamp = datetime.datetime.now().isoformat()
    if not sample:
        s3client.put_object(Body=timestamp, Bucket=s3bucket, Key='splitter/latest')

    key = "splitter/{}".format(timestamp)
    filename = 'train.hdf'

    print("Writing test to s3://{}/{}/{}".format(s3bucket, key, filename))
    train_train.to_hdf(filename, 'key_to_store', mode='w')
    s3resource.Bucket(s3bucket).upload_file(filename, '{key}/{filename}'.format(key=key, filename=filename))

    filename = 'test.hdf'
    print("Writing test to s3://{}/{}/{}".format(s3bucket, key, filename))

    train_validation.to_hdf(filename, 'key_to_store', mode='w')
    s3resource.Bucket(s3bucket).upload_file(filename, '{key}/{filename}'.format(key=key, filename=filename))

    print("Done")
