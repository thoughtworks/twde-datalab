import pandas as pd
import boto3
import os
import s3fs
import datetime
from io import StringIO


def get_validation_period(latest_date_train):
    # we want from wednesday to thursday (encoded as int 3) for a 15 day period
    offset = (latest_date_train.weekday() - 3) % 7
    end_of_validation_period = latest_date_train - pd.DateOffset(days=offset)
    begin_of_validation_period = end_of_validation_period - pd.DateOffset(days=15)
    return (begin_of_validation_period, end_of_validation_period)


def split_validation_train_by_validation_period(train, validation_begin_date, validation_end_date):
    train_validation= train[(train['date'] >= validation_begin_date) & (train['date'] <= validation_end_date) ]
    train_train = train[train['date']< validation_begin_date]
    return train_train, train_validation


def move_items_from_train_to_validation(train, validation, items_to_remove):
    train2 = train[~train.item_nbr.isin(items_to_remove)]
    validation_to_add = training[training.item_nbr.isin(items_to_remove)]
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
    aws_akid = os.environ['AWS_ID']
    aws_seckey = os.environ['AWS_SECRET']

    print("this will only print if running python load_and_split.py")
    s3bucket = "twde-datalab"
    # TODO change input path to 'merger/{find latest output date}/bigTable.csv'
    s3bigTablePath = "data/v7/bigTable2016-2017.csv"

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3bucket, Key=s3bigTablePath)
    body = obj['Body']
    csv_string = body.read().decode('utf-8')
    train = pd.read_csv(StringIO(csv_string))

    print("read to pandas dataframe")

    train['date'] = pd.to_datetime(train['date'], format="%Y-%m-%d")

    print(train.shape)

    latest_date = train['date'].max()

    begin_of_validation, end_of_validation = get_validation_period(latest_date)

    print(begin_of_validation)
    print(end_of_validation)

    train_train, train_validation = split_validation_train_by_validation_period(train, begin_of_validation, end_of_validation)

    s3ValidationDataPath = "s3://twde-datalab/splitter/" + datetime.datetime.now().isoformat() + "/"

    fs = s3fs.S3FileSystem(key=aws_akid, secret=aws_seckey)

    print("writing train...")

    bytes_to_write = train_train.to_csv(None).encode()
    with fs.open(s3ValidationDataPath + 'train.csv', 'wb') as f:
        f.write(bytes_to_write)

    print("writing test...")

    bytes_to_write = train_validation.to_csv(None).encode()
    with fs.open(s3ValidationDataPath + 'test.csv', 'wb') as f:
        f.write(bytes_to_write)
