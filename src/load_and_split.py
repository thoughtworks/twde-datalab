import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

#we want from wednesday to thursday (3) for 2 weeks
def get_validation_period(latest_date_train):
    offset = (latest_date_train.weekday()-3) % 7
    end_of_validation_period = latest_date_train - pd.DateOffset(days=offset)
    begin_of_validation_period = end_of_validation_period - pd.DateOffset(days=15)
    return (begin_of_validation_period, end_of_validation_period)

def split_validation_train_by_validation_period(train, validation_begin_date, validation_end_date):
    train_validation= train[(train['date']>= validation_begin_date) & (train['date']<= validation_end_date) ]
    train_train = train[train['date']< validation_begin_date]
    return train_train, train_validation

if __name__ == "__main__":
    print("this will only print if running python load_and_split.py")
    s3bigTablePath = "s3://twde-datalab/data/v3/bigTable/part-00000-f10ddaab-2fac-421f-be84-e649592384f7-c000.snappy.parquet"

    # TODO load entire folder
    train = pd.read_parquet(s3bigTablePath, engine='pyarrow')

    train['date'] = pd.to_datetime(train['date'], format="%Y-%m-%d")

    print(train.shape())

    latest_date = train['date'].max()

    begin_of_validation, end_of_validation = get_validation_period(latest_date)

    print(begin_of_validation)
    print(end_of_validation)

    train_train, train_validation = split_validation_train_by_validation_period(train, begin_of_validation, end_of_validation)

    s3trainDataPath = "s3://twde-datalab/data/v3/val/train/"
    s3testDataPath = "s3://twde-datalab/data/v3/val/test/"

    train_table = pa.Table.from_pandas(train_train)

    print("writing train...")

    pq.write_table(train_table, s3trainDataPath + 'train.parquet')



