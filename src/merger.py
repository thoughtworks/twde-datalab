import pandas as pd
import datetime
from io import StringIO
import boto3


def load_data(s3, sample):
    s3bucket = "twde-datalab"
    # Load all tables from raw data
    tables = {}
    tables_to_download = ['stores', 'items', 'transactions', 'cities', 'holidays_events']
    if sample:
        tables_to_download.append('sample_train')
        tables_to_download.append('sample_test')
    else:
        tables_to_download.append('last_year_train')
        tables_to_download.append('test')

    for t in tables_to_download:
        key = "raw/{table}.csv".format(table=t)
        print("Loading data from {}".format(key))

        csv_string = s3.get_object(Bucket=s3bucket, Key=key)['Body'].read().decode('utf-8')
        tables[t] = pd.read_csv(StringIO(csv_string))
    return tables


def left_outer_join(left_table, right_table, on):
    new_table = left_table.merge(right_table, how='left', on=on)
    return new_table


def filter_for_latest_year(train):
    train['date'] = pd.to_datetime(train['date'])
    latest_date = train['date'].max()
    year_offset = latest_date - pd.DateOffset(days=365)
    print("Filtering for dates after {}".format(year_offset))
    return train[train['date'] > year_offset]


def join_tables_to_train_data(s3, tables, timestamp, sample, truncate=True):
    filename = 'bigTable'
    if sample:
        table = 'sample_train'
    else:
        table = 'last_year_train'

    # this is not necessary if we download truncated data
    # if truncate:
    #    # Use only the latest year worth of data
    #    tables[table] = filter_for_latest_year(tables[table])
    filename += '2016-2017'
    filename += '.csv'
    bigTable = add_tables(table, tables)
    write_data_to_s3(s3, bigTable, filename, timestamp, sample)

def add_days_off(bigTable, tables):
    holidays = tables['holidays_events']
    holidays['date'] = pd.to_datetime(holidays['date'], format="%Y-%m-%d")

    # Isolating events that do not correspond to holidays
    # TODO use events? events=holidays.loc[holidays.type=='Event']
    holidays=holidays.loc[holidays.type!='Event']

    # Creating a categorical variable showing weekends
    bigTable['dayoff']=[x in [6,7] for x in bigTable.dayofweek]

    #TODO ignore transferred holidays

    # Adjusting this variable to show all holidays
    for (d,t,l,n) in zip(holidays.date,holidays.type,holidays.locale,holidays.locale_name):
        if t!='Work Day':
            if l=='National':
                bigTable.loc[bigTable.date==d,'dayoff']=True
            elif l=='Regional':
                bigTable.loc[(bigTable.date==d)&(bigTable.state==n),'dayoff']=True
            else:
                bigTable.loc[(bigTable.date==d)&(bigTable.city==n),'dayoff']=True
        else:
            bigTable.loc[(bigTable.date==d),'dayoff']=False
    return bigTable

def join_tables_to_test_data(s3, tables, timestamp, sample):
    if sample:
        table = 'sample_test'
    else:
        table = 'test'
    bigTable = add_tables(table, tables)
    filename = 'bigTestTable.csv'
    write_data_to_s3(s3, bigTable, filename, timestamp, sample)


def add_date_columns(df):
    print("Converting date columns into year, month, day, day of week, and days from last datapoint")
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    maxdate = df.date.max()

    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.month
    df['day'] = df.date.dt.day
    df['dayofweek'] = df.date.dt.dayofweek
    df['days_til_end_of_data'] = (maxdate - df.date).dt.days

    return df


def add_tables(base_table, tables):
    print("Joining {}.csv and items.csv".format(base_table))
    bigTable = left_outer_join(tables[base_table], tables['items'], 'item_nbr')

    print("Joining stores.csv to bigTable")
    bigTable = left_outer_join(bigTable, tables['stores'], 'store_nbr')

    print("Joining transactions.csv to bigTable")
    bigTable = left_outer_join(bigTable, tables['transactions'], ['store_nbr', 'date'])

    print("Joining cities.csv to bigTable")
    bigTable = left_outer_join(bigTable, tables['cities'], 'city')

    bigTable = add_date_columns(bigTable)

    bigTable = add_days_off(bigTable, tables)
    #TODO drop date? x.drop('date', axis=1)
    return bigTable


def write_data_to_s3(s3, table, filename, timestamp, sample=False):
    s3bucket = "twde-datalab"
    if not sample:
        s3.put_object(Body=timestamp, Bucket=s3bucket, Key='merger/latest')

    key = "merger/{timestamp}".format(timestamp=timestamp)
    print("Writing to s3://{}/{}/{}".format(s3bucket, key, filename))

    csv_buffer = StringIO()
    table.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=s3bucket, Key='{key}/{filename}'.format(key=key, filename=filename), Body=csv_buffer.getvalue())


if __name__ == "__main__":
    s3 = boto3.client('s3')
    timestamp = datetime.datetime.now().isoformat()
    sample = False
    tables = load_data(s3, sample)

    print("Joining data to train.csv to make bigTable")
    join_tables_to_train_data(s3, tables, timestamp, sample)

    print("Joining data to test.csv to make bigTable")
    join_tables_to_test_data(s3, tables, timestamp, sample)
