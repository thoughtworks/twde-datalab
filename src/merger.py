import pandas as pd
import datetime
from io import StringIO
import boto3


def load_data(sample):
    s3bucket = "twde-datalab"
    # Load all tables from raw data
    tables = {}
    tables_to_download = ['stores', 'items', 'transactions', 'cities', 'holidays_events', 'cpi']
    if sample:
        tables_to_download.append('sample_train')
        tables_to_download.append('sample_test')
    else:
        tables_to_download.append('train30days')
        tables_to_download.append('test')

    for t in tables_to_download:
        key = "raw/{table}.csv".format(table=t)
        print("Loading data from {}".format(key))

        s3client = boto3.client('s3')
        csv_string = s3client.get_object(Bucket=s3bucket, Key=key)['Body'].read().decode('utf-8')
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


def join_tables_to_train_data(tables, sample):
    filename = 'bigTable'
    if sample:
        table = 'sample_train'
    else:
        table = 'train30days'

    filename += '30days'
    filename += '.hdf'
    bigTable = add_tables(table, tables)
    return bigTable, filename


def add_days_off(bigTable, tables):
    holidays = tables['holidays_events']
    holidays['date'] = pd.to_datetime(holidays['date'], format="%Y-%m-%d")

    # Isolating events that do not correspond to holidays
    # TODO use events? events=holidays.loc[holidays.type=='Event']
    holidays = holidays.loc[holidays.type != 'Event']

    # Creating a categorical variable showing weekends
    bigTable['dayoff'] = [x in [5, 6] for x in bigTable.dayofweek]

    # TODO ignore transferred holidays

    # Adjusting this variable to show all holidays
    for (d, t, l, n) in zip(holidays.date, holidays.type, holidays.locale, holidays.locale_name):
        if t != 'Work Day':
            if l == 'National':
                bigTable.loc[bigTable.date == d, 'dayoff'] = True
            elif l == 'Regional':
                bigTable.loc[(bigTable.date == d) & (bigTable.state == n), 'dayoff'] = True
            else:
                bigTable.loc[(bigTable.date == d) & (bigTable.city == n), 'dayoff'] = True
        else:
            bigTable.loc[(bigTable.date == d), 'dayoff'] = False
    return bigTable


def join_tables_to_test_data(tables, sample):
    if sample:
        table = 'sample_test'
    else:
        table = 'test'
    bigTable = add_tables(table, tables)
    filename = 'bigTestTable.hdf'
    return bigTable, filename


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

    return bigTable


def add_percentage_transactions(bigTable):
    bigTable['percent_in_transactions'] = bigTable.unit_sales / bigTable.transactions
    return bigTable


def add_transactions_per_capita(bigTable):
    bigTable['transactions_per_capita'] = bigTable.transactions / bigTable.residents
    return bigTable


def write_data_to_s3(table, filename, timestamp, sample=''):
    s3resource = boto3.resource('s3')
    s3client = boto3.client('s3')
    s3bucket = "twde-datalab"

    if not sample:
        print("Putting timestamp as latest file: {}".format(timestamp))
        s3client.put_object(Body=timestamp, Bucket=s3bucket, Key='merger/latest')
    else:
        timestamp = 'sample'
    key = "merger/{timestamp}".format(timestamp=timestamp)
    print("Writing to s3://{}/{}/{}".format(s3bucket, key, filename))

    table.to_hdf(filename, 'key_to_store', mode='w')
    s3resource.Bucket(s3bucket).upload_file(filename, '{key}/{filename}'.format(key=key, filename=filename))


def add_sales_variance(bigTable):
    """ Adds a new column reporting the variance
    in unit_sales for each (item, store) tuple
    """
    df = bigTable.groupby(['store_nbr', 'item_nbr'])['unit_sales'].var().reset_index()
    bigTable2 = bigTable.merge(df.rename(columns={'unit_sales': 'item_store_sales_variance'}), on=['store_nbr', 'item_nbr'])
    return bigTable2


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sample", help="Use sample data? true | false", type=str)
    parser.add_argument("-u", "--upload", help="Upload finished file <true | false>", type=str)

    sample = False
    upload = True
    args = parser.parse_args()
    if args.sample == 'true':
        sample = True
    if args.upload == 'false':
        upload = False

    timestamp = datetime.datetime.now().isoformat()
    print("Started job at {}".format(timestamp))
    tables = load_data(sample)

    print("Joining data to train.csv to make bigTable")
    bigTable, trainFilename = join_tables_to_train_data(tables, sample)

    print("Adding date columns")
    bigTable = add_date_columns(bigTable)

    print("Joining cpi.csv to bigTable")
    bigTable = left_outer_join(bigTable, tables['cpi'], ['year', 'month'])

    print("Adding days off")
    bigTable = add_days_off(bigTable, tables)

    print("Adding item sales per store transaction")
    bigTable = add_percentage_transactions(bigTable)

    print("Adding transactions per capita")
    bigTable = add_transactions_per_capita(bigTable)

    print("Calculating item-store sale variance")
    bigTable = add_sales_variance(bigTable)

    # Make test.csv have the same features as bigTable
    # TODO: Make this less spaghetti code by doing both
    # merges at the same time
    print("Joining data to test.csv to make bigTestTable")
    bigTestTable, testFilename = join_tables_to_test_data(tables, sample)

    print("Adding date columns")
    bigTestTable = add_date_columns(bigTestTable)

    print("Joining cpi.csv to bigTable")
    bigTestTable = left_outer_join(bigTestTable, tables['cpi'], ['year', 'month'])

    print("Adding days off")
    bigTestTable = add_days_off(bigTestTable, tables)

    print("Adding transactions per capita")
    bigTestTable = add_transactions_per_capita(bigTestTable)

    print(bigTable.head())
    if upload:
        write_data_to_s3(bigTable, trainFilename, timestamp, sample)
        write_data_to_s3(bigTestTable, testFilename, timestamp, sample)
    else:
        print("Finished without uploading")
