import os
import pandas as pd
import s3fs


def fetch_data(tables_to_download, data_path):
    s3bucket = "twde-datalab/"
    # Load all tables from raw data

    if not os.path.exists(data_path + 'raw'):
        os.makedirs(data_path + 'raw')

    for t in tables_to_download:
        key = "raw/{table}.csv".format(table=t)

        if not os.path.exists(data_path + key):
            print("Downloading data from {}".format(key))
            s3 = s3fs.S3FileSystem(anon=True)
            try:
                s3.get(s3bucket + key, data_path + key)
            except OSError.FileNotFoundError:
                print("Could not find {0}. Was this generated locally?".
                      format(key))


def load_data(tables_to_load, data_path):
    tables = {}
    for t in tables_to_load:
        key = "raw/{table}.csv".format(table=t)
        print("Loading data to dataframe from {}".format(key))
        tables[t] = pd.read_csv(data_path + key)
    return tables


def left_outer_join(left_table, right_table, on):
    new_table = left_table.merge(right_table, how='left', on=on)
    return new_table


def join_tables_to_train_data(tables, base_table):
    filename = 'bigTable.csv'

    print("Joining {}.csv and items.csv".format(base_table))
    bigTable = left_outer_join(tables[base_table], tables['items'], 'item_nbr')

    print("Joining transactions.csv to bigTable")
    bigTable = left_outer_join(bigTable,
                               tables['transactions'],
                               ['store_nbr', 'date'])

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
    for (d, t, lo, n) in zip(holidays.date, holidays.type,
                             holidays.locale, holidays.locale_name):
        if t != 'Work Day':
            if lo == 'National':
                bigTable.loc[bigTable.date == d, 'dayoff'] = True
            elif lo == 'Regional':
                bigTable.loc[(bigTable.date == d) & (bigTable.state == n),
                             'dayoff'] = True
            else:
                bigTable.loc[(bigTable.date == d) & (bigTable.city == n),
                             'dayoff'] = True
        else:
            bigTable.loc[(bigTable.date == d), 'dayoff'] = False
    return bigTable


def add_date_columns(df):
    print("Converting date columns into year, month, day,"
          " day of week, and days from last datapoint")
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    maxdate = df.date.max()

    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.month
    df['day'] = df.date.dt.day
    df['dayofweek'] = df.date.dt.dayofweek
    df['days_til_end_of_data'] = (maxdate - df.date).dt.days

    return df


def write_data(table, filename):
    if not os.path.exists('data/merger'):
        os.makedirs('data/merger')

    print("Writing to data/merger/{}".format(filename))
    table.to_csv('data/merger/' + filename, index=False)


def add_sales_variance(bigTable):
    """ Adds a new column reporting the variance
    in unit_sales for each (item, store) tuple
    """
    df = bigTable.groupby(['store_nbr', 'item_nbr'])['unit_sales']\
        .var().reset_index()
    return bigTable.merge(df.rename(columns={'unit_sales':
                                             'item_store_sales_variance'
                                             }), on=['store_nbr', 'item_nbr'])


def main(base_table='quito_stores_sample2016-2017', data_path='data/'):
    tables = [base_table, 'items', 'transactions', 'holidays_events', 'cpi']

    fetch_data(tables, data_path)

    allTables = load_data(tables, data_path)

    print("Joining data to train.csv to make bigTable")
    bigTable, trainFilename = join_tables_to_train_data(allTables, base_table)

    print("Adding date columns")
    bigTable = add_date_columns(bigTable)

    print("Joining cpi.csv to bigTable")
    bigTable = left_outer_join(bigTable, allTables['cpi'], ['year', 'month'])

    print("Adding days off")
    bigTable = add_days_off(bigTable, allTables)

    # print("Calculating item-store sale variance")
    # bigTable = add_sales_variance(bigTable)

    print(bigTable.head())

    write_data(bigTable, trainFilename)

    print("Finished merging")


if __name__ == "__main__":
    main()
