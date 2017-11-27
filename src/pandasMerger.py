import pandas as pd
import datetime
import os


def left_outer_join(left_table, right_table, on):
    new_table = left_table.merge(right_table, how='left', on=on)
    return new_table


def filter_for_latest_year(train):
    train['date'] = pd.to_datetime(train['date'])
    latest_date = train['date'].max()
    year_offset = latest_date - pd.DateOffset(days=365)
    print("Filtering for dates after {}".format(year_offset))
    return train[train['date'] > year_offset]


def join_tables_to_train_data():
    tables['train'] = pd.read_csv("../data/train.csv")

    # Use only the latest year worth of data
    tables['train'] = filter_for_latest_year(tables['train'])

    add_tables('train')


def join_tables_to_test_data():
    tables['test'] = pd.read_csv("../data/test.csv")
    add_tables('test')


def add_tables(base_table):
    print("Joining {}.csv and items.csv".format(base_table))
    bigTable = left_outer_join(tables[base_table], tables['items'], 'item_nbr')

    print("Joining stores.csv to bigTable")
    bigTable = left_outer_join(bigTable, tables['stores'], 'store_nbr')

    print("Joining transactions.csv to bigTable")
    bigTable = left_outer_join(bigTable, tables['transactions'], ['store_nbr', 'date'])

    print("Joining cities.csv to bigTable")
    bigTable = left_outer_join(bigTable, tables['cities'], 'city')

    print("Writing bigTable to file")
    path = '../data/merger/{base_table}/{timestamp}/'.format(base_table=base_table, timestamp=datetime.datetime.now().isoformat())
    if not os.path.exists(path):
        os.makedirs(path)
    bigTable.to_csv('{path}bigTable2016-2017.csv'.format(path=path), index=False)


if __name__ == "__main__":
    # Load all tables from raw data
    tables = {'stores': None, 'items': None, 'transactions': None, 'cities': None}
    for t in tables.keys():
        print("Reading table: {}".format(t))
        tables[t] = pd.read_csv("../data/{table}.csv".format(table=t))

    # join_tables_to_train_data()
    join_tables_to_test_data()
