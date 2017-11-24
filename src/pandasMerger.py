import pandas as pd


def leftOuterJoin(left_table, right_table, on):
    new_table = left_table.merge(right_table, how='left', on=on)
    return new_table


def filter_for_latest_year(train):
    train['date'] = pd.to_datetime(train['date'])
    latest_date = train['date'].max()
    year_offset = latest_date - pd.DateOffset(days=365)
    print("Filtering for dates after {}".format(year_offset))
    return train[train['date'] > year_offset]


if __name__ == "__main__":
    # Load all tables from raw data
    tables = {'stores': None, 'items': None, 'train': None, 'transactions': None, 'cities': None}
    for t in tables.keys():
        print("Reading table: {}".format(t))
        tables[t] = pd.read_csv("../data/{table}.csv".format(table=t))

    # Use only the latest year worth of data
    tables['train'] = filter_for_latest_year(tables['train'])

    print("Joining train.csv and items.csv")
    bigTable = leftOuterJoin(tables['train'], tables['items'], 'item_nbr')

    print("Joining stores.csv to bigTable")
    bigTable = leftOuterJoin(bigTable, tables['stores'], 'store_nbr')

    print("Joining transactions.csv to bigTable")
    bigTable = leftOuterJoin(bigTable, tables['transactions'], ['store_nbr', 'date'])

    print("Joining cities.csv to bigTable")
    bigTable = leftOuterJoin(bigTable, tables['cities'], 'city')

    print("Writing bigTable to file")
    bigTable.to_csv('../data/v7/bigTable2016-2017.csv', index=False)
