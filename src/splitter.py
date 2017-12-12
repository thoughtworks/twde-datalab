import pandas as pd
import numpy as np
import os


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

def write_data(table, filename):
    if not os.path.exists('./splitter'):
        os.makedirs('./splitter')

    print("Writing to ./splitter/{}".format(filename))
    table.to_csv('splitter/' + filename, index=False)

if __name__ == "__main__":
    print("Loading data from merger output")
    train = pd.read_csv("./merger/bigTable.csv")

    train['date'] = pd.to_datetime(train['date'], format="%Y-%m-%d")

    latest_date = train['date'].max()

    begin_of_validation, end_of_validation = get_validation_period(latest_date)

    print("Splitting data between {} and {}".format(begin_of_validation, end_of_validation))
    train_train, train_validation = split_validation_train_by_validation_period(train, begin_of_validation,
                                                                                end_of_validation)
    write_data(train_train, 'train.csv')

    write_data(train_validation, 'validation.csv')

    print("Finished splitting")
