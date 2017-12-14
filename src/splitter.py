import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split


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


def main():
    print("Loading data from merger output")
    train = pd.read_csv("./merger/bigTable.csv")

    train['date'] = pd.to_datetime(train['date'], format="%Y-%m-%d")

    print("Splitting data 70:30 train:validation")

    train_train, train_validation = train_test_split(train, test_size=0.3)

    write_data(train_train, 'train.csv')

    write_data(train_validation, 'validation.csv')

    print("Finished splitting")


if __name__ == "__main__":
    main()
