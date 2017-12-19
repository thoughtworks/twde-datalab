import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split


def write_data(table, filename):
    if not os.path.exists('data/splitter'):
        os.makedirs('data/splitter')

    print("Writing to data/splitter/{}".format(filename))
    table.to_csv('data/splitter/' + filename, index=False)


def main():
    print("Loading data from merger output")
    train = pd.read_csv("data/merger/bigTable.csv")

    train['date'] = pd.to_datetime(train['date'], format="%Y-%m-%d")

    print("Splitting data 70:30 train:validation")

    train_train, train_validation = train_test_split(train, test_size=0.3)

    write_data(train_train, 'train.csv')

    write_data(train_validation, 'validation.csv')

    print("Finished splitting")


if __name__ == "__main__":
    main()
