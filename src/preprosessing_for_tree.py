import pandas as pd
from sklearn.preprocessing import LabelEncoder


def convert_date_column(df):
    print("Converting date columns into year, month, day, day of week, and days from last datapoint")
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    maxdate = df.date.max()

    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.month
    df['day'] = df.date.dt.day
    df['dayofweek'] = df.date.dt.dayofweek
    df['days_til_end_of_data'] = (maxdate - df.date).dt.days

    return df.drop('date', axis=1)


def encode_categorical_columns(df):
    obj_df = df.select_dtypes(include=['object', 'bool']).copy().fillna('-1')
    lb = LabelEncoder()
    for col in obj_df.columns:
        df[col] = lb.fit_transform(obj_df[col])
    return df


def preprocess_for_trees(df):
    df = convert_date_column(df)
    df = encode_categorical_columns(df)
    return df
