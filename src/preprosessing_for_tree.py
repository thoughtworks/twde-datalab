import pandas as pd
from sklearn.preprocessing import LabelEncoder


def convert_date_column(df):
    print("Convering date columns into day, month, and year")
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")

    df["day"] = df['date'].map(lambda x: x.day)
    df["month"] = df['date'].map(lambda x: x.month)
    df["year"] = df['date'].map(lambda x: x.year)

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
