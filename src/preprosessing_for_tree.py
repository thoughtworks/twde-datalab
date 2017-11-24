import pandas as pd


def convert_date_column(df):
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")

    df["day"] = df['date'].map(lambda x: x.day)
    df["month"] = df['date'].map(lambda x: x.month)
    df["year"] = df['date'].map(lambda x: x.year)

    print("converted date columns into day, month, and year")
    return df.drop('date', axis=1)


def encode_categorical_columns(df):
    obj_df = df.select_dtypes(include=['object']).copy()
    from sklearn.preprocessing import LabelEncoder
    lb = LabelEncoder()
    for col in obj_df.columns:
        df[col] = lb.fit_transform(obj_df[col])
    print("encoded categorical columns:", obj_df.columns)
    return df


def preprocess_for_trees(df):
    df = convert_date_column(df)
    df = df.fillna(-1)
    print("filled na values with -1")
    df = encode_categorical_columns(df)
    return df
