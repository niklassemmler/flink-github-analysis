from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os


PLOT_EXTENSION = 'svg'

def savefig(f: plt.Figure, fname: str):
    f.tight_layout()
    if not os.path.splitext(fname)[1]:
        fname = f"{fname}.{PLOT_EXTENSION}"
    f.savefig('plots/' + fname, facecolor='w', dpi=400)

def writefile(df: pd.DataFrame, fname: str):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    df.to_html(f"output/{fname}.html")

def initialize_datetime(data: pd.DataFrame):
    for col in data.columns:
        if col.endswith('Date') or col.endswith('At'):
            data[col] = pd.to_datetime(data[col])

def add_first_review_column(dataset: pd.DataFrame):
    def min_review(row):
        ts = np.nan
        ts2 = np.nan
        if isinstance(row['firstReviewThreadCreatedAt'], pd.Timestamp): 
            ts = row['firstReviewThreadCreatedAt']
        if isinstance(row['firstReviewCreatedAt'], pd.Timestamp):
            ts2 = row['firstReviewCreatedAt']
        if ts is np.nan:
            return ts2
        if ts2 is np.nan:
            return ts
        return min(ts, ts2)

    assert 'firstReviewCreatedAt' in dataset.columns
    assert 'firstReviewThreadCreatedAt' in dataset.columns
    return dataset.apply(min_review, axis=1)

def add_time_to_first_review_column(dataset: pd.DataFrame):
    def time_to_first_review(row):
        if row['createdAt'] is np.nan or row['createdAt'] is None:
            return np.nan
        if row['firstReview'] is np.nan or row['firstReview'] is None:
            return np.nan
        return pd.to_datetime(row['firstReview']) - pd.to_datetime(row['createdAt'])

    assert 'createdAt' in dataset.columns
    assert 'firstReview' in dataset.columns
    return dataset.apply(time_to_first_review, axis=1)

def add_lifetime_column(dataset: pd.DataFrame):
    def life_time(row):
        if row['createdAt'] is np.nan or row['createdAt'] is None:
            return np.nan
        if row['closedAt'] is np.nan or row['closedAt'] is None:
            return np.nan
        return pd.to_datetime(row['closedAt']) - pd.to_datetime(row['createdAt'])

    assert 'createdAt' in dataset.columns
    assert 'closedAt' in dataset.columns
    return dataset.apply(life_time, axis=1)

def create_label_dataset(data: pd.DataFrame) -> pd.DataFrame:
    data = data.explode('labels')
    data = data.loc[~data['labels'].isna()]
    data = data.rename(columns={'labels': 'name'})
    return data
