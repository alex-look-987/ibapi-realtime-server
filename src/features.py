import asyncio
import numpy as np
import pandas as pd
from miscs.columns import dates, funcs_delta, window_delta, feature_delta
from technical_indicators.time_series_features import computation, date_feature, session

def features(df: pd.DataFrame): 

    df['date'] = df.index
    df['index'] = np.arange(len(df))

    df = date_feature(df, dates[:-1])
    df = session(df)

    df = computation(df, funcs_delta, window_delta, feature_delta, False)

    df.dropna(inplace=True)

    return df

async def features_async(df):
    return await asyncio.to_thread(features, df)
