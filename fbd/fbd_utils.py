#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Fri Dec 11 16:40:54 2020.

@author: williammartin
"""

# import standard libraries
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from glob import glob
import numpy as np
import os
from urllib.request import urlopen
# import third-party libraries
import dask
from scipy.cluster.hierarchy import dendrogram
from sklearn.linear_model import LinearRegression
#import dask.dataframe as dd
import pandas as pd
# import local libraries


def update_constituents(const_mat, db, date, n=3000):

    # create empty dataframe
    data = pd.DataFrame()

    # get data of last day of moment, try again if we are a weekend or other
    while data.empty:
        data = \
            db.raw_sql(
                "select permno, shrout, prc from crsp.dsf where "
                f"date >= '{date}' and date <= '{date}'")

        # go back one day
        date = pd.to_datetime(date) - timedelta(days=1)
        date = date.strftime('%Y-%m-%d')

    data['mcap'] = data['shrout'].abs()*data['prc'].abs()
    data.drop(columns=['shrout', 'prc'], inplace=True)

    # retain 3000 biggest mcap
    data = data.nlargest(n, 'mcap')

    # turn permno to int
    data['permno'] = data['permno'].astype(int)

    # add ones to const mat
    const_mat.at[date[:-3], data['permno'].values] = 1

    return const_mat


def download_returns(db, start, end, permnos):

    # get stock returns
    data = \
        db.raw_sql(
            f"select permno, date, ret from crsp.dsf where permno in "
            f"({', '.join(permnos)}) and date >= '{start}' and date "
            f"<= '{end}'")

    data = data.set_index('date')

    # clean into dataframe
    X = data.pivot(columns='permno', values='ret')
    X.columns = X.columns.astype(int)
    # add missing columns
    miss_cols = list(set(permnos.astype(int)) - set(X.columns))
    X[miss_cols] = np.nan

    return X


def download_info(db, permnos):

    # get information of stocks
    data = \
        db.raw_sql(
            f"select permno, comnam, naics, siccd, ticker "
            f"from crsp.dse "
            f"where permno in ({', '.join(permnos)})"
            )

    data.dropna(axis=0, subset=['comnam', 'ticker'],
                how='all', inplace=True)
    data.drop_duplicates(inplace=True)
    data.reset_index(inplace=True, drop=True)

    return data


def download_fama(start, end):

    filename = 'data/fama/temp.csv.zip'

    def get_url_content(url):
        url = urlopen(url)
        output = open(filename, 'wb')
        output.write(url.read())
        output.close()

    def filter_dates(df):
        # turn index to date
        df.index = pd.to_datetime(df.index, format='%Y%m%d')
        # get between start and end
        df = df[(df.index >= start) & (df.index <= end)]
        return df

    # get fama french 5 factors
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"
    get_url_content(url)

    fama = pd.read_csv(filename, compression='zip', index_col=0, header=2)
    os.remove(filename)
    fama = filter_dates(fama)

    # get momentum
    url = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip'
    get_url_content(url)

    mom = pd.read_csv(filename, compression='zip', index_col=0,
                      skiprows=12, skipfooter=1, engine='python')
    os.remove(filename)
    mom = filter_dates(mom)
    mom = mom.rename(columns = {'Mom   ': 'Mom'})

    # get st and lt rev
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_ST_Reversal_Factor_daily_CSV.zip"
    get_url_content(url)

    st_rev = pd.read_csv(filename, compression='zip', index_col=0,
                         skiprows=13, skipfooter=1, engine='python')
    os.remove(filename)
    st_rev = filter_dates(st_rev)

    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_LT_Reversal_Factor_daily_CSV.zip"
    get_url_content(url)

    lt_rev = pd.read_csv(filename, compression='zip', index_col=0,
                         skiprows=13, skipfooter=1, engine='python')
    os.remove(filename)
    lt_rev = filter_dates(lt_rev)

    return fama, mom, st_rev, lt_rev


def read_parquet(filename, permnos=None):
    """Read parquet and get read columns corresponding to permnos.

    Parameters
    ----------
    filename : str
        Filename of parquet to get data.
    permnos : list of str, optional
        List of permnos to parse. The default is None.

    Returns
    -------
    res : pd.DataFrame
        Returns of permnos.

    """
    res = pd.read_parquet(filename, engine='pyarrow', columns=permnos)
    return res


def get_permno_returns(permno, const_mat, start=None, end=None):

    const_p = const_mat[permno]
    dates = const_p[const_p == 1].index.values

    if start is not None and end is not None:
        dates_dt = pd.to_datetime(dates)
        dates_dt = dates_dt[(dates_dt >= start) & (dates_dt <= end)]
        dates = list(dates_dt.strftime('%Y-%m'))

    filenames = [
            glob('data/returns/raw/{}.parquet'.format(d))[0] for d in dates
            ]

    rp = dask.delayed(read_parquet)
    promises = [rp(f, [permno]) for f in filenames]
    promises = dask.delayed(pd.concat)(promises)

    return promises


def get_monthly_returns(end, const_mat, months=2, delay=False):

    end_dt = pd.to_datetime(end)
    start_dt = end_dt - relativedelta(months=months-1)
    dates_dt = pd.date_range(start=start_dt, end=end_dt, freq='MS')
    dates = list(dates_dt.strftime('%Y-%m'))
    permnos = const_mat.loc[end]
    permnos = list(permnos[permnos == 1].index.values)

    filenames = [
            glob('data/returns/raw/{}.parquet'.format(d))[0] for d in dates
            ]

    # non dask is faster if taking only 1 or 2 months at a time
    if not delay:
        return pd.concat([read_parquet(f, permnos) for f in filenames])
    else:
        rp = dask.delayed(read_parquet)
        promises = [rp(f, permnos) for f in filenames]
        return dask.delayed(pd.concat)(promises)


def count_na(df, index):

    num_na = df.isna().sum().to_frame().T
    num_na.index = [index]
    return num_na


def ols_fast(X, y, permno):
    # try other methods
    #lr = LinearRegression().fit(X, y)
    #beta = np.linalg.inv(X.T@X)@X.T@y
    try:
        # more robust ! 
        lr = LinearRegression().fit(X, y)
        m, c = np.array(lr.coef_), np.array([lr.intercept_])
        #m, c, _, _ = np.linalg.lstsq(X, y, rcond=None) # not robust ! 
    except:
        m, c = np.array([np.nan]*X.shape[1]), np.array([np.nan]*1)

    # create dataframe with loadinds
    df = pd.DataFrame(columns=[permno], data=np.concatenate([c, m]))

    return df


# source: https://scikit-learn.org/stable/auto_examples/cluster/plot_agglomerative_dendrogram.html
def plot_dendrogram(model, **kwargs):
    # Create linkage matrix and then plot the dendrogram

    # create the counts of samples under each node
    counts = np.zeros(model.children_.shape[0])
    n_samples = len(model.labels_)
    for i, merge in enumerate(model.children_):
        current_count = 0
        for child_idx in merge:
            if child_idx < n_samples:
                current_count += 1  # leaf node
            else:
                current_count += counts[child_idx - n_samples]
        counts[i] = current_count

    linkage_matrix = np.column_stack([model.children_, model.distances_,
                                      counts]).astype(float)

    # Plot the corresponding dendrogram
    dendrogram(linkage_matrix, **kwargs)


if __name__ == '__main__':
    pass
