#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Created on Fri Dec 11 16:40:54 2020.

@author: williammartin
"""

# import standard libraries
from datetime import timedelta
from glob import glob
import os
from urllib.request import urlopen
# import third-party libraries
import dask
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
    X = pd.DataFrame()
    for per, df in data.groupby('permno'):
        df.drop(columns='permno', inplace=True)
        df.columns = [int(per)]
        X = pd.concat([X, df], axis=1, ignore_index=False)

    return X


def download_info(db, permnos):

    # get information of stocks
    data = \
        db.raw_sql(
            f"select permno, comnam, naics, ticker from crsp.dse where permno in "
            f"({', '.join(permnos)})")

    data.dropna(axis=0, subset=['comnam', 'ticker'], how='all', inplace=True)
    data.drop_duplicates(inplace=True)
    data.reset_index(inplace=True, drop=True)

    return data


def download_fama(start, end):

    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"

    # read zip file
    filename = 'data/fama/fama_french.csv.zip'
    url = urlopen(url)
    output = open(filename, 'wb')
    output.write(url.read())
    output.close()

    fama = pd.read_csv(filename, compression='zip', index_col=0,
                       header=2)
    os.remove(filename)

    # turn index to date
    fama.index = pd.to_datetime(fama.index, format='%Y%m%d')

    # get between start and end
    fama = fama[(fama.index >= start) & (fama.index <= end)]

    # save localy
    fama.to_csv('data/fama/fama.csv.gz', compression='gzip')


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

    promises = []
    for f in filenames:
        res = dask.delayed(read_parquet)(f, permno)
        promises.append(res)

    return dask.compute(promises)


if __name__ == '__main__':
    pass
