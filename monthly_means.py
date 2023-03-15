import xarray as xr
import numpy as np
import pandas as pd
import datetime
import glob
import os
from multiprocessing.pool import Pool


def monthly_mean_table(nc_path):
    region_name = os.path.basename(os.path.dirname(nc_path)).split('-')[0]
    print(region_name)

    ds = xr.open_dataset(nc_path)

    post1979 = ds['time'] >= pd.to_datetime(datetime.datetime(1980, 1, 1))
    time = ds['time'][post1979].values
    ids = ds['rivid'].values.astype(str)
    q_array = ds['Qout'].sel(time=post1979).values

    ds.close()

    df = pd.DataFrame(q_array, index=time, columns=ids)

    normal_time_period = np.logical_and(
        df.index >= pd.to_datetime(datetime.datetime(1991, 1, 1)),
        df.index < pd.to_datetime(datetime.datetime(2021, 1, 1))
    )

    # calculate monthly averages for all months in period of record
    monthly_means = df.groupby(df.index.strftime("%Y-%m")).mean()
    monthly_means.index.name = 'time'
    # monthly_means.to_csv(f'./tables/{region_name}_monthly_means.csv')

    # define monthly normal based on 1991-2020
    monthly_normal = df[normal_time_period].groupby(df[normal_time_period].index.strftime("%m")).mean()
    monthly_normal.index.name = 'month'
    # monthly_normal.to_csv(f'./tables/{region_name}_monthly_normals_19912020.csv')

    # create new index with month number for division broadcasting by matching month number
    monthly_means = (
        monthly_means
        .reset_index()
        .set_index(pd.Series(monthly_means.index).apply(lambda x: x.split('-')[-1]))
    )

    # divide the monthly means by the monthly normals by matching the month
    monthly_ratios = (
        monthly_means
        .drop('time', axis=1)
        .div(monthly_normal.loc[monthly_means.index].values, axis=1)
    )
    monthly_ratios.index = monthly_means['time']
    monthly_ratios = monthly_ratios.transpose()
    monthly_ratios.to_parquet(f'./tables/{region_name}_monthly_ratios.parquet')

    # drop columns with inf or nan values
    monthly_ratios = monthly_ratios.replace([np.inf, -np.inf], np.nan).dropna(axis=0, how='any')

    monthly_ranks = monthly_ratios.rank(axis=0).astype(int)
    monthly_ranks.to_parquet(f'./tables/{region_name}_monthly_ranks.parquet')
    monthly_percentiles = monthly_ranks.div(monthly_ranks.max(axis=1), axis=0)
    monthly_percentiles.to_parquet(f'./tables/{region_name}_monthly_percentiles.parquet')

    # classify the values depending on their percentile rank
    bins = [0, .13, .28, .72, .87, float('inf')]
    labels = ['Low Flow', 'Below Normal', 'Normal', 'Above Normal', 'High Flow']
    colors = ['#CD233F', '#FFA885', '#E7E2BC', '#8ECEEE', '#2C7DCD']

    # classify all values in the dataframe into one of 5 classes
    monthly_classes = pd.DataFrame({col: pd.cut(monthly_percentiles[col], bins=bins, labels=labels) for col in monthly_percentiles.columns})
    monthly_classes.to_parquet(f'./tables/{region_name}_monthly_classes.parquet')
    monthly_colors = pd.DataFrame({col: pd.cut(monthly_percentiles[col], bins=bins, labels=colors) for col in monthly_percentiles.columns})
    monthly_colors.to_parquet(f'./tables/{region_name}_monthly_colors.parquet')
    return


if __name__ == '__main__':
    table_dir = './tables/'
    ncs = glob.glob('/Users/rchales/Data/geoglows_hindcast/20220430_netcdf/*/*.nc')

    with Pool(6) as p:
        p.map(monthly_mean_table, ncs)

    # ncs = glob.glob('/Users/rchales/Data/geoglows_hindcast/20220430_netcdf/central_america-geoglows/*.nc')
    # monthly_mean_table(ncs[0])

