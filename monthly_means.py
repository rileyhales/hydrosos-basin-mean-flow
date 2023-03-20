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
    mon_means = df.groupby(df.index.strftime("%Y-%m")).mean()
    mon_means.index.name = 'time'

    # define monthly normal based on 1991-2020
    mon_normals = df[normal_time_period].groupby(df[normal_time_period].index.strftime("%m")).mean()
    mon_normals.index.name = 'month'
    mon_normals.transpose().to_parquet(f'./tables/{region_name}_monthly_normals_19912020.parquet')

    # create new index with month number for division broadcasting by matching month number
    mon_means = (
        mon_means
        .reset_index()
        .set_index(pd.Series(mon_means.index).apply(lambda x: x.split('-')[-1]))
    )

    # divide the monthly means by the monthly normals by matching the month
    mon_ratios = (
        mon_means
        .drop('time', axis=1)
        .div(mon_normals.loc[mon_means.index].values, axis=1)
    )
    mon_ratios.index = mon_means['time']
    mon_ratios = mon_ratios.replace([np.inf, -np.inf], np.nan).dropna(axis=1, how='any')  # drop cols with inf or nan
    mon_ratios.transpose().to_parquet(f'./tables/{region_name}_monthly_ratios.parquet')

    # calculate the ranks by grouping the dataframe by month and ranking values against other values in the same month
    mon_ranks = pd.concat([
        mon_ratios[mon_ratios.index.str.endswith(f'-{month}')].rank() for month in
        pd.Series(mon_ratios.index).apply(lambda x: x.split('-')[-1]).unique()
    ]).astype(int).sort_index()
    # monthly_ranks = mon_ratios.rank(axis=0).astype(int)
    mon_ranks.transpose().to_parquet(f'./tables/{region_name}_monthly_ranks.parquet')

    mon_percentile = mon_ranks.div(mon_ranks.max(axis=1), axis=0)
    mon_percentile.transpose().to_parquet(f'./tables/{region_name}_monthly_percentiles.parquet')

    # classify the values depending on their rank percentile
    bins = [0, .13, .28, .72, .87, float('inf')]
    classes = ['Low Flow', 'Below Normal', 'Normal', 'Above Normal', 'High Flow']
    colors = ['#CD233F', '#FFA885', '#E7E2BC', '#8ECEEE', '#2C7DCD']

    # classify all values in the dataframe into one of 5 classes
    monthly_classes = pd.DataFrame({col: pd.cut(mon_percentile[col], bins=bins, labels=classes) for col in mon_percentile.columns})
    monthly_classes.transpose().to_parquet(f'./tables/{region_name}_monthly_classes.parquet')
    # monthly_colors = pd.DataFrame({col: pd.cut(mon_percentile[col], bins=bins, labels=colors) for col in mon_percentile.columns})
    (
        monthly_classes
        .replace('Low Flow', '#CD233F')
        .replace('Below Normal', '#FFA885')
        .replace('Normal', '#E7E2BC')
        .replace('Above Normal', '#8ECEEE')
        .replace('High Flow', '#2C7DCD')
        .transpose()
        .to_parquet(f'./tables/{region_name}_monthly_colors.parquet')
    )
    # monthly_colors.to_parquet(f'./tables/{region_name}_monthly_colors.parquet')
    return


def quarterly_mean_table(nc_path):
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
    quarter_means = df.groupby(df.index.strftime("%Y-") + df.index.quarter.astype(str)).mean()
    quarter_means.index.name = 'time'

    # define monthly normal based on 1991-2020
    quarter_normals = df[normal_time_period].groupby(df[normal_time_period].index.quarter).mean()
    quarter_normals.index.name = 'quarter'
    quarter_normals.columns = quarter_normals.columns.astype(str)
    quarter_normals.index = quarter_normals.index.astype(str)
    quarter_normals.transpose().to_parquet(f'./tables/{region_name}_quarterly_normals_19912020.parquet')

    # create new index with month number for division broadcasting by matching month number
    quarter_means = (
        quarter_means
        .reset_index()
        .set_index(pd.Series(quarter_means.index).apply(lambda x: x.split('-')[-1]))
    )

    # divide the monthly means by the monthly normals by matching the month
    quarter_ratios = (
        quarter_means
        .drop('time', axis=1)
        .div(quarter_normals.loc[quarter_means.index].values, axis=1)
    )
    quarter_ratios.index = quarter_means['time']
    quarter_ratios = quarter_ratios.replace([np.inf, -np.inf], np.nan).dropna(axis=1, how='any')  # drop cols with inf or nan
    quarter_ratios.transpose().to_parquet(f'./tables/{region_name}_quarterly_ratios.parquet')

    # calculate the ranks by grouping the dataframe by month and ranking values against other values in the same month
    quarter_ranks = pd.concat([
        quarter_ratios[quarter_ratios.index.str.endswith(f'-{month}')].rank() for month in
        pd.Series(quarter_ratios.index).apply(lambda x: x.split('-')[-1]).unique()
    ]).astype(int).sort_index()
    # monthly_ranks = quarter_ratios.rank(axis=0).astype(int)
    quarter_ranks.transpose().to_parquet(f'./tables/{region_name}_quarterly_ranks.parquet')

    quarter_percentile = quarter_ranks.div(quarter_ranks.max(axis=1), axis=0)
    quarter_percentile.transpose().to_parquet(f'./tables/{region_name}_quarterly_percentiles.parquet')

    # classify the values depending on their rank percentile
    bins = [0, .13, .28, .72, .87, float('inf')]
    classes = ['Low Flow', 'Below Normal', 'Normal', 'Above Normal', 'High Flow']
    colors = ['#CD233F', '#FFA885', '#E7E2BC', '#8ECEEE', '#2C7DCD']

    # classify all values in the dataframe into one of 5 classes
    monthly_classes = pd.DataFrame({col: pd.cut(quarter_percentile[col], bins=bins, labels=classes) for col in quarter_percentile.columns})
    monthly_classes.transpose().to_parquet(f'./tables/{region_name}_quarterly_classes.parquet')
    # monthly_colors = pd.DataFrame({col: pd.cut(quarter_percentile[col], bins=bins, labels=colors) for col in quarter_percentile.columns})
    (
        monthly_classes
        .replace('Low Flow', '#CD233F')
        .replace('Below Normal', '#FFA885')
        .replace('Normal', '#E7E2BC')
        .replace('Above Normal', '#8ECEEE')
        .replace('High Flow', '#2C7DCD')
        .transpose()
        .to_parquet(f'./tables/{region_name}_quarterly_colors.parquet')
    )
    # monthly_colors.to_parquet(f'./tables/{region_name}_monthly_colors.parquet')
    return


if __name__ == '__main__':
    table_dir = './tables/'
    # ncs = glob.glob('/Users/rchales/Data/geoglows_hindcast/20220430_netcdf/*/*.nc')
    #
    # with Pool(6) as p:
    #     p.map(monthly_mean_table, ncs)

    ncs = glob.glob('/Users/rchales/Data/geoglows_hindcast/20220430_netcdf/central_america-geoglows/*.nc')
    # monthly_mean_table(ncs[0])
    quarterly_mean_table(ncs[0])
