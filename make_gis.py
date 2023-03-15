import geopandas as gpd
import glob
import os
from multiprocessing.pool import Pool
import matplotlib.pyplot as plt
import contextily as cx

import pandas as pd


def join_gpkg_table(gpkg_path, table_path):
    gdf = gpd.read_file(gpkg_path)

    table = pd.read_parquet(table_path)
    table = table.astype(str)
    table.index = table.index.astype(int)

    gdf = gdf.merge(table, left_on='DrainLnID', right_index=True, how='left')
    gdf = gdf[['geometry', *table.columns]]
    gdf = gdf.dropna(axis=0, how='any')
    # gdf.to_file(f'./gis/{region_name}_date_colors.gpkg', driver='GPKG')

    for column in sorted(table.columns.values.tolist()):
        fig, ax = plt.subplots(tight_layout=True, figsize=(4, 3), dpi=400)
        fig.suptitle(f'Monthly Mean Basin Flow - {column}')
        ax.set_ylabel('')
        ax.set_xlabel('')
        ax.set_xticks([])
        ax.set_yticks([])
        gdf['geometry'].plot(ax=ax, color=gdf[column], linewidth=0)
        cx.add_basemap(ax=ax, zoom=9, source=cx.providers.Esri.WorldTopoMap, attribution='')
        fig.savefig(f'./figures/DR_map_{column}.png')
        plt.close(fig)
    return


def _subroutine(g, c):
    fig, ax = plt.subplots(tight_layout=True, figsize=(4, 3), dpi=400)
    fig.suptitle(f'Monthly Mean Basin Flow - {c}')
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_xticks([])
    ax.set_yticks([])
    g['geometry'].plot(ax=ax, color=g[c], linewidth=0)
    cx.add_basemap(ax=ax, zoom=9, source=cx.providers.Esri.WorldTopoMap, attribution='')
    fig.savefig(f'./figures/DR_map_{c}.png')
    plt.close(fig)


if __name__ == '__main__':
    gpkg_paths = glob.glob('/Users/rchales/Data/geoglows_delineation/catchment_shapefile/*/*.shp')
    region_names = [os.path.basename(x).split('-')[0] for x in gpkg_paths]
    table_paths = [f'./tables/{x}_monthly_classes.parquet' for x in region_names]

    # with Pool(1) as p:
    #     p.starmap(join_gpkg_table, zip(gpkg_paths, table_paths))

    # gpkg = glob.glob('/Users/rchales/Data/geoglows_delineation/catchment_shapefile/central_america*/*.shp')[0]
    gpkg = './dominican_republic_catchments.gpkg'
    table = './tables/central_america_monthly_colors.parquet'
    gdf = gpd.read_file(gpkg)

    table = pd.read_parquet(table)
    table = table.astype(str)
    table.index = table.index.astype(int)

    gdf = gdf.merge(table, left_on='DrainLnID', right_index=True, how='left')
    gdf = gdf[['geometry', *table.columns]]
    gdf = gdf.dropna(axis=0, how='any')
    # gdf.to_file(f'./gis/{region_name}_date_colors.gpkg', driver='GPKG')

    with Pool(10) as p:
        p.starmap(_subroutine, [[gdf, col] for col in table.columns])
