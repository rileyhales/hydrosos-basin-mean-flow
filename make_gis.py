import geopandas as gpd
import glob
import os
from multiprocessing.pool import Pool
import matplotlib.pyplot as plt
import contextily as cx
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

import pandas as pd


def join_gpkg_table(gpkg_path, table_path):
    gdf = gpd.read_file(gpkg_path)

    table = pd.read_parquet(table_path)
    table = table.astype(str)
    table.index = table.index.astype(int)

    gdf = gdf.merge(table, left_on='DrainLnID', right_index=True, how='left')
    gdf = gdf[['geometry', *table.columns]]
    gdf = gdf.dropna(axis=0, how='any')
    # gdf.to_file(f'./gis_src/{region_name}_date_colors.gpkg', driver='GPKG')

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


labels = ['Low Flow', 'Below Normal', 'Normal', 'Above Normal', 'High Flow']
colors = ['#CD233F', '#FFA885', '#E7E2BC', '#8ECEEE', '#2C7DCD']
custom_legend = [Line2D([0], [0], color=c, lw=5, label=l) for c, l in zip(colors, labels)]


def _subroutine(g, c):
    fig, axm = plt.subplots(1, 1, tight_layout=True, figsize=(8, 5), dpi=400)
    fig.suptitle(f'Monthly Mean Basin Flow - {c}', fontsize=16)
    axm.set_ylabel('')
    axm.set_xlabel('')
    axm.set_xticks([])
    axm.set_yticks([])

    # increase the range of the y axes to place a buffer around the plotted area
    g['geometry'].plot(ax=axm, color=g[c], linewidth=0)
    y_min, y_max = axm.get_ylim()
    axm.set_ylim(y_min - 0.12 * (y_max - y_min), y_max + 0.12 * (y_max - y_min))

    cx.add_basemap(ax=axm, zoom=9, source=cx.providers.Esri.WorldTopoMap, attribution='')
    axm.legend(custom_legend, labels, loc="upper center", ncol=5, bbox_to_anchor=(0.5, 1.11))
    fig.savefig(f'./figures/DR_map_{c.replace("-", "")}.png')
    plt.close(fig)
    return


if __name__ == '__main__':
    gpkg_paths = glob.glob('/Users/rchales/Data/geoglows_delineation/catchment_shapefile/*/*.shp')
    region_names = [os.path.basename(x).split('-')[0] for x in gpkg_paths]
    table_paths = [f'./tables/{x}_monthly_classes.parquet' for x in region_names]

    # with Pool(1) as p:
    #     p.starmap(join_gpkg_table, zip(gpkg_paths, table_paths))

    # gpkg = glob.glob('/Users/rchales/Data/geoglows_delineation/catchment_shapefile/central_america*/*.shp')[0]
    gpkg = './dominican_republic_basins.gpkg'
    table = './tables/central_america_monthly_colors.parquet'
    gdf = gpd.read_file(gpkg)

    table = pd.read_parquet(table)
    table = table.astype(str)
    table.index = table.index.astype(int)

    gdf = gdf.merge(table, left_on='DrainLnID', right_index=True, how='left')
    gdf = gdf[['geometry', *table.columns]]
    gdf = gdf.dropna(axis=0, how='any')

    with Pool(20) as p:
        p.starmap(_subroutine, [[gdf, col] for col in table.columns])

    # for column in sorted(table.columns.values.tolist()):
    #     _subroutine(gdf, column)
