# -*- coding: utf-8 -*-
"""
SIDIK - National Oceanographic Data Center (NODC)
An automatic System to Process Wind-Wave dataset from BMKG-OFS

Written by ekosusilo, ekosusilo@live.com

change log:
Created on : Sat May 27 08:21:56 2023
"""

# load modules
# ---------------
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import dask.array as da
import os
import netCDF4
import pydap
import metpy
import metpy.calc as calc
import matplotlib
from matplotlib import pyplot as plt
from getpass import getpass
from datetime import date, datetime, timedelta
import logging

# ------------------------------------
print(f"Data Processing Starting --> {datetime.now()}")
# ------------------------------------

# routine tasking (delay 1 day)
today = date.today() - timedelta(days=1)

# set date for manual tasking
# today = datetime.strptime('2023-05-01', "%Y-%m-%d")

# get date attirubute
dt = today.strftime("%Y%m%d")
yr = today.strftime("%Y")
mt = today.strftime("%m")

# define varible names
data_id = {'global', 'hires', 'reg'}
varname = {'hs','uwnd','vwnd'}

# define output directory
output_dir = "D:/sidik_ops/tmp/"

# provide opendap credentials
user = 'brol'
passw = 'M4R1T1M_BROL'

# general loop
# ---------------
for id in data_id:
    
    print(f"Unduh data --> {id}")
    
    # define dataset filename
    ncdf_name = "w3g_"+id+"_"+dt+"_1200.nc"
    csv_name = "w3g_"+id+"_"+dt+"_1200.csv"
    shp_name = "w3g_"+id+"_"+dt+"_1200.shp"
    
    # define opendap url
    ncdf_url = [f"https://{user}:{passw}@maritim.bmkg.go.id/opendap/ww3gfs/{yr}/{mt}/{ncdf_name}"]
    
    # read dataset from opendap server
    ds = xr.open_mfdataset(ncdf_url)
    
    # select/slice dataset based on variables and dimensions
    dx = ds[varname]
    dx = dx.isel(time=6).sel(lat=slice(-25,20),lon=slice(86,146))
    
    # generate ncdf and save output to local storage
    dx.to_netcdf(f"{output_dir}{ncdf_name}")

    # Calculate wind speed and direction (metpy module)
    dx["WIND_DIREC"] = calc.wind_direction(dx.uwnd, dx.uwnd)   
    dx["WIND_SPEED"] = calc.wind_speed(dx.uwnd, dx.vwnd)
    
    '''
    #Calculate wind speed and direction (numpy module)
    wind_spd = np.sqrt(df.uwnd**2 + df.vwnd**2)
    wind_dir = np.arctan2(df.uwnd, df.uwnd) * 180 / np.pi
    
    #adjusted to meteorological convention (i.e., 0 degrees is north, 90 degrees is east, etc.).
    wind_dir = 270 - wind_dir
    wind_dir[wind_dir < 0] += 360

    df["windspd"] = wind_spd
    df["winddir"] = wind_dir
    '''
    
    # manipulate xarray
    df = dx.drop_vars(['uwnd', 'vwnd', 'time'])
    df = df.rename_vars({'hs': 'WAVE'})
    df = df.rename({'lat': 'POINT_Y', 'lon': 'POINT_X'})
    df = df.to_dataframe().round(2).reset_index()

    # Generate csv
    df.to_csv(f"{output_dir}{csv_name}", index_label='POINTID', sep = ';')
    
    # Generate shapefiles using geopandas
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.POINT_X, df.POINT_Y))
    gdf.to_file(f"{output_dir}{shp_name}", driver='ESRI Shapefile')   
        
# ------------------------------------
print(f"Data Processing Finished --> {datetime.now()}")
# ------------------------------------