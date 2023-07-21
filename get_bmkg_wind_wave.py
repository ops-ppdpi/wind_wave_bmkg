"""
SIDIK - National Oceanographic Data Center (NODC)
An automatic System to Process Wind-Wave dataset from BMKG-OFS

Written by ekosusilo, ekosusilo@live.com

change log:
Created on Sat May 27 08:21:56 2023
"""

# load modules
# ---------------
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import os
import sys
import ftplib
# import netCDF4
# import pydap
import metpy
import metpy.calc as calc
from datetime import date, datetime, timedelta
from ftplib import FTP, error_perm

# define working directory
work_dir = "C:/Users/ORSPPDPI1/Documents/ww_bmkg"

# create log file
sys.stdout = open(f'{work_dir}/get_bmkg_wind_wave.log', 'a')

print("-----------------------------------------------------")
print(f"Data Processing Starting ({datetime.now()})")
print("-----------------------------------------------------")

# routine tasking 
today = date.today()

# set date for manual tasking
# today = datetime.strptime("2023-06-02", "%Y-%m-%d")

# get input date attributes (data availabilty : delay 1 day)
order_date = today - timedelta(days=1)
dt_input = order_date.strftime("%Y%m%d")
yr_input = order_date.strftime("%Y")
mt_input = order_date.strftime("%m")

# get output date attributes (data availabilty : delay 1 day)
dt_output = today.strftime("%Y%m%d")
yr_output = today.strftime("%Y")
mt_output = today.strftime("%m")

# define product type and varible names
type_id = ("1200")  # 1200 or 0000
data_id = {"global", "hires", "reg"}
varname = {"hs","uwnd","vwnd"}

# provide bmkg opendap credentials
bmkg_host = "maritim.bmkg.go.id/opendap/ww3gfs"
bmkg_user = "brol"
bmkg_pass = "M4R1T1M_BROL"
    
# provide ftp sidik credentials
sidik_host = "fs.bpol.net"
sidik_user = "pjkadmin"
sidik_pass = "sidik$pjk"

# provide ftp laut nusanara credentials
aln_host = "118.97.27.118"
aln_user = "lautnusantara"
aln_pass = "ppdp14apps"

# data processing loop
# ---------------

print(f"* Data Processing : {today}")

for id in data_id:
    
    print(f"* Unduh data --> {id}")
    
    # define output directory
    output_dir_name = f"ww_{dt_output}"
    output_dir_path = f"{work_dir}/{id}/{yr_output}/{output_dir_name}/"
    output_ftp_path = f"sidik/model/ww_bmkg/{id}/{yr_output}/{output_dir_name}/"
    output_aln_path = f"lautnusantara/"
    
    # check or create output directory
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    # else:
    #     print("* Directory already exists")
        
    # define dataset filename
    ncd_name = f"w3g_{id}_{dt_input}_{type_id}.nc"
    csv_name = f"ww_{dt_output}.csv"
    dbf_name = f"ww_{dt_output}.dbf"
    prj_name = f"ww_{dt_output}.prj"
    shp_name = f"ww_{dt_output}.shp"
    shx_name = f"ww_{dt_output}.shx"
    
    # define opendap url
    ncd_url = [f"https://{bmkg_user}:{bmkg_pass}@{bmkg_host}/{yr_input}/{mt_input}/{ncd_name}"]
    
    # read dataset from opendap server
    ds = xr.open_mfdataset(ncd_url)
    
    # select/slice dataset based on variables and dimensions
    dx = ds[varname]
    if type_id == "1200":
        dx = dx.isel(time=6).sel(lat=slice(-25,20),lon=slice(86,146))
    else:
        dx = dx.isel(time=10).sel(lat=slice(-25,20),lon=slice(86,146))
    
    # generate ncdf and save output to local storage
    # dx.to_netcdf(f"{output_dir}{ncd_name}")

    # convert dask array to numpy array (i.e. metpy module doesn't support dask array)
    dx = dx.compute() 
    
    # Calculate wind speed and direction (metpy module)
    dx["WIND_DIREC"] = calc.wind_direction(dx.uwnd, dx.vwnd, convention='from')  # option: from/to
    dx["WIND_SPEED"] = calc.wind_speed(dx.uwnd, dx.vwnd)
      
    # manipulate xarray
    df = dx.drop_vars(["uwnd", "vwnd", "time"])
    df = df.rename_vars({"hs": "WAVE"})
    df = df.rename({"lat": "POINT_Y", "lon": "POINT_X"})
    df = df.to_dataframe().round(2).reset_index()

    # Generate csv
    df.to_csv(f"{output_dir_path}{csv_name}", float_format="%.2f", index_label="POINTID", sep = ";")
    
    # Generate shapefiles using geopandas
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.POINT_X, df.POINT_Y))
    gdf.to_file(f"{output_dir_path}{shp_name}", driver="ESRI Shapefile") 
    
    # upload into sidik server
    sidik_ftp = ftplib.FTP(sidik_host, sidik_user, sidik_pass)
    
    ## create new directory
    try:
        sidik_ftp.mkd(output_ftp_path)
    except error_perm as e:
        if not e.args[0].startswith('550'): 
            raise
            
    ## upload all files
    for j in {"cpg", "csv", "dbf", "shp", "shx"}:
     
        file_to_upload = f"{output_dir_path}ww_{dt_output}.{j}"
        file_to_stored = f"{output_ftp_path}ww_{dt_output}.{j}"
                
        with open(file_to_upload, "rb") as file:
            sidik_ftp.storbinary(f"STOR {file_to_stored}", file)
    
    sidik_ftp.quit()
    
    # upload into laut nusantara server
    if id == "reg":
        aln_ftp = ftplib.FTP(aln_host, aln_user, aln_pass)
        
        file_to_upload = f"{output_dir_path}ww_{dt_output}.csv"
        file_to_stored = f"lautnusantara/ww_{dt_output}.csv"

        with open(file_to_upload, "rb") as file:
            aln_ftp.storbinary(f"STOR {file_to_stored}", file)
        
        aln_ftp.quit()
        
    
print("-----------------------------------------------------")
print(f"Data Processing Finished ({datetime.now()})")
print("-----------------------------------------------------")

# save log file
sys.stdout.close()
sys.stdout = sys.__stdout__


'''
to install moduls
1. open anaconda powershell prompt
2. type pip install <modul_name>
    i.e. pip instal netCDF4, pydap, metpy, geopandas, h5netcdf, pyslp
3. sometimes you need to ugrade moduls
    i.e pip install -U paramiko, cryptography
        pip install --upgrade python-lsp-server

WARNING
metpy wind direction convention note:
‘from’ returns the direction the wind is coming from (meteorological convention)
‘to’ returns the direction the wind is going towards (oceanographic convention)

# Calculate wind speed and direction (numpy module)
dx["WIND_DIREC"] = np.arctan2(dx.uwnd, dx.vwnd) * 180 / np.pi
dx["WIND_SPEED"] = np.sqrt(dx.uwnd**2 + dx.vwnd**2)
    
# adjusted to meteorological convention (i.e., 0 degrees is north, 90 degrees is east, etc.).
dx["WIND_DIREC"] = 270 - dx["WIND_DIREC"]
dx.loc[dx["WIND_DIREC"] < 0, "WIND_DIREC"] += 360
'''

'''
import os.path, os
from ftplib import FTP, error_perm

host = 'localhost'
port = 21

ftp = FTP()
ftp.connect(host,port)
ftp.login('user','pass')
filenameCV = "directorypath"

def placeFiles(ftp, path):
    for name in os.listdir(path):
        localpath = os.path.join(path, name)
        if os.path.isfile(localpath):
            print("STOR", name, localpath)
            ftp.storbinary('STOR ' + name, open(localpath,'rb'))
        elif os.path.isdir(localpath):
            print("MKD", name)

            try:
                ftp.mkd(name)

            # ignore "directory already exists"
            except error_perm as e:
                if not e.args[0].startswith('550'): 
                    raise

            print("CWD", name)
            ftp.cwd(name)
            placeFiles(ftp, localpath)           
            print("CWD", "..")
            ftp.cwd("..")

placeFiles(ftp, filenameCV)

ftp.quit()
'''