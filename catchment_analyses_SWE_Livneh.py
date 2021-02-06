#!/bin/python

import xarray as xr
import sys
import pandas as pd
import numpy as np

hydro_model_list = [('VIC', 'calib_inverse', 'P1', '/pool0/data/orianac/crcc/data/outputs/VIC/calib_inverse/historical/merged.19500101-20111231.nc'),
                    ('VIC', 'ORNL', 'P2', '/pool0/data/orianac/crcc/data/outputs/VIC/ORNL/historical/merged.19500101-20111231.nc'),
                    ('VIC', 'NCAR', 'P3', '/pool0/data/orianac/crcc/data/outputs/VIC/NCAR/historical/merged.19500101-20111231.nc'),
                    ('PRMS', 'calib_inverse', 'P1', '/pool0/data/orianac/FROM_RAID2/bpa/runs/historical/prms/june2017_historical/nc/merged.19500101-20111231.nc')]
locations = pd.read_csv(
    './rupp_et_al_2020_locations.csv', header=None)[0].values

# Load all the masks
mask = {}
weight = {}
for location_id in locations:
    try:
        mask_path = '/pool0/data/orianac/FROM_RAID9/bpa/RVIC_params_masks/remapped/remapUH_{}.nc'.format(
            location_id)
        mask[location_id] = xr.open_dataset(mask_path).fraction
        weight[location_id] = np.cos(np.deg2rad(mask[location_id].lat, dtype=np.float32))
        weight[location_id].name = "weights"
    except:
        print('Failed to open mask: {}'.format(mask_path))


for (hydro_model, path_parameter_name, official_convention, file_path) in hydro_model_list:
    try:
        print('Processing data file: {}'.format(file_path))
        data = xr.open_dataset(file_path)
        data = data[["SWE"]].load().transpose("lat", "lon", "time").compute()
        swe_attrs = data['SWE'].attrs
    except:
        print('Failed to open data file: {}'.format(file_path))

    for location_id in locations:
        out_path = '/pool0/data/nijssen/crcc/rupp_et_al_2020/catchment_{location_id}_weighted_mean_SWE_Livneh_19500101-20111231_{hydro_model}-{official_convention}.nc'.format(
            location_id=location_id,
            hydro_model=hydro_model,
            official_convention=official_convention)
        try:
            print('\tProcessing location {}'.format(location_id))
            data_subset = data.where(mask[location_id]).weighted(weight[location_id]).mean(dim=['lat', 'lon'], keep_attrs=True)
            data_subset['SWE'].attrs = swe_attrs
            data_subset.to_netcdf(out_path)
        except:
            print('Oh no! Something failed for {} for location {}'.format(
                file_path, location_id))
