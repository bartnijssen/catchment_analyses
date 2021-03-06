#!/bin/python

import xarray as xr
import sys
import pandas as pd
import numpy as np

gcm_list = ['MIROC5', 'CanESM2', 'CCSM4', 'CNRM-CM5', 'CSIRO-Mk3-6-0',
            'GFDL-ESM2M', 'HadGEM2-CC', 'HadGEM2-ES', 'inmcm4', 'IPSL-CM5A-MR']
scenario_list = ['RCP45', 'RCP85']
# NCAR files contain precipitation and temperature
hydro_model_list = [('VIC', 'NCAR', 'P3')]
downscaling = 'maca'
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


for (hydro_model, path_parameter_name, official_convention) in hydro_model_list:
    for scenario in scenario_list:
        for gcm in gcm_list:
            # open the large file only once - then process all locations
            file_path = '/pool0/data/orianac/bpa/output_files_from_hyak/output_files/merged.19500101-20991231.nc.{hydro_model}.{downscaling}.{scenario}.{path_parameter_name}.{gcm}'.format(
                hydro_model=hydro_model,
                downscaling=downscaling,
                scenario=scenario,
                path_parameter_name=path_parameter_name,
                gcm=gcm)
            try:
                print('Processing data file: {}'.format(file_path))
                data = xr.open_dataset(file_path)
                data = data[['Precipitation', 'Tair']].load()
                prcp_attrs = data['Precipitation'].attrs
                tair_attrs = data['Tair'].attrs
            except:
                print('Failed to open data file: {}'.format(file_path))

            for location_id in locations:
                out_path = '/pool0/data/nijssen/crcc/rupp_et_al_2020/catchment_{location_id}_weighted_mean_T_P_19500101-20991231_{scenario}_{gcm}_{downscaling}.nc'.format(
                    location_id=location_id,
                    downscaling=downscaling,
                    scenario=scenario,
                    gcm=gcm)
                try:
                    print('\tProcessing location {}'.format(location_id))
                    data_subset = data.where(mask[location_id]).weighted(weight[location_id]).mean(dim=['lat', 'lon'], keep_attrs=True)
                    data_subset['Precipitation'].attrs = prcp_attrs
                    data_subset['Tair'].attrs = tair_attrs
                    data_subset.to_netcdf(out_path)
                except:
                    print('Oh no! Something failed for {} for location {}'.format(
                        file_path, location_id))
