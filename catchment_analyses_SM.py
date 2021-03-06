#!/bin/python

import xarray as xr
import sys
import pandas as pd
import numpy as np

gcm_list = ['MIROC5', 'CanESM2', 'CCSM4', 'CNRM-CM5', 'CSIRO-Mk3-6-0',
            'GFDL-ESM2M', 'HadGEM2-CC', 'HadGEM2-ES', 'inmcm4', 'IPSL-CM5A-MR']
scenario_list = ['RCP45', 'RCP85']
hydro_model_list = [('VIC', 'calib_inverse', 'P1', ['Soil_liquid']),
                    ('VIC', 'ORNL', 'P2', ['Soil_liquid']),
                    ('VIC', 'NCAR', 'P3', ['Soil_liquid']),
                    ('PRMS', 'calib_inverse', 'P1', ['Soil_moisture', 'Groundwater_storage'])]
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


for (hydro_model, path_parameter_name, official_convention, sm_vars) in hydro_model_list:
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
                data = data[sm_vars].load()
                if hydro_model == 'VIC':
                    data = data.sum(dim='soil_layers', keep_attrs=True)
                    sm_attrs = data[sm_vars[0]].attrs
                elif hydro_model == 'PRMS':
                    x = data[sm_vars[0]]
                    sm_attrs = data[sm_vars[0]].attrs
                    for i in range(1, len(sm_vars)):
                        x += data[sm_vars[i]]
                        sm_attrs['long_name'] += '+{}'.format(data[sm_vars[i]].attrs['long_name'])
                    data = x.to_dataset()
                else:
                    print('How did we get here?')
                    raise Exception('Unknown model')
            except:
                print('Failed to open data file: {}'.format(file_path))

            for location_id in locations:
                out_path = '/pool0/data/nijssen/crcc/rupp_et_al_2020/catchment_{location_id}_weighted_mean_SM_19500101-20991231_{scenario}_{gcm}_{downscaling}_{hydro_model}-{official_convention}.nc'.format(
                    location_id=location_id,
                    hydro_model=hydro_model,
                    downscaling=downscaling,
                    scenario=scenario,
                    official_convention=official_convention,
                    gcm=gcm)
                try:
                    print('\tProcessing location {}'.format(location_id))
                    data_subset = data.where(mask[location_id]).weighted(weight[location_id]).mean(dim=['lat', 'lon'], keep_attrs=True)
                    data_subset[sm_vars[0]].attrs = sm_attrs
                    data_subset.to_netcdf(out_path)
                except:
                    print('Oh no! Something failed for {} for location {}'.format(
                        file_path, location_id))
