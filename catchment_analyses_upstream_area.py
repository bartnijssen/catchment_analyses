#!/bin/python

import xarray as xr
import sys
import pandas as pd
import numpy as np

locations = pd.read_csv(
    './rupp_et_al_2020_locations.csv', header=None)[0].values
latres = np.deg2rad(1/16)
lonres = np.deg2rad(1/16)
earth_radius = 6371 # km
areafactor = latres * lonres * earth_radius**2
# Load all the masks
mask = {}
area = {}
for location_id in sorted(locations):
    try:
        mask_path = '/pool0/data/orianac/FROM_RAID9/temp/remapped/remapUH_{}.nc'.format(
            location_id)
        mask = xr.open_dataset(mask_path).fraction
        area_by_lat = np.cos(np.deg2rad(mask.lat, dtype=np.float32)) * areafactor
        area = (mask*area_by_lat).sum()
    except:
        print('Failed to open mask: {}'.format(mask_path))
    print('{: <8}: {: >8.0f}'.format(location_id, area.values))
