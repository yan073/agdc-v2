from __future__ import absolute_import, division, print_function

import logging
import numpy as np
from collections import defaultdict
from itertools import product
from datetime import datetime
from copy import deepcopy
from pathlib import Path

from functools import reduce
import rasterio
import dateutil.tz
from datacube.storage.storage import write_dataset_to_netcdf
from datacube.storage.masking import make_mask
from datacube.api.geo_xarray import append_solar_day, _get_spatial_dims
from datacube.utils import intersect_points, union_points
from datacube.utils import read_documents
from datacube.model import DatasetType, GeoPolygon
from datacube.model.utils import make_dataset, xr_apply, datasets_to_doc
from model_v1 import Ls57Arg25Bands
from tci_utils import calculate_tci
from utils_v1 import calculate_stack_statistic_count_observed, calculate_stack_statistic_median
from utils_v1 import calculate_stack_statistic_min, calculate_stack_statistic_max
from utils_v1 import calculate_stack_statistic_mean, calculate_stack_statistic_percentile
from utils_v1 import calculate_stack_statistic_variance, calculate_stack_statistic_standard_deviation
from dateutil.tz import tzlocal

_log = logging.getLogger(__name__)


def product_lookup(sat, dataset_type):
    """
    Finds product name from dataset type and sensor name
    :param sat: input dataset type and sensor name
    :param dataset_type: It can be pqa within nbar
    :return: product name like 'ls8_nbar_albers'
    """
    prod_list = [('ls5_nbar_albers', 'LANDSAT_5'), ('ls5_nbar_albers', 'nbar'),
                 ('ls7_nbar_albers', 'LANDSAT_7'), ('ls7_nbar_albers', 'nbar'),
                 ('ls8_nbar_albers', 'LANDSAT_8'), ('ls8_nbar_albers', 'nbar'),
                 ('ls5_nbart_albers', 'LANDSAT_5'), ('ls5_nbart_albers', 'nbart'),
                 ('ls7_nbart_albers', 'LANDSAT_7'), ('ls7_nbart_albers', 'nbart'),
                 ('ls8_nbart_albers', 'LANDSAT_8'), ('ls8_nbart_albers', 'nbart'),
                 ('ls5_pq_albers', 'LANDSAT_5'), ('ls5_pq_albers', 'pqa'),
                 ('ls7_pq_albers', 'LANDSAT_7'), ('ls7_pq_albers', 'pqa'),
                 ('ls8_pq_albers', 'LANDSAT_8'), ('ls8_pq_albers', 'pqa')]

    my_dict = defaultdict(list)
    for k, v in prod_list:
        my_dict[k].append(v)
    for k, v in my_dict.items():
        if sat in v[0] and dataset_type in v[1]:
            return k
    return None


def write_crs_attributes(geobox):
    extents = {'grid_mapping_name': geobox.crs['PROJECTION'].lower(),
               'semi_major_axis': str(geobox.geographic_extent.crs.semi_major_axis),
               'semi_minor_axis': str(geobox.geographic_extent.crs.semi_major_axis),
               'inverse_flattening': str(geobox.geographic_extent.crs.inverse_flattening),
               'false_easting': str(geobox.crs.proj.false_easting),
               'false_northing': str(geobox.crs.proj.false_northing),
               'latitude_of_projection_origin': str(geobox.crs.proj.latitude_of_center),
               'longitude_of_central_meridian': str(geobox.crs.proj.longitude_of_center),
               'long_name': geobox.crs['PROJCS'],
               'standard_parallel': str((geobox.crs.proj.standard_parallel_1,
                                         geobox.crs.proj.standard_parallel_2)),
               'GeoTransform': geobox.affine.to_gdal(),
               'spatial_ref': geobox.crs.wkt,
               'geographic': str(geobox.crs.geographic),
               'projected': str(geobox.crs.projected)
               }
    return extents


def write_global_attributes(self, geobox):
    geo_extents = geobox.geographic_extent.to_crs('EPSG:4326').points
    geo_extents = geo_extents + [geo_extents[0]]
    geospatial_bounds = "POLYGON((" + ", ".join("{0} {1}".format(*p) for p in geo_extents) + "))"
    long_name = geobox.geographic_extent.crs.crs_str
    extents = {
        'Conventions': 'CF-1.6, ACDD-1.3',
        'comment': 'Geographic Coordinate System, ' + long_name,
        'Created': 'File Created on ' + str(datetime.now()) + ' for season ' + self.season.name +
                   ' for year ' + self.acq_min.strftime("%Y") +
                   "-" + self.acq_max.strftime("%Y"),
        'title': 'Statistical Data files From the Australian Geoscience Data Cube',
        'institution': 'GA',
        'processing_level': 'L3',
        'product_version': '2.0.0',
        'project': 'AGDC',
        'geospatial_bounds': geospatial_bounds,
        'geospatial_bounds_crs': 'EPSG:4326',
        'geospatial_lat_min': min(lat for lon, lat in geo_extents),
        'geospatial_lat_max': max(lat for lon, lat in geo_extents),
        'geospatial_lat_units': 'degrees_north',
        'geospatial_lon_min': min(lon for lon, lat in geo_extents),
        'geospatial_lon_max': max(lon for lon, lat in geo_extents),
        'geospatial_lon_units': "degrees_east",
        'grid_mapping_name': "Albers_Conic_Equal_Area"
    }
    return extents


def do_compute(self, data, odata, dtype):  # pylint: disable=too-many-branches

    _log.info("doing computations for %s on  %s of on odata shape %s",
              self.statistic.name, datetime.now(), odata.shape)
    ndv = np.nan
    # pylint: disable=range-builtin-not-iterating
    for x_offset, y_offset in product(range(0, 4000, 4000),
                                      range(0, 4000, self.chunk_size)):
        # if self.dataset_type.name == "TCI":
        #    stack = data[x_offset: 4000, y_offset: y_offset+self.chunk_size]
        # else:
        #    stack = data.isel(x=slice(x_offset, 4000), y=slice(y_offset, y_offset+self.chunk_size)).load().data
        stack = data[x_offset: 4000, y_offset: y_offset + self.chunk_size]
        _log.info("stack stats shape %s for %s for (%03d ,%03d) x_offset %d for y_offset %d",
                  stack.shape, self.band.name, self.x_cell, self.y_cell, x_offset, y_offset)
        stack_stat = None
        if self.statistic.name == "MIN":
            stack_stat = calculate_stack_statistic_min(stack=stack, ndv=ndv, dtype=dtype)
        elif self.statistic.name == "MAX":
            stack_stat = calculate_stack_statistic_max(stack=stack, ndv=ndv, dtype=dtype)
        elif self.statistic.name == "MEAN":
            stack_stat = calculate_stack_statistic_mean(stack=stack, ndv=ndv, dtype=dtype)
        elif self.statistic.name == "GEOMEDIAN":
            tran_data = np.transpose(stack)
            _log.info("\t shape of data array to pass %s", np.shape(tran_data))
            # stack_stat = geomedian(tran_data, 1e-3, maxiters=20)
        elif self.statistic.name == "MEDIAN":
            stack_stat = calculate_stack_statistic_median(stack=stack, ndv=ndv, dtype=dtype)
        elif self.statistic.name == "VARIANCE":
            stack_stat = calculate_stack_statistic_variance(stack=stack, ndv=ndv, dtype=dtype)
        elif self.statistic.name == "STANDARD_DEVIATION":
            stack_stat = calculate_stack_statistic_standard_deviation(stack=stack, ndv=ndv, dtype=dtype)
        elif self.statistic.name == "COUNT_OBSERVED":
            stack_stat = calculate_stack_statistic_count_observed(stack=stack, ndv=ndv, dtype=dtype)
        elif 'PERCENTILE' in self.statistic.name:
            percent = int(str(self.statistic.name).split('_')[1])
            _log.info("\tcalculating percentile %d", percent)
            stack_stat = calculate_stack_statistic_percentile(stack=stack,
                                                              percentile=percent,
                                                              ndv=ndv, interpolation=self.interpolation)

        odata[y_offset:y_offset + self.chunk_size, x_offset:4000] = stack_stat
        _log.info("stats finished for (%03d, %03d) band %s on %s", self.x_cell, self.y_cell, self.band.name, odata)
    return odata


def fuse_data(self, gw, dtype, mindt, maxdt):  # pylint: disable=too-many-locals,too-many-branches
    # pylint: disable=too-many-statements
    my_cell = (self.x_cell, self.y_cell)
    my_data = defaultdict()
    tdata = None
    # first collect datasets for each satellite
    for satellite in list(self.satellites):
        prodname = product_lookup(satellite, self.dataset_type.value.lower())
        cell_info = gw.list_cells(my_cell, product=prodname, time=(mindt, maxdt))
        my_data[satellite] = cell_info
        for k, v in cell_info.iteritems():
            _log.info("\t data sources for satellite %s %s", satellite, v)

    ls_stack = np.zeros((1, 1, 1), dtype=dtype)
    cell_list_obj = None
    origattr = None
    for satellite in list(self.satellites):
        _log.info("\tloading dataset for %3d %4d on band  %s stats  %s  in the date range  %s %s for satellite %s",
                  self.x_cell, self.y_cell, self.band.name, self.statistic.name, mindt, maxdt, satellite)
        pq = None
        cell_info = my_data[satellite]
        if cell_info[my_cell]:
            cell_list_obj = cell_info[my_cell]
            data = gw.load(cell_list_obj, dask_chunks={'time': len(cell_list_obj['sources']),
                                                       'y': self.chunk_size, 'x': self.chunk_size})
        else:
            _log.info("\t No data found for (%d %d) in the date range  %s %s", self.x_cell, self.y_cell,
                      mindt, maxdt)
            continue
        origattr = data.attrs
        mask_clear = None
        if self.mask_pqa_apply:
            prodname = product_lookup(satellite, 'pqa')
            pq = gw.list_cells(my_cell, product=prodname, time=(mindt, maxdt))
            if pq:
                _log.info("\tloading pq dataset for %3d %4d on band  %s stats  %s from  %s to %s for satellite %s",
                          self.x_cell, self.y_cell, self.band.name, self.statistic.name, mindt, maxdt, satellite)
                pq = gw.load(pq[my_cell], dask_chunks={'time': len(pq[my_cell]['sources']),
                                                       'y': self.chunk_size, 'x': self.chunk_size})
                for mask in self.mask_pqa_mask:
                    if mask.name == "PQ_MASK_CLEAR_ELB":
                        mask_clear = pq['pixelquality'] & 15871 == 15871
                    elif mask.name == "PQ_MASK_CLEAR":
                        mask_clear = pq['pixelquality'] & 16383 == 16383
                    else:
                        mask_clear = make_mask(pq, apply_mask())
                data = data.where(mask_clear)
            else:
                _log.info("\t No PQ data exists")
            _log.info("\tpq dataset call completed for %3d %4d on band  %s stats  %s pqdata %s",
                      self.x_cell, self.y_cell, self.band.name, self.statistic.name, pq)

        append_solar_day(data, get_mean_longitude(data))
        data = data.groupby('solar_day').max(dim='time')
        season_dict = {'SUMMER': 'DJF', 'AUTUMN': 'MAM', 'WINTER': 'JJA', 'SPRING': 'SON',
                       'CALENDAR_YEAR': 'year', 'QTR_1': '1', 'QTR_2': '2',
                       'QTR_3': '3', 'QTR_4': '4'}
        if "QTR" in self.season.name:
            data = data.isel(solar_day=data.groupby('solar_day.quarter').groups[int(season_dict[self.season.name])])
        elif "CALENDAR" in self.season.name:
            year = int(str(data.groupby('solar_day.year').groups.keys()).strip('[]'))
            data = data.isel(solar_day=data.groupby('solar_day.year').groups[year])
        elif self.season.name == "TIDAL":
            # extract each landsat time series
            ls_list = list()
            for xx in self.date_list:
                if xx[2] == satellite.rsplit('_', 1)[1]:
                    ls_list.append(xx)
            if len(ls_list) >= 1:
                lsdata = [xx[4:] for xx in ls_list]
                # change into local time
                from_zone = dateutil.tz.tzutc()
                lsdata = [datetime.strptime(xx,
                                            "%Y-%m-%dT%H:%M:%S").replace(tzinfo=from_zone).astimezone(tzlocal()).date()
                          for xx in lsdata]
                # now check and get the correct dates after PQ applied
                sol_lsdata = list()
                if pq:
                    solar_day_date = data.solar_day.values.astype('datetime64[D]').tolist()
                    solar_day_date = [dt for dt in solar_day_date]
                    for dt in lsdata:
                        if dt in solar_day_date:
                            sol_lsdata.append(dt)
                else:
                    sol_lsdata = lsdata
                data = data.sel_points(solar_day=sol_lsdata)
                tdata = data
                _log.info("Received band %s data is %s ", self.band.name, data)
            else:
                _log.info("\t No Landsat data for %s", satellite)
                continue
            _log.info("Tidal datasets constructed")
        else:
            data = data.isel(solar_day=data.groupby('solar_day.season').groups[season_dict[self.season.name]])

        if self.band.name in [t.name for t in Ls57Arg25Bands]:
            data = get_band_data(self, data)
        else:
            data = get_derive_data(self, data)

        stack = np.zeros((len(data), 4000, 4000), dtype=dtype)
        _log.info("\t Time to start stacking for %s %s", satellite, str(datetime.now()))
        for x_offset, y_offset in product(range(0, 4000, 4000),
                                          range(0, 4000, 400)):
            stack[:, y_offset:y_offset + 400, x_offset:4000] = \
                data.isel(x=slice(x_offset, 4000), y=slice(y_offset, y_offset + 400)).load().data
            _log.info("\t Time to stack 400 chunks  %s ", str(datetime.now()))
        _log.info("\t Time to finish stacking for %s %s", satellite, str(datetime.now()))
        if ls_stack.any():
            ls_stack = np.vstack((stack, ls_stack))
        else:
            ls_stack = stack
    if self.season.name == "TIDAL":
        return tdata, ls_stack, cell_list_obj, origattr
    else:
        return data, ls_stack, cell_list_obj, origattr


def get_derive_data(self, data):
    output_data = None
    sat = ",".join(self.satellites)
    _log.info("getting derived data for %s for satellite %s", self.dataset_type.name, sat)

    blue = data.blue
    green = data.green
    red = data.red
    nir = data.nir
    sw1 = data.swir1
    sw2 = data.swir2
    if self.band.name == "NDFI":
        output_data = (sw1 - nir) / (sw1 + nir)
        output_data.name = "NDFI data"
    elif self.band.name == "NDVI":
        output_data = (nir - red) / (nir + red)
        output_data.name = "NDVI data"
    elif self.band.name == "NDWI":
        output_data = (green - nir) / (green + nir)
        output_data.name = "NDWI data"
    elif self.band.name == "MNDWI":
        output_data = (green - sw1) / (green + sw1)
        output_data.name = "MNDWI data"
    elif self.band.name == "NBR":
        output_data = (nir - sw2) / (nir + sw2)
        output_data.name = "NBR data"
    elif self.band.name == "EVI":
        g, l, c1, c2 = self.evi_args  # pylint: disable=unpacking-non-sequence
        output_data = g * ((nir - red) / (nir + c1 * red - c2 * blue + l))
        output_data.name = "EVI data"
        _log.info("EVI cooefficients used are G=%f, l=%f, c1=%f, c2=%f", g, l, c1, c2)
    elif self.band.name == "TCI":
        output_data = calculate_tci(self.band.name, sat, blue, green, red, nir, sw1, sw2)
        _log.info(" shape of TCI array is %s", output_data.shape)
    return output_data


def get_band_data(self, data):  # pylint: disable=too-many-branches
    band_data = None
    if self.band.name == "BLUE":
        band_data = data.blue
    if self.band.name == "GREEN":
        band_data = data.green
    if self.band.name == "RED":
        band_data = data.red
    if self.band.name == "NEAR_INFRARED":
        band_data = data.nir
    if self.band.name == "SHORT_WAVE_INFRARED_1":
        band_data = data.swir1
    if self.band.name == "SHORT_WAVE_INFRARED_2":
        band_data = data.swir2
    return band_data


def apply_mask(self):
    ga_pixel_bit = {name: True for name in
                    ('swir2_saturated',
                     'red_saturated',
                     'blue_saturated',
                     'nir_saturated',
                     'green_saturated',
                     'tir_saturated',
                     'swir1_saturated')}
    ga_pixel_bit.update(dict(contiguous=False, land_sea='land', cloud_shadow_acca='no_cloud_shadow',
                             cloud_acca='no_cloud', cloud_fmask='no_cloud',
                             cloud_shadow_fmask='no_cloud_shadow'))

    for mask in self.mask_pqa_mask:
        if mask.name == "PQ_MASK_CONTIGUITY":
            ga_pixel_bit.update(dict(contiguous=True))
        if mask.name == "PQ_MASK_CLOUD_FMASK":
            ga_pixel_bit.update(dict(cloud_fmask='no_cloud'))
        if mask.name == "PQ_MASK_CLOUD_ACCA":
            ga_pixel_bit.update(dict(cloud_acca='no_cloud_shadow'))
        if mask.name == "PQ_MASK_CLOUD_SHADOW_ACCA":
            ga_pixel_bit.update(dict(cloud_shadow_acca='no_cloud_shadow'))
        if mask.name == "PQ_MASK_SATURATION":
            ga_pixel_bit.update(dict(blue_saturated=False, green_saturated=False, red_saturated=False,
                                     nir_saturated=False, swir1_saturated=False, tir_saturated=False,
                                     swir2_saturated=False))
        if mask.name == "PQ_MASK_SATURATION_OPTICAL":
            ga_pixel_bit.update(dict(blue_saturated=False, green_saturated=False, red_saturated=False,
                                     nir_saturated=False, swir1_saturated=False, swir2_saturated=False))
        if mask.name == "PQ_MASK_SATURATION_THERMAL":
            ga_pixel_bit.update(dict(tir_saturated=False))
        _log.info("applying bit mask %s on %s ", mask.name, ga_pixel_bit)

    return ga_pixel_bit


def config_loader(index, app_config_file):
    app_config_path = Path(app_config_file)
    _, config = next(read_documents(app_config_path))
    config['app_config_file'] = app_config_path.name

    chunking = config['storage']['chunking']
    chunking = [chunking[dim] for dim in config['storage']['dimension_order']]
    source_type = index.products.get_by_name(config['source_type'])
    var_param_keys = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous', 'attrs'}
    variable_params = {}
    for mapping in config['measurements']:
        varname = mapping['name']
        variable_params[varname] = {k: v for k, v in mapping.items() if k in var_param_keys}
        variable_params[varname]['chunksizes'] = chunking

    config['variable_params'] = variable_params

    config['nbar_dataset_type'] = source_type
    return config


def make_stats_config(index, config, **query):
    dry_run = query.get('dry_run', True)
    config['overwrite'] = query.get('overwrite', False)

    source_type = index.products.get_by_name(config['source_type'])
    if not source_type:
        _log.error("Source DatasetType %s does not exist", config['source_type'])
        return 1

    output_type_definition = deepcopy(source_type.definition)
    output_type_definition['name'] = config['output_type']
    output_type_definition['managed'] = True
    output_type_definition['description'] = config['description']
    output_type_definition['storage'] = config['storage']
    output_type_definition['metadata']['format'] = {'name': 'NetCDF'}
    output_type_definition['metadata']['product_type'] = config.get('product_type', 'fractional_cover')

    var_def_keys = {'name', 'dtype', 'nodata', 'units', 'aliases', 'spectral_definition', 'flags_definition'}

    output_type_definition['measurements'] = [{k: v for k, v in measurement.items() if k in var_def_keys}
                                              for measurement in config['measurements']]

    chunking = config['storage']['chunking']
    chunking = [chunking[dim] for dim in config['storage']['dimension_order']]

    var_param_keys = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous', 'attrs'}
    variable_params = {}
    for mapping in config['measurements']:
        varname = mapping['name']
        variable_params[varname] = {k: v for k, v in mapping.items() if k in var_param_keys}
        variable_params[varname]['chunksizes'] = chunking

    config['variable_params'] = variable_params

    output_type = DatasetType(source_type.metadata_type, output_type_definition)
    if not dry_run:
        _log.info('Created DatasetType %s', output_type.name)
        output_type = index.products.add(output_type)

    config['nbar_dataset_type'] = source_type
    config['stats_metadata_type'] = output_type

    return config


def get_app_metadata(config):
    doc = {
        'lineage': {
            'algorithm': {
                'name': 'datacube-ingest',
                'version': config.get('version', 'unknown'),
                'repo_url': 'https://github.com/data-cube/agdc-v2/agdc-v2.git',
                'parameters': {'configuration_file': config.get('app_config_file', 'stats_app')}
            },
        }
    }
    return doc


def stats_extra_metadata(config, ds, cell_list_obj, filename):
    geobox = cell_list_obj['geobox']

    global_attributes = config['global_attributes']
    variable_params = config['variable_params']
    file_path = Path(config['location'] + "/" + filename)
    output_type = config['stats_metadata_type']

    def _make_dataset(labels, sources):
        assert len(sources)

        source_data = reduce(union_points, (dataset.extent.to_crs(geobox.crs).points for dataset in sources))
        valid_data = intersect_points(geobox.extent.points, source_data)
        dataset = make_dataset(dataset_type=output_type,
                               sources=sources,
                               extent=geobox.extent,
                               center_time=labels['time'],
                               uri=file_path.absolute().as_uri(),
                               app_info=get_app_metadata(config),
                               valid_data=GeoPolygon(valid_data, geobox.crs))
        return dataset

    sources = cell_list_obj['sources']
    datasets = xr_apply(sources, _make_dataset, dtype='O')
    ds['dataset'] = datasets_to_doc(datasets)

    if config.get('overwrite', False):
        ds.unlink()

    write_dataset_to_netcdf(ds, global_attributes, variable_params, Path(file_path))

    return


def get_mean_longitude(cell_dataset):
    x, y = _get_spatial_dims(cell_dataset)
    mean_lat = float(cell_dataset[x][0] + cell_dataset[x][-1]) / 2.
    mean_lon = float(cell_dataset[y][0] + cell_dataset[y][-1]) / 2.
    bounds = {'left': mean_lon, 'right': mean_lon, 'top': mean_lat, 'bottom': mean_lat}
    input_crs = cell_dataset.crs.wkt
    left, bottom, right, top = rasterio.warp.transform_bounds(input_crs, 'EPSG:4326', **bounds)
    return left
