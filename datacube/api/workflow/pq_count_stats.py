#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ===============================================================================

author__ = 'u81051'


import argparse
import gdal
import logging
import luigi
import numpy
import osr
import os
import sys
import time
import dask
from collections import namedtuple
from datacube.api.utils_v1 import parse_date_min, parse_date_max, PqaMask, writeable_dir
from datacube.api.utils_v1 import  pqa_mask_arg, season_arg
from datacube.api.model_v1 import Ls57Arg25Bands, Ls8Arg25Bands, NdviBands, NdfiBands, TciBands, Pq25Bands, Fc25Bands, Wofs25Bands, NdwiBands, MndwiBands, EviBands, NbrBands, DsmBands,  dataset_type_database, dataset_type_derived_nbar, DatasetType
from datacube.api.utils_v1 import build_season_date_criteria
from datacube.api.utils_v1 import get_satellite_string
from datacube.api.utils_v1 import Season, SEASONS, NDV
from datacube.api.workflow_setup import Task
from itertools import product
from enum import Enum
import datacube.api
import xarray as xr
from datetime import datetime
from datacube.index import index_connect
from datacube.api import make_mask, list_flag_names
from datacube.storage.storage import GeoBox

dask.set_options(get=dask.async.get_sync)
#dask.set_options(get=dask.threaded.get)
_log = logging.getLogger()

_log.setLevel(logging.DEBUG)

EpochParameter = namedtuple('Epoch', ['increment', 'duration'])

class Satellite(Enum):
    __order__ = "LANDSAT_5 LANDSAT_7 LANDSAT_8"

    LANDSAT_5 = "LANDSAT_5"
    LANDSAT_7 = "LANDSAT_7"
    LANDSAT_8 = "LANDSAT_8"

'''
class DatasetType(Enum):
    __order__ = "nbar, nbart, ortho"

    nbar = "nbar"
    nbart = "nbart"
    ortho = "ortho"
'''

def satellite_arg(s):
    if s in [sat.name for sat in Satellite]:
        return Satellite[s]
    raise argparse.ArgumentTypeError("{0} is not a supported satellite".format(s))

def dataset_type_arg(s):
    if s in [t.name for t in DatasetType]:
        return DatasetType[s]
    raise argparse.ArgumentTypeError("{0} is not a supported dataset type".format(s))

# support all bands

def percentile_interpolation_arg(s):
    if s in [t.name for t in PercentileInterpolation]:
        return PercentileInterpolation[s]
    raise argparse.ArgumentTypeError("{0} is not a supported percentile interpolation".format(s))

'''
class PQmask(Enum):
    __order__ = "band_1_saturated band_2_saturated band_3_saturated band_4_saturated band_5_saturated band_6_1_saturated band_6_2_saturated band_7_saturated contiguity land_obs cloud_acca cloud_fmask cloud_shadow_acca cloud_shadow_fmask"
    band_1_saturated = 0 
    band_2_saturated = 1
    band_3_saturated = 2
    band_4_saturated = 3
    band_5_saturated = 4
    band_6_1_saturated = 5
    band_6_2_saturated = 6
    band_7_saturated = 7
    contiguity = 8
    land_obs = 9
    cloud_acca = 10
    cloud_fmask = 11
    cloud_shadow_acca = 12
    cloud_shadow_fmask = 13
'''

class MaskProduct(Enum):
    __order__ = "TOTAL_OBS CLEAR SATURATION SAT_OPTICAL SAT_THERMAL CONTIGUITY LAND CLOUD CLOUD_ACCA CLOUD_FMASK CLOUD_SH_ACCA CLOUD_SH_FMASK"

    TOTAL_OBS = 0
    CLEAR = 16383 
    SATURATION = 255
    SAT_OPTICAL = 159
    SAT_THERMAL = 96
    CONTIGUITY = 256
    LAND = 512
    CLOUD = 15360
    CLOUD_ACCA = 1024
    CLOUD_FMASK = 2048
    CLOUD_SH_ACCA = 4096
    CLOUD_SH_FMASK = 8192


class PQCountTask(object):
    def __init__(self, name="PQ count Workflow"):

        self.name = name
        self.parser = argparse.ArgumentParser(prog=sys.argv[0], description=self.name)
        self.x_min = None
        self.y_min = None

        #self.dc = None

        self.acq_min = None
        self.acq_max = None
        self.epoch = None
        self.seasons = None
        self.satellites = None
        self.output_directory = None
        self.mask_pqa_apply = False
        self.mask_pqa_mask = None 

        self.local_scheduler = None
        self.workers = None

        self.dataset_type = None

        self.chunk_size = None

        self.statistics = None

        self.interpolation = None

        self.evi_args = None

    def setup_arguments(self):

        self.parser.add_argument("--x-min", help="X index for cells", action="store", dest="x_min", type=int,
                                 choices=range(-99, 98 + 1), required=True, metavar="99 ... 99")


        self.parser.add_argument("--y-min", help="Y index for cells", action="store", dest="y_min", type=int,
                                 choices=range(-99, 98 + 1), required=True, metavar="-99 ... 99")


        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1985")

        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2014")

        self.parser.add_argument("--epoch", help="Epoch increment and duration (e.g. 5 6 means 1985-1990, 1990-1995, etc)",
                                 action="store", dest="epoch", type=int, nargs=2, default=[5, 6])

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellites",
                                 type=str, nargs="+", 
                                 default=["LANDSAT_5", "LANDSAT_7", "LANDSAT_8"])

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=False)

        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask,
                                 default=[PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY, PqaMask.PQ_MASK_CLOUD],
                                 metavar=" ".join([ts.name for ts in PqaMask]))

        self.parser.add_argument("--local-scheduler", help="Use local luigi scheduler rather than MPI",
                                 action="store_true",
                                 dest="local_scheduler", default=False)

        self.parser.add_argument("--workers", help="Number of worker tasks", action="store", dest="workers", type=int,
                                 default=16)

        group = self.parser.add_mutually_exclusive_group()

        group.add_argument("--quiet", help="Less output", action="store_const", dest="log_level", const=logging.WARN)
        group.add_argument("--verbose", help="More output", action="store_const", dest="log_level", const=logging.DEBUG)

        self.parser.set_defaults(log_level=logging.INFO)
        self.parser.add_argument("--dataset-type", help="The type of dataset to process", action="store",
                                 dest="dataset_type", type=dataset_type_arg, choices=self.get_supported_dataset_types(), 
                                 default=DatasetType.nbar,
                                 metavar=" ".join([dt.name for dt in self.get_supported_dataset_types()]))
                                 

        self.parser.add_argument("--chunk-size", help="dask chunk size", action="store", dest="chunk_size", type=int,
                                 choices=range(1, 4000 + 1),
                                 default=1000,  # required=True
                                 metavar="0 ... 4000"
                                 )


        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Season]))

    def process_arguments(self, args):

        # # Call method on super class
        # # super(self.__class__, self).process_arguments(args)
        # workflow.Workflow.process_arguments(self, args)

        self.x_min = args.x_min
        self.y_min = args.y_min

        self.output_directory = args.output_directory

        self.acq_min = parse_date_min(args.acq_min)
        self.acq_max = parse_date_max(args.acq_max)

        self.satellites = args.satellites

        if args.epoch:
            self.epoch = EpochParameter(int(args.epoch[0]), int(args.epoch[1]))

        self.seasons = args.season

        self.mask_pqa_apply = args.mask_pqa_apply
        self.mask_pqa_mask = args.mask_pqa_mask

        self.local_scheduler = args.local_scheduler
        self.workers = args.workers

        _log.setLevel(args.log_level)

        self.dataset_type = args.dataset_type

        # # Verify that all the requested satellites have the requested bands
        #
        # for satellite in self.satellites:
        #     if not all(item in [b.name for b in get_bands(self.dataset_type, satellite)] for item in self.bands):
        #         _log.error("Requested bands [%s] not ALL present for satellite [%s]", self.bands, satellite)
        #         raise Exception("Not all bands present for all satellites")

        self.chunk_size = args.chunk_size


    def log_arguments(self):

        # # Call method on super class
        # # super(self.__class__, self).log_arguments()
        # workflow.Workflow.log_arguments(self)

        _log.info("""
        x = {x_min:03d} 
        y = {y_min:03d} 
        """.format(x_min=self.x_min, y_min=self.y_min ))

        _log.info("""
        acq = {acq_min} to {acq_max}
        epoch = {epoch}
        satellites = {satellites}
        output directory = {output_directory}
        PQ apply = {mask_pqa_apply}
        PQA mask = {pqa_mask}
        local scheduler = {local_scheduler}
        workers = {workers}
        """.format(acq_min=self.acq_min, acq_max=self.acq_max,
                   epoch="{increment:d} / {duration:d}".format(increment=self.epoch.increment, duration=self.epoch.duration),
                   satellites=" ".join(self.satellites),
                   output_directory=self.output_directory, mask_pqa_apply=self.mask_pqa_apply,
                   pqa_mask=self.mask_pqa_apply and " ".join([mask.name for mask in self.mask_pqa_mask]) or "",
                   local_scheduler=self.local_scheduler, workers=self.workers))

        _log.info("""
        dataset to retrieve = {dataset_type}
        dask chunk size = {chunk_size}
        seasons = {seasons}
        """.format(dataset_type=self.dataset_type.name,
                   chunk_size=self.chunk_size, 
                   seasons=" ".join([s.name for s in self.seasons])))

    def get_epochs(self):

        from dateutil.rrule import rrule, YEARLY
        from dateutil.relativedelta import relativedelta

        for dt in rrule(YEARLY, interval=self.epoch.increment, dtstart=self.acq_min, until=self.acq_max):
            acq_min = dt.date()
            acq_max = acq_min + relativedelta(years=self.epoch.duration, days=-1)

            acq_min = max(self.acq_min, acq_min)
            acq_max = min(self.acq_max, acq_max)

            yield acq_min, acq_max

    @staticmethod
    def get_supported_dataset_types():
        return dataset_type_database  + dataset_type_derived_nbar

    def get_seasons(self):

        for season in self.seasons:
            yield season


    def create_all_tasks(self):

        cells = (self.x_min, self.y_min)
        _log.info(" cell values  %s", cells)
    #cells = dc.list_cells(product='nbart', time=((2013,10,1), (2013,10,13)), latitude=(-21, -22), longitude=(142, 144))
        for ((acq_min, acq_max), season) in product(self.get_epochs(), self.get_seasons()):
            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(self.acq_min, self.acq_max, season,
                                                                                      seasons=SEASONS,
                                                                                      extend=True)
            mindt = (int(criteria[0][0].strftime("%Y")),int(criteria[0][0].strftime("%m")), int(criteria[0][0].strftime("%d")))
            maxdt = (int(criteria[0][1].strftime("%Y")), int(criteria[0][1].strftime("%m")), int(criteria[0][1].strftime("%d")))
            _log.info("Creating task at %s for epoch stats %s %s %s %s %s crit min date %s , crit max date %s",datetime.now(), self.x_min, self.y_min, acq_min_extended, acq_max_extended, season, mindt, maxdt)
            yield self.create_new_task(x=self.x_min, y=self.y_min, acq_min=acq_min_extended, acq_max=acq_max_extended,
                                  season=season, 
                                  dataset_type=self.dataset_type,  
                                  mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask, 
                                  chunk_size=self.chunk_size)
                                  
          
    def create_new_task(self, x, y, acq_min, acq_max, season, dataset_type, mask_pqa_apply, 
                       mask_pqa_mask, chunk_size):
            return EpochCountTask(x=x, y=y,  
                                               acq_min=acq_min, acq_max=acq_max,
                                               season=season,
                                               satellites=self.satellites,
                                               dataset_type=dataset_type,
                                               mask_pqa_apply=mask_pqa_apply, mask_pqa_mask=mask_pqa_mask,
                                               chunk_size=chunk_size, 
                                               output_directory=self.output_directory)

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()
        if self.local_scheduler:
            luigi.build(self.create_all_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)
        else:
            import luigi.contrib.mpi as mpi
            mpi.run(self.create_all_tasks())



class EpochCountTask(Task):
    x = luigi.IntParameter()
    y = luigi.IntParameter()

    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()

    season = luigi.Parameter()
    #epochs = luigi.Parameter(is_list=True, significant=False)

    satellites = luigi.Parameter(is_list=True)

    dataset_type = luigi.Parameter()

    #bands = luigi.Parameter(is_list=True)

    mask_pqa_apply = luigi.BooleanParameter()
    mask_pqa_mask = luigi.Parameter(is_list=True)

    chunk_size = luigi.IntParameter()

    output_directory = luigi.Parameter()

    def output(self):
        from datetime import date

        season = SEASONS[self.season]

        acq_min = self.acq_min.strftime("%Y%m%d")
        acq_max = self.acq_max.strftime("%Y%m%d")

        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])

        return luigi.LocalTarget(os.path.join(self.output_directory,
                                              "{satellite}_PQCOUNT_{x:03d}_{y:03d}_{acq_min}_{acq_max}_{season_start}_{season_end}.nc".format(satellite=self.satellites[0],
                                                  x=self.x, y=self.y,
                                                  acq_min=acq_min,
                                                  acq_max=acq_max,
                                                  season_start=season_start,
                                                  season_end=season_end
                                              )))
    def apply_mask(self):

        GA_GOOD_PIXEL = {name: False for name in ('band_5_saturated',
                        'band_6_1_saturated',
                        'cloud_shadow_acca',
                        'cloud_fmask',
                        'band_3_saturated',
                        'band_1_saturated',
                        'band_4_saturated',
                        'band_2_saturated',
                        'cloud_acca',
                        'band_6_2_saturated',
                        'cloud_shadow_fmask',
                        'band_7_saturated')}
        GA_GOOD_PIXEL.update(dict(contiguity=True, land_obs=True))
        for mask in self.mask_pqa_mask:
            if mask.name == "PQ_MASK_CLEAR" :
                #_log.info("applying mask for %s on %s", mask.name,  GA_GOOD_PIXEL)
                return GA_GOOD_PIXEL
        #return GA_GOOD_PIXEL
        GA_PIXEL_BIT = {name: True for name in ('band_5_saturated',
                        'band_6_1_saturated',
                        'cloud_shadow_acca',
                        'cloud_fmask',
                        'band_3_saturated',
                        'band_1_saturated',
                        'band_4_saturated',
                        'band_2_saturated',
                        'cloud_acca',
                        'band_6_2_saturated',
                        'cloud_shadow_fmask',
                        'band_7_saturated')}
        GA_PIXEL_BIT.update(dict(contiguity=False, land_obs=False))

        for mask in self.mask_pqa_mask:
            if mask.name == "PQ_MASK_CONTIGUITY" :
                GA_PIXEL_BIT.update(dict(contiguity=True))
            if mask.name == "PQ_MASK_CLOUD_FMASK" :
                GA_PIXEL_BIT.update(dict(cloud_fmask=False)) 
            if mask.name == "PQ_MASK_CLOUD_ACCA" :
                GA_PIXEL_BIT.update(dict(cloud_acca=False)) 
            if mask.name == "PQ_MASK_CLOUD_SHADOW_ACCA" :
                GA_PIXEL_BIT.update(dict(cloud_shadow_acca=False)) 
            if mask.name == "PQ_MASK_SATURATION" :
                GA_PIXEL_BIT.update(dict(band_1_saturated=False, band_2_saturated=False, band_3_saturated=False, band_4_saturated=False, band_5_saturated=False, band_6_1_saturated=False, band_6_2_saturated=False, band_7_saturated=False))
            if mask.name == "PQ_MASK_SATURATION_OPTICAL" :
                GA_PIXEL_BIT.update(dict(band_1_saturated=False, band_2_saturated=False, band_3_saturated=False, band_4_saturated=False, band_5_saturated=False, band_7_saturated=False)) 
            if mask.name == "PQ_MASK_SATURATION_OPTICAL" :
                GA_PIXEL_BIT.update(dict(band_6_1_saturated=False, band_6_2_saturated=False)) 
            _log.info("applying bit mask %s on %s ", mask.name, GA_PIXEL_BIT)

        return GA_PIXEL_BIT

    def write_crs_attributes(self, index, storage_type, crs_spatial_ref):

        tile_index = (self.x, self.y)
        storage_det = index.storage.types.get_by_name(storage_type)
        geobox = GeoBox.from_storage_type(storage_det, tile_index)

        extents = {
              'grid_mapping_name': geobox.crs.GetAttrValue('PROJECTION'),
              'semi_major_axis': str(geobox.geographic_extent.crs.GetSemiMajor()),
              'semi_minor_axis': str(geobox.geographic_extent.crs.GetSemiMinor()),
              'inverse_flattening': str(geobox.geographic_extent.crs.GetInvFlattening()),
              'false_easting': str(geobox.crs.GetProjParm('false_easting')), 
              'false_northing': str(geobox.crs.GetProjParm('false_northing')),
              'latitude_of_projection_origin': str(geobox.crs.GetProjParm('latitude_of_center')),
              'longitude_of_central_meridian': str(geobox.crs.GetProjParm('longitude_of_center')),
              'long_name': geobox.crs.GetAttrValue('PROJCS'),
              'standard_parallel': (geobox.crs.GetProjParm('standard_parallel_1'),
                                   geobox.crs.GetProjParm('standard_parallel_2')), 
              'spatial_ref': crs_spatial_ref,
              'GeoTransform': geobox.affine.to_gdal(),
              'crs_wkt': geobox.crs.ExportToWkt() 
        }        
        return extents

               
    def write_geographical_extents_attributes(self, index, storage_type):

        tile_index = (self.x, self.y)
        #_log.info("storage type found %s ", storage_type) 
        storage_det = index.storage.types.get_by_name(storage_type)
        geobox = GeoBox.from_storage_type(storage_det, tile_index)

        geo_extents = geobox.geographic_extent.to_crs('EPSG:4326').points
        geo_extents = geo_extents + [geo_extents[0]]
        geospatial_bounds = "POLYGON((" + ", ".join("{0} {1}".format(*p) for p in geo_extents) + "))"
        geospatial_bounds_crs = "EPSG:4326"

        geospatial_lat_min = min(lat for lon, lat in geo_extents)
        geospatial_lat_max = max(lat for lon, lat in geo_extents)
        geospatial_lat_units = "degrees_north"
        geospatial_lon_min = min(lon for lon, lat in geo_extents)
        geospatial_lon_max = max(lon for lon, lat in geo_extents)
        geospatial_lon_units = "degrees_east"
        long_name= geobox.geographic_extent.crs.GetAttrValue('GEOGCS')
        extents = {
              'Conventions': 'CF-1.6, ACDD-1.3',
              'comment': 'Geographic Coordinate System, ' + long_name,
              'Created': 'File Created on ' + str(datetime.now()) + ' for season ' + self.season.name + ' for year ' + self.acq_min.strftime("%Y")
               + "-" + self.acq_max.strftime("%Y"),
              'title': 'Statistical Data files From the Australian Geoscience Data Cube',
              'institution': 'GA',
              'processing_level': 'L3',
              'product_version': '2.0.0',
              'project': 'AGDC',
              'geospatial_bounds':geospatial_bounds,
              'geospatial_bounds_crs':geospatial_bounds_crs,
              'geospatial_lat_min': geospatial_lat_min,
              'geospatial_lat_max': geospatial_lat_max,
              'geospatial_lat_units': geospatial_lat_units,
              'geospatial_lon_min': geospatial_lon_min,
              'geospatial_lon_max': geospatial_lon_max,
              'geospatial_lon_units': geospatial_lon_units,
              'grid_mapping_name': geobox.crs.GetAttrValue('PROJECTION')
        }        
        return extents

    def data_write(self, dtype):

        filename = self.output().path
        acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(self.acq_min, self.acq_max, self.season,
                                                                                      seasons=SEASONS,
                                                                                      extend=True)
        mindt = (int(criteria[0][0].strftime("%Y")),int(criteria[0][0].strftime("%m")), int(criteria[0][0].strftime("%d")))
        maxdt = (int(criteria[0][1].strftime("%Y")), int(criteria[0][1].strftime("%m")), int(criteria[0][1].strftime("%d")))
        index = index_connect()
        dc = datacube.api.API(index, app="stats-app")
        #datasetquery= {(self.x, self.y), 'product'= self.dataset_type, 'platform'= self.satellites , 'time'=(mindt, maxdt)}
        #datasetquery = {(self.x, self.y), 'product': self.dataset_type, 'platform': self.satellites, 'time': (mindt, maxdt)}
        _log.info("\tcalling dataset for %3d %4d on pq data in the date range  %s %s ", self.x, self.y, mindt, maxdt) 
        data = dc.get_dataset_by_cell((self.x, self.y), product='pqa', platform=self.satellites ,time=(mindt, maxdt))
        if data:
            _log.info("\t pq dataset shape reurned %s", data.pixelquality.shape)
        else:
            _log.info("\t no pq dataset found")
        crs_spatial_ref = data.crs.spatial_ref
        storage_type = data.storage_type
        #data = dc.get_dataset_by_cell(**datasetquery)

        #mask_clear=self.apply_mask()
        #data = dc.get_dataset_by_cell(**datasetquery)
        #create spatialreference
        descriptor = self.write_geographical_extents_attributes(index, storage_type)
        #_log.info("descriptor values %s", descriptor)
        stats_dataset = None
        filename = self.output().path
        mask_clear = None

        GA_GOOD_PIXEL = {'band_5_saturated':False,
                        'band_6_1_saturated':False,
                        'cloud_shadow_acca':False,
                        'cloud_fmask':False,
                        'band_3_saturated':False,
                        'band_1_saturated':False,
                        'band_4_saturated':False,
                        'band_2_saturated':False,
                        'cloud_acca':False,
                        'band_6_2_saturated':False,
                        'cloud_shadow_fmask':False,
                        'band_7_saturated':False}
        GA_GOOD_PIXEL.update(dict(contiguity=True, land_obs=True))


        #pq = data.isel(x=slice(0, 4000), y=slice(0, 4000)).load().data
        observation_count = dict()
        shape =(4000,4000)
        for i in MaskProduct:
            observation_count[i] = numpy.zeros(shape=shape, dtype=numpy.int16)
        pqdata = data['pixelquality'].values
        _log.info("\tloading pq dataset for %3d %4d on %s", self.x, self.y, self.dataset_type.name)
        pqmaskdata = numpy.ma.array(pqdata)
        _log.info("\t pqmaskdata is %s", pqmaskdata)
        observ_all = numpy.ma.count(pqmaskdata, axis=0) 
        
        for i in MaskProduct:
            pqmaskdata.mask = numpy.ma.nomask
            if i.name == "TOTAL_OBS":
                observation_count[i] = observ_all
            else:           
                pqmaskdata.mask = pqmaskdata & i.value == i.value
                _log.info("\t pqmaskdata is %s value %d", pqmaskdata, i.value)
                observation_count[i] = numpy.ma.count_masked(pqmaskdata, axis=0)
                # Be aware above array converted into int64 
                observation_count[i] = observation_count[i].astype('int16')
                _log.info("\t masked count for %s data %s dtype %s", i.name, observation_count[i], observation_count[i].dtype)
        
        # get pq object  and drop vars and time
        data = data.pixelquality.isel(time=0).drop('time')
        all_vars = dict() 
        tmp_vars = dict()
        dtype = numpy.int16
        # create a dictionary of count variables
        for i in MaskProduct:
            stats_var = i.name 
            stats_var = stats_var + "_" + self.acq_min.strftime("%Y")

            data.data = observation_count[i]
            _log.info("\t zzz masked count  for %s data %s dtype %s", i.name, data.data, data.dtype)
            data.attrs.clear()
            stats_dataset = data.to_dataset(name = stats_var)
            tmp_vars = {stats_var:{'zlib': True}}
            all_vars.update(tmp_vars)
            stats_dataset.get(stats_var).attrs.update(dict(Comment1='Count calculated on pq datasets for bits ' + i.name))
            if self.season:
                stats_dataset.get(stats_var).attrs.update(dict(long_name= 'pq count for ' + str(self.satellites[0]).lower() + ' data of ' + str(self.season.name).lower()))
                stats_dataset.get(stats_var).attrs.update(dict(standard_name = 'pq_count_' + str(self.season.name).lower() + '_season_' + str(self.satellites[0]).lower()))
            else:
                stats_dataset.get(stats_var).attrs.update(dict(long_name= 'pq count reporting for '
                + str(self.satellites[0]).lower() + ' and duration  ' + self.acq_min.strftime("%Y%mm%dd") + '-' + self.acq_max.strftime("%Y%mm%dd")))
                stats_dataset.get(stats_var).attrs.update(dict(standard_name =  'pq_count_' + self.acq_min.strftime("%Y%mm%dd") + '-' + self.acq_max.strftime("%Y%mm%dd") + '_' + str(self.satellites[0]).lower()))
           
            stats_dataset.get(stats_var).attrs.update(dict(units='metre', _FillValue= '0', grid_mapping = "crs"))    
    
            stats_dataset.attrs.update(dict(descriptor))
            crs_attr = self.write_crs_attributes(index, storage_type, crs_spatial_ref)
            crs_variable = {'crs':0}
        #create crs variable
            stats_dataset = stats_dataset.assign(**crs_variable)
        # global attributes
            stats_dataset.crs.attrs =  crs_attr
            self.coordinate_attr_update(stats_dataset)
            _log.info("stats is ready to write for %s %s (%d, %d) " ,stats_var, self.dataset_type.name, self.x, self.y)
            if i.name == "TOTAL_OBS":
                stats_dataset.to_netcdf(filename, mode='w', format='NETCDF4', engine='netcdf4', encoding={stats_var:{'zlib': True}})
            else:
        #stats_dataset.to_netcdf(filename, mode='w', format='NETCDF4', engine='netcdf4', encoding=all_vars)
            #stats_dataset.to_netcdf(filename, mode='a', format='NETCDF4', engine='netcdf4', encoding={stats_var:{'zlib': True}})
                count_ds = xr.open_dataset(filename)
                newds = count_ds.merge(stats_dataset)
                newds.load()
                count_ds.close()
                newds.to_netcdf(filename, format='NETCDF4', engine='netcdf4', encoding=all_vars)
        return 
       
    def coordinate_attr_update(self, stats_dataset):
        stats_dataset.get('x').attrs.update(dict(long_name = "x coordinate of projection",standard_name = "projection_x_coordinate",axis = "X"))
        stats_dataset.get('y').attrs.update(dict(long_name = "y coordinate of projection",standard_name = "projection_y_coordinate",axis = "Y"))


    def run(self):

        _log.info("Doing count analysis for [%s] dataset", self.dataset_type.name)
        filename = self.output().path
       
        #dtype='float32'
        dtype=numpy.int16
        self.data_write(dtype)
        # update x and y coordinates axis/name attributes to silence gdal warning like "Longitude/X dimension"
        #stats_dataset.to_netcdf(filename, mode='w', format='NETCDF4', engine='netcdf4', encoding={stats_var:{'dtype': dtype, 'scale_factor': 0.1, 'add_offset': 5, 'zlib': True, '_FillValue':-999}})



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    PQCountTask().run()
