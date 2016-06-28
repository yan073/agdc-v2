#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2016 Geoscience Australia
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


"""
    Calculates stats for time series LANDSAT data
    __author__ = 'u81051'
"""

from __future__ import absolute_import
from __future__ import division
import argparse
import logging
import os
import sys
from collections import namedtuple
from itertools import product
from datetime import datetime
import abc
from enum import Enum
import dask
import numpy as np
import luigi
from luigi.task import flatten
from datacube.api.utils_v1 import parse_date_min, parse_date_max, PqaMask, Statistic, writeable_dir
from datacube.api.utils_v1 import pqa_mask_arg, statistic_arg, season_arg, calculate_stack_statistic_median
from datacube.api.model_v1 import Ls57Arg25Bands, Ls8Arg25Bands, NdviBands, NdfiBands, TciBands, Pq25Bands, Fc25Bands
from datacube.api.model_v1 import Wofs25Bands, NdwiBands, MndwiBands, EviBands, NbrBands, DsmBands
from datacube.api.model_v1 import DATASET_TYPE_DATABASE, DATASET_TYPE_DERIVED_NBAR, DatasetType
from datacube.api.utils_v1 import PercentileInterpolation, SEASONS
from datacube.api.utils_v1 import Season, NDV, build_season_date_criteria
from datacube.api.utils_v1 import calculate_stack_statistic_count_observed
from datacube.api.utils_v1 import calculate_stack_statistic_min, calculate_stack_statistic_max
from datacube.api.utils_v1 import calculate_stack_statistic_mean, calculate_stack_statistic_percentile
from datacube.api.utils_v1 import calculate_stack_statistic_variance, calculate_stack_statistic_standard_deviation
from datacube.index import index_connect
# from datacube.api import make_mask, list_flag_names
from datacube.storage.storage import GeoBox
# from datacube.api.fast import geomedian
from datacube.api.tci_utils import calculate_tci
import datacube.api

dask.set_options(get=dask.async.get_sync)
# dask.set_options(get=dask.threaded.get)
_log = logging.getLogger()

_log.setLevel(logging.DEBUG)

EpochParameter = namedtuple('Epoch', ['increment', 'duration'])


class Satellite(Enum):
    """
       Order and satellite names
    """
    __order__ = "LANDSAT_5 LANDSAT_7 LANDSAT_8"

    LANDSAT_5 = "LANDSAT_5"
    LANDSAT_7 = "LANDSAT_7"
    LANDSAT_8 = "LANDSAT_8"


def satellite_arg(s):
    if s in [sat.name for sat in Satellite]:
        return Satellite[s]
    raise argparse.ArgumentTypeError("{0} is not a supported satellite".format(s))


def dataset_type_arg(s):
    if s in [t.name for t in DatasetType]:
        return DatasetType[s]
    raise argparse.ArgumentTypeError("{0} is not a supported dataset type".format(s))


# support all bands
def all_arg_band_arg(s):  # pylint: disable=too-many-branches
    bandclass = None
    if s in [t.name for t in Ls57Arg25Bands]:
        bandclass = Ls57Arg25Bands[s]
    elif s in [t.name for t in NdviBands]:
        bandclass = NdviBands[s]
    elif s in [t.name for t in TciBands]:
        bandclass = TciBands[s]
    elif s in [t.name for t in Ls8Arg25Bands]:
        bandclass = Ls8Arg25Bands[s]
    elif s in [t.name for t in Pq25Bands]:
        bandclass = Pq25Bands[s]
    elif s in [t.name for t in Fc25Bands]:
        bandclass = Fc25Bands[s]
    elif s in [t.name for t in Wofs25Bands]:
        bandclass = Wofs25Bands[s]
    elif s in [t.name for t in NdwiBands]:
        bandclass = NdwiBands[s]
    elif s in [t.name for t in NdfiBands]:
        bandclass = NdfiBands[s]
    elif s in [t.name for t in MndwiBands]:
        bandclass = MndwiBands[s]
    elif s in [t.name for t in EviBands]:
        bandclass = EviBands[s]
    elif s in [t.name for t in NbrBands]:
        bandclass = NbrBands[s]
    elif s in [t.name for t in DsmBands]:
        bandclass = DsmBands[s]
    else:
        raise argparse.ArgumentTypeError("{0} is not a supported band supported band"
                                         .format(s))
    return bandclass


def percentile_interpolation_arg(s):
    if s in [t.name for t in PercentileInterpolation]:
        return PercentileInterpolation[s]
    raise argparse.ArgumentTypeError("{0} is not a supported percentile interpolation"
                                     .format(s))


class Task(luigi.Task):         # pylint: disable=metaclass-assignment
    __metaclass__ = abc.ABCMeta

    def complete(self):

        for output in flatten(self.output()):
            if not output.exists():
                return False
        for dep in flatten(self.deps()):
            if not dep.complete():
                return False
        return True

    @abc.abstractmethod
    def output(self):
        return


class StatsTask(object):       # pylint: disable=too-many-instance-attributes
    def __init__(self, name="Band Statistics Workflow"):

        self.name = name
        self.parser = argparse.ArgumentParser(prog=sys.argv[0],
                                              description=self.name)
        self.x_min = None
        self.y_min = None
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
        self.bands = None
        self.chunk_size = None
        self.statistics = None
        self.interpolation = None
        self.evi_args = None

    def setup_arguments(self):
        # pylint: disable=range-builtin-not-iterating
        self.parser.add_argument("--x-min", help="X index for cells", action="store", dest="x_min", type=int,
                                 choices=range(-99, 98 + 1), required=True,
                                 metavar="-99 ... 99")

        self.parser.add_argument("--y-min", help="Y index for cells", action="store", dest="y_min", type=int,
                                 choices=range(-99, 98 + 1), required=True,
                                 metavar="-99 ... 99")

        self.parser.add_argument("--output-directory", help="output directory", action="store", dest="output_directory",
                                 type=writeable_dir, required=True)

        self.parser.add_argument("--acq-min", help="Acquisition Date", action="store", dest="acq_min", type=str,
                                 default="1985")

        self.parser.add_argument("--acq-max", help="Acquisition Date", action="store", dest="acq_max", type=str,
                                 default="2014")

        self.parser.add_argument("--epoch",
                                 help="Epoch increment and duration (e.g. 5 6 means 1985-1990, 1990-1995, etc)",
                                 action="store", dest="epoch", type=int, nargs=2, default=[5, 6])

        self.parser.add_argument("--satellite", help="The satellite(s) to include", action="store", dest="satellites",
                                 type=str, nargs="+",
                                 default=["LANDSAT_5", "LANDSAT_7", "LANDSAT_8"])

        self.parser.add_argument("--mask-pqa-apply", help="Apply PQA mask", action="store_true", dest="mask_pqa_apply",
                                 default=False)

        self.parser.add_argument("--mask-pqa-mask", help="The PQA mask to apply", action="store", dest="mask_pqa_mask",
                                 type=pqa_mask_arg, nargs="+", choices=PqaMask,
                                 default=[PqaMask.PQ_MASK_SATURATION, PqaMask.PQ_MASK_CONTIGUITY,
                                          PqaMask.PQ_MASK_CLOUD],
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
        self.parser.add_argument("--band", help="The band(s) to process", action="store",
                                 default=Ls57Arg25Bands,  # required=True,
                                 dest="bands", type=all_arg_band_arg, nargs="+",
                                 metavar=" ".join([b.name for b in Ls57Arg25Bands]))
        self.parser.add_argument("--chunk-size", help="dask chunk size", action="store", dest="chunk_size", type=int,
                                 choices=range(1, 4000 + 1),
                                 default=1000,  # required=True
                                 metavar="0 ... 4000")
        self.parser.add_argument("--statistic", help="The statistic(s) to produce", action="store",
                                 default=[Statistic.PERCENTILE_25, Statistic.PERCENTILE_50, Statistic.PERCENTILE_75],
                                 dest="statistic", type=statistic_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Statistic]))
        self.parser.add_argument("--interpolation", help="The interpolation method to use", action="store",
                                 default=PercentileInterpolation.NEAREST,  # required=True,
                                 dest="interpolation", type=percentile_interpolation_arg,
                                 metavar=" ".join([s.name for s in PercentileInterpolation]))
        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Season]))
        self.parser.add_argument("--evi-args", help="evi args(e.g. 2.5,1,6,7.5 for G,L,C1 and C2)",
                                 metavar="G,L,C1 and C2 2.5, 1, 6, 7.5",
                                 action="store", dest="evi_args", type=float, nargs=4, default=[2.5, 1, 6, 7.5])

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
        self.bands = args.bands
        self.chunk_size = args.chunk_size
        self.statistics = args.statistic
        self.evi_args = args.evi_args

        if args.interpolation:
            self.interpolation = args.interpolation
        else:
            self.interpolation = [PercentileInterpolation.NEAREST]

    def log_arguments(self):

        _log.info("\t x = %03d, y = %03d acq = {%s} to {%s} epoch = {%d/%d} satellites = {%s} bands = %s ",
                  self.x_min, self.y_min, self.acq_min, self.acq_max, self.epoch.increment, self.epoch.duration,
                  " ".join(self.satellites), " ".join([b.name for b in self.bands]))
        _log.info("\t output directory = {%s} PQ apply = %s PQA mask = %s local scheduler = %s workers = %s",
                  self.output_directory, self.mask_pqa_apply, self.mask_pqa_apply and
                  " ".join([mask.name for mask in self.mask_pqa_mask]) or "", self.local_scheduler, self.workers)
        _log.info("\t dataset to retrieve %s dask chunk size = %d seasons = %s statistics = %s interpolation = %s",
                  self.dataset_type, self.chunk_size, " ".join([s.name for s in self.seasons]),
                  " ".join([s.name for s in self.statistics]), self.interpolation.name)

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
        return DATASET_TYPE_DATABASE + DATASET_TYPE_DERIVED_NBAR

    def get_seasons(self):
        for season in self.seasons:
            yield season

    def create_all_tasks(self):
        cells = (self.x_min, self.y_min)
        _log.info(" cell values  %s", cells)
        for ((acq_min, acq_max), season, band, statistic) in product(self.get_epochs(), self.get_seasons(),
                                                                     self.bands, self.statistics):
            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(self.acq_min, self.acq_max,
                                                                                      season,
                                                                                      extend=True)
            mindt = (int(criteria[0][0].strftime("%Y")), int(criteria[0][0].strftime("%m")),
                     int(criteria[0][0].strftime("%d")))
            maxdt = (int(criteria[0][1].strftime("%Y")), int(criteria[0][1].strftime("%m")),
                     int(criteria[0][1].strftime("%d")))
            _log.info("Creating task at %s for epoch stats %s %s %s %s %s crit min date %s , crit max date %s",
                      datetime.now(),
                      self.x_min, self.y_min, acq_min_extended, acq_max_extended, season, mindt, maxdt)
            yield self.create_new_task(x=self.x_min, y=self.y_min, acq_min=acq_min_extended, acq_max=acq_max_extended,
                                       season=season, dataset_type=self.dataset_type, band=band,
                                       mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                       chunk_size=self.chunk_size, statistic=statistic, statistics=self.statistics,
                                       interpolation=self.interpolation, evi_args=self.evi_args)

    # pylint: disable=too-many-arguments
    def create_new_task(self, x, y, acq_min, acq_max, season, dataset_type, band, mask_pqa_apply,
                        mask_pqa_mask, chunk_size, statistic, statistics, interpolation,
                        evi_args):
        return EpochStatisticsTask(x_cell=x, y_cell=y, acq_min=acq_min, acq_max=acq_max,
                                   season=season, satellites=self.satellites,
                                   dataset_type=dataset_type, band=band,
                                   mask_pqa_apply=mask_pqa_apply, mask_pqa_mask=mask_pqa_mask,
                                   chunk_size=chunk_size, statistic=statistic,
                                   statistics=statistics, interpolation=interpolation,
                                   output_directory=self.output_directory, evi_args=evi_args)

    def run(self):

        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()
        if self.local_scheduler:
            luigi.build(self.create_all_tasks(), local_scheduler=self.local_scheduler, workers=self.workers)
        else:
            import luigi.contrib.mpi as mpi
            mpi.run(self.create_all_tasks())


class EpochStatisticsTask(Task):     # pylint: disable=abstract-method
    x_cell = luigi.IntParameter()
    y_cell = luigi.IntParameter()
    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()
    season = luigi.Parameter()
    # epochs = luigi.Parameter(is_list=True, significant=False)
    satellites = luigi.Parameter()
    dataset_type = luigi.Parameter()
    band = luigi.Parameter()
    mask_pqa_apply = luigi.BoolParameter()
    mask_pqa_mask = luigi.Parameter()
    chunk_size = luigi.IntParameter()
    statistic = luigi.Parameter()
    statistics = luigi.Parameter()
    interpolation = luigi.Parameter()
    output_directory = luigi.Parameter()
    evi_args = luigi.FloatParameter()

    def output(self):

        season = SEASONS[self.season]
        acq_min = self.acq_min
        acq_max = self.acq_max
        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])
        sat = ",".join(self.satellites)
        filename = "{sat}_{prod}_{x:03d}_{y:03d}_{acq_min}_{acq_max}_{sea_st}_{sea_en}_{band}_{stat}.nc" \
                   .format(sat=sat, prod=str(self.dataset_type.name).upper(),
                           x=self.x_cell, y=self.y_cell, acq_min=acq_min, acq_max=acq_max, sea_st=season_start,
                           sea_en=season_end, band=self.band.name, stat=self.statistic.name)
        return luigi.LocalTarget(os.path.join
                                 (self.output_directory, filename))

    def apply_mask(self):

        ga_good_pixel = {name: False for name in
                         ('band_5_saturated',
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
        ga_good_pixel.update(dict(contiguity=True, land_obs=True))
        for mask in str(self.mask_pqa_mask):
            if mask.name == "PQ_MASK_CLEAR":
                # _log.info("applying mask for %s on %s", mask.name,  GA_GOOD_PIXEL)
                return ga_good_pixel
        # return GA_GOOD_PIXEL
        ga_pixel_bit = {name: True for name in
                        ('band_5_saturated',
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
        ga_pixel_bit.update(dict(contiguity=False, land_obs=False))

        for mask in str(self.mask_pqa_mask):
            if mask.name == "PQ_MASK_CONTIGUITY":
                ga_pixel_bit.update(dict(contiguity=True))
            if mask.name == "PQ_MASK_CLOUD_FMASK":
                ga_pixel_bit.update(dict(cloud_fmask=False))
            if mask.name == "PQ_MASK_CLOUD_ACCA":
                ga_pixel_bit.update(dict(cloud_acca=False))
            if mask.name == "PQ_MASK_CLOUD_SHADOW_ACCA":
                ga_pixel_bit.update(dict(cloud_shadow_acca=False))
            if mask.name == "PQ_MASK_SATURATION":
                ga_pixel_bit.update(dict(band_1_saturated=False, band_2_saturated=False, band_3_saturated=False,
                                         band_4_saturated=False, band_5_saturated=False, band_6_1_saturated=False,
                                         band_6_2_saturated=False, band_7_saturated=False))
            if mask.name == "PQ_MASK_SATURATION_OPTICAL":
                ga_pixel_bit.update(dict(band_1_saturated=False, band_2_saturated=False, band_3_saturated=False,
                                         band_4_saturated=False, band_5_saturated=False, band_7_saturated=False))
            if mask.name == "PQ_MASK_SATURATION_OPTICAL":
                ga_pixel_bit.update(dict(band_6_1_saturated=False, band_6_2_saturated=False))
            _log.info("applying bit mask %s on %s ", mask.name, ga_pixel_bit)

        return ga_pixel_bit

    def write_crs_attributes(self, index, storage_type, crs_spatial_ref):

        tile_index = (self.x_cell, self.y_cell)
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
        tile_index = (self.x_cell, self.y_cell)
        storage_det = index.storage.types.get_by_name(storage_type)
        geobox = GeoBox.from_storage_type(storage_det, tile_index)
        geo_extents = geobox.geographic_extent.to_crs('EPSG:4326').points
        geo_extents = geo_extents + [geo_extents[0]]
        geospatial_bounds = "POLYGON((" + ", ".join("{0} {1}".format(*p) for p in geo_extents) + "))"
        long_name = geobox.geographic_extent.crs.GetAttrValue('GEOGCS')
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
            'grid_mapping_name': geobox.crs.GetAttrValue('PROJECTION')
        }
        return extents

    def initialise_odata(self, dtype):
        shape = (4000, 4000)
        nbar = np.empty(shape, dtype=dtype)
        nbar.fill(NDV)
        return nbar

    def do_compute(self, data, odata, dtype):     # pylint: disable=too-many-branches

        _log.info("doing computations for %s on  %s of on odata shape %s",
                  self.statistic.name, datetime.now(), odata.shape)
        ndv = np.nan
        # pylint: disable=range-builtin-not-iterating
        for x_offset, y_offset in product(range(0, 4000, 4000),
                                          range(0, 4000, self.chunk_size)):
            if self.dataset_type.name == "TCI":
                stack = data[x_offset: 4000, y_offset: y_offset+self.chunk_size]
            else:
                stack = data.isel(x=slice(x_offset, 4000), y=slice(y_offset, y_offset+self.chunk_size)).load().data
            _log.info("stack stats shape %s for %s for (%03d ,%03d) x_offset %d for y_offset %d",
                      stack.shape, self.band.name, self.x_cell, self.y_cell, x_offset, y_offset)
            if self.statistic.name == "MIN":
                stack_stat = calculate_stack_statistic_min(stack=stack, ndv=ndv, dtype=dtype)
                # data = data.reduce(numpy.nanmin, axis=0)
                # odata = odata.min(axis=0, skipna='True')
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

            odata[y_offset:y_offset+self.chunk_size, x_offset:4000] = stack_stat
        _log.info("stats finished for (%03d, %03d) band %s on %s", self.x_cell, self.y_cell, self.band.name, odata)
        return odata

    def get_derive_data(self, data, pq, mask_clear):
        ndvi = None
        sat = ",".join(self.satellites)
        _log.info("getting derived data for %s for satellite %s", self.dataset_type.name, sat)
        if pq:
            data = data.where(mask_clear)
        if sat == "LANDSAT_8":
            blue = data.band_2
            green = data.band_3
            red = data.band_4
            nir = data.band_5
            sw1 = data.band_6
            sw2 = data.band_7
        else:
            blue = data.band_1
            green = data.band_2
            red = data.band_3
            nir = data.band_4
            sw1 = data.band_5
            sw2 = data.band_7
        if self.dataset_type.name == "NDFI":
            ndvi = (sw1 - nir) / (sw1 + nir)
            ndvi.name = "NDFI data"
        if self.dataset_type.name == "NDVI":
            ndvi = (nir - red) / (nir + red)
            ndvi.name = "NDVI data"
        if self.dataset_type.name == "NDWI":
            ndvi = (green - nir) / (green + nir)
            ndvi.name = "NDWI data"
        if self.dataset_type.name == "MNDWI":
            ndvi = (green - sw1) / (green + sw1)
            ndvi.name = "MNDWI data"
        if self.dataset_type.name == "NBR":
            ndvi = (nir - sw2) / (nir + sw2)
            ndvi.name = "NBR data"
        if self.dataset_type.name == "EVI":
            g, l, c1, c2 = self.evi_args      # pylint: disable=unpacking-non-sequence
            ndvi = g * ((nir - red) / (nir + c1 * red - c2 * blue + l))
            ndvi.name = "EVI data"
            _log.info("EVI cooefficients used are G=%f, l=%f, c1=%f, c2=%f", g, l, c1, c2)
        if self.dataset_type.name == "TCI":
            ndvi = calculate_tci(self.band.name, sat, blue, green, red, nir, sw1, sw2)
            _log.info(" shape of TCI array is %s", ndvi.shape)
        return ndvi

    def get_band_data(self, data):     # pylint: disable=too-many-branches
        sat = ",".join(self.satellites)
        if self.band.name == "BLUE":
            if sat == "LANDSAT_8":
                band_data = data.band_2
            else:
                band_data = data.band_1
        if self.band.name == "GREEN":
            if sat == "LANDSAT_8":
                band_data = data.band_3
            else:
                band_data = data.band_2
        if self.band.name == "RED":
            if sat == "LANDSAT_8":
                band_data = data.band_4
            else:
                band_data = data.band_3
        if self.band.name == "NEAR_INFRARED":
            if sat == "LANDSAT_8":
                band_data = data.band_5
            else:
                band_data = data.band_4
        if self.band.name == "SHORT_WAVE_INFRARED_1":
            if sat == "LANDSAT_8":
                band_data = data.band_6
            else:
                band_data = data.band_5
        if self.band.name == "SHORT_WAVE_INFRARED_2":
            if sat == "LANDSAT_8":
                band_data = data.band_7
            else:
                band_data = data.band_7
        return band_data

    def data_write(self, dtype):        # pylint: disable=too-many-branches,too-many-statements
        # pylint: disable=too-many-boolean-expressions,too-many-locals

        acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(self.acq_min, self.acq_max,
                                                                                  self.season,
                                                                                  extend=True)
        mindt = (int(criteria[0][0].strftime("%Y")), int(criteria[0][0].strftime("%m")),
                 int(criteria[0][0].strftime("%d")))
        maxdt = (int(criteria[0][1].strftime("%Y")), int(criteria[0][1].strftime("%m")),
                 int(criteria[0][1].strftime("%d")))
        index = index_connect()
        dc = datacube.api.API(index, app="stats-app")
        _log.info("\tcalling dataset for %3d %4d on band  %s stats  %s  in the date range  %s %s for satellite %s",
                  self.x_cell, self.y_cell, self.band.name, self.statistic.name, mindt, maxdt, self.satellites)
        pq = None
        prodname = (self.dataset_type.name).lower()
        if prodname == "ndvi" or prodname == "ndwi" or prodname == "ndfi" or prodname == "mndwi" or \
           prodname == "nbr" or prodname == "evi" or prodname == "tci":
            prodname = "nbar"
        data = dc.get_dataset_by_cell((self.x_cell, self.y_cell), product=prodname, platform=self.satellites,
                                      time=(mindt, maxdt))
        _log.info("\tshape of data received %s", data.band_2.shape)
        crs_spatial_ref = data.crs.spatial_ref
        storage_type = data.storage_type
        descriptor = self.write_geographical_extents_attributes(index, storage_type)
        stats_dataset = None
        mask_clear = None
        ga_good_pixel = {'band_5_saturated': False,
                         'band_6_1_saturated': False,
                         'cloud_shadow_acca': False,
                         'cloud_fmask': False,
                         'band_3_saturated': False,
                         'band_1_saturated': False,
                         'band_4_saturated': False,
                         'band_2_saturated': False,
                         'cloud_acca': False,
                         'band_6_2_saturated': False,
                         'cloud_shadow_fmask': False,
                         'band_7_saturated': False}
        ga_good_pixel.update(dict(contiguity=True, land_obs=True))
        if self.mask_pqa_apply:
            pq = dc.get_dataset_by_cell((self.x_cell, self.y_cell), product='pqa', platform=self.satellites,
                                        time=(mindt, maxdt))
            _log.info("\tpq dataset call completed for %3d %4d on band  %s stats  %s",
                      self.x_cell, self.y_cell, self.band.name, self.statistic.name)
            if pq:
                mask_clear = pq['pixelquality'] & 15871 == 15871
                # mask_clear = pq['pixelquality']  == 16383
                # _log.info("called pq dataset for pq data for band %s and length of pq data %d and %s",
                #            self.band.name, len(pq), pq['pixelquality'].time.values)
            else:
                _log.info("No pixel quality available")
        tci_data = None
        if self.band.name in [t.name for t in Ls57Arg25Bands]:
            band_data = self.get_band_data(data)
            if pq:
                data = band_data.where(mask_clear)
            else:
                data = band_data
            _log.info("Received band %s data is %s ", self.band.name, band_data)
            data = data.chunk(chunks=(self.chunk_size, self.chunk_size))
        elif self.dataset_type.name == "TCI":
            tci_data = self.get_derive_data(data, pq, mask_clear)
            data = data.band_1
        else:
            data = self.get_derive_data(data, pq, mask_clear)      # pylint: disable=redefined-variable-type
            _log.info("Received band %s data is %s ", self.band.name, data)

        # create a stats data variable
        stats_var = None
        odata = self.initialise_odata(dtype)
        if self.dataset_type.name == "TCI":
            odata = self.do_compute(tci_data, odata, dtype)
        else:
            odata = self.do_compute(data, odata, dtype)
        # variable name
        stats_var = str(self.statistic.name).lower()
        if stats_var == "standard_deviation":
            stats_var = "std_dev"
        if stats_var == "count_observed":
            stats_var = "count"
        stats_var = stats_var + "_" + self.acq_min.strftime("%Y")
        data = data.isel(time=0).drop('time')
        data.data = odata
        stats_dataset = data.to_dataset(name=stats_var)
        if self.band.name in [t.name for t in Ls57Arg25Bands]:
            stats_dataset.get(stats_var).attrs.update(dict(Comment1='Statistics calculated on ' +
                                                           band_data.attrs.get('long_name')))
            stats_dataset.attrs = band_data.attrs
        else:
            stats_dataset.get(stats_var).attrs.update(dict(Comment1='Statistics calculated on ' +
                                                           self.dataset_type.name + ' datasets'))
            if (self.dataset_type.name).lower() == "evi":
                stats_dataset.get(stats_var).attrs.update(dict(Comment='Parameters ' +
                                                               str(self.evi_args) +
                                                               ' for G,L,C1,C2 are used respectively'))
            if (self.dataset_type.name).lower() == "tci":
                stats_dataset.get(stats_var).attrs.update(dict(Comment='This is based on  ' +
                                                               self.band.name + ' algorithm'))
        if self.season:
            stats_dataset.get(stats_var).attrs.update(dict(long_name=str(self.statistic.name).lower() +
                                                           ' seasonal statistics for ' +
                                                           str(self.season.name).lower() + ' of ' +
                                                           str("_".join(self.satellites)).lower()))
            stats_dataset.get(stats_var).attrs.update(dict(standard_name=str(self.statistic.name).lower() +
                                                           '_' + str(self.season.name).lower() + '_season_' +
                                                           str("_".join(self.satellites)).lower()))
        else:
            stats_dataset.get(stats_var).attrs.update(dict(long_name=str(self.statistic.name).lower() +
                                                           ' statistics for ' +
                                                           str("_".join(self.satellites)).lower() +
                                                           ' and duration  ' + self.acq_min.strftime("%Y%mm%dd") + '-' +
                                                           self.acq_max.strftime("%Y%mm%dd")))
            stats_dataset.get(stats_var).attrs.update(dict(standard_name=str(self.statistic.name).lower() +
                                                           '_' + self.acq_min.strftime("%Y%mm%dd") + '-' +
                                                           self.acq_max.strftime("%Y%mm%dd") + '_' +
                                                           str("_".join(self.satellites)).lower()))
        if 'PERCENTILE' in self.statistic.name:
            stats_dataset.get(stats_var).attrs.update(dict(Comment2='Percentile method used ' +
                                                           self.interpolation.name))
        stats_dataset.get(stats_var).attrs.update(dict(units='metre', _FillValue='-999', grid_mapping="crs"))
        stats_dataset.attrs.update(dict(descriptor))
        crs_attr = self.write_crs_attributes(index, storage_type, crs_spatial_ref)
        crs_variable = {'crs': 0}
        # create crs variable
        stats_dataset = stats_dataset.assign(**crs_variable)
        # global attributes
        stats_dataset.crs.attrs = crs_attr
        _log.info("stats is ready for %s (%d, %d) for %s %s", self.dataset_type.name, self.x_cell, self.y_cell,
                  self.band.name, self.statistic.name)
        return stats_dataset, stats_var

    def coordinate_attr_update(self, stats_dataset):

        stats_dataset.get('x').attrs.update(dict(long_name="x coordinate of projection",
                                                 standard_name="projection_x_coordinate", axis="X"))
        stats_dataset.get('y').attrs.update(dict(long_name="y coordinate of projection",
                                                 standard_name="projection_y_coordinate", axis="Y"))

    def run(self):

        _log.info("Doing band [%s] statistic [%s] ", self.band.name, self.statistic.name)
        filename = self.output().path
        dtype = np.float32
        if self.band.name in [t.name for t in Ls57Arg25Bands] or str(self.statistic.name) == "COUNT_OBSERVED":
            dtype = np.int16
        stats_dataset, stats_var = self.data_write(dtype)
        # update x and y coordinates axis/name attributes to silence gdal warning like "Longitude/X dimension"
        self.coordinate_attr_update(stats_dataset)
        # stats_dataset.to_netcdf(filename, mode='w', format='NETCDF4', engine='netcdf4',
        #       encoding={stats_var:{'dtype': dtype, 'scale_factor': 0.1, 'add_offset': 5, 'zlib': True,
        #  '_FillValue':-999}})
        stats_dataset.to_netcdf(filename, mode='w', format='NETCDF4', engine='netcdf4',
                                encoding={stats_var: {'zlib': True}})

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    StatsTask().run()
