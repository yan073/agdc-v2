#!/usr/bin/env python

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

from dateutil.rrule import rrule, YEARLY
import dask
import numpy as np
import luigi
import luigi.contrib
from luigi.task import flatten
from utils_v1 import parse_date_min, parse_date_max, PqaMask, Statistic, writeable_dir
from utils_v1 import pqa_mask_arg, statistic_arg, season_arg
from model_v1 import Ls57Arg25Bands, Ls8Arg25Bands, NdviBands, NdfiBands, TciBands, Pq25Bands, Fc25Bands
from model_v1 import Wofs25Bands, NdwiBands, MndwiBands, EviBands, NbrBands, DsmBands
from model_v1 import DATASET_TYPE_DATABASE, DATASET_TYPE_DERIVED_NBAR, DatasetType
from utils_v1 import PercentileInterpolation, SEASONS
from utils_v1 import Season, NDV, build_season_date_criteria

import datacube.api
from dateutil.relativedelta import relativedelta
from datacube.api import GridWorkflow
from app_utils import product_lookup, write_crs_attributes, write_global_attributes
from app_utils import do_compute, config_loader
from app_utils import make_stats_config, stats_extra_metadata, fuse_data

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


class Task(luigi.Task):  # pylint: disable=metaclass-assignment
    """
       Luigi completion task
    """
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


class StatsRunner(object):  # pylint: disable=too-many-instance-attributes
    """
    Parse command line arguments then run a task to produce statistic output.

    Uses ``luigi`` and :class:`EpochStatsTask`.
    """

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
        self.period = None
        self.date_list = None

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
                                 default=[PqaMask.PQ_MASK_CLEAR_ELB],
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
                                 default=[Statistic.PERCENTILE_10, Statistic.PERCENTILE_50, Statistic.PERCENTILE_90],
                                 dest="statistic", type=statistic_arg, nargs="+",
                                 metavar=" ".join([s.name for s in Statistic]))
        self.parser.add_argument("--interpolation", help="The interpolation method to use", action="store",
                                 default=PercentileInterpolation.NEAREST,  # required=True,
                                 dest="interpolation", type=percentile_interpolation_arg,
                                 metavar=" ".join([s.name for s in PercentileInterpolation]))
        self.parser.add_argument("--season", help="The seasons for which to produce statistics", action="store",
                                 default=Season,  # required=True,
                                 dest="season", type=season_arg, nargs='+',
                                 metavar=" ".join([s.name for s in Season]))
        self.parser.add_argument("--evi-args", help="evi args(e.g. 2.5,1,6,7.5 for G,L,C1 and C2)",
                                 metavar="G,L,C1 and C2 2.5, 1, 6, 7.5",
                                 action="store", dest="evi_args", type=float, nargs=4, default=[2.5, 1, 6, 7.5])
        self.parser.add_argument("--period", help="day intervals", action="store", dest="period", type=str,
                                 default="1201-0204")
        self.parser.add_argument("--date-list", help="list of sat and acq dates",
                                 action="store",
                                 dest="date_list", type=str,
                                 nargs="+", default=['ls5/2008-01-01', 'ls8/2015-03-01'])

    def process_arguments(self, args):

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
        self.period = args.period
        self.date_list = args.date_list

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
        _log.info("\t dataset to retrieve %s dask chunk size=%d seasons=%s statistics=%s interpolation=%s dates are %s",
                  self.dataset_type, self.chunk_size, self.seasons,
                  " ".join([s.name for s in self.statistics]), self.interpolation.name, self.date_list)

    def get_epochs(self):

        for season in self.seasons:
            if season.name == "TIDAL":
                yield self.get_tidal_date_ranges()
                return
        for dt in rrule(YEARLY, interval=self.epoch.increment, dtstart=self.acq_min, until=self.acq_max):
            acq_min = dt.date()
            acq_max = acq_min + relativedelta(years=self.epoch.duration, days=-1)
            acq_min = max(self.acq_min, acq_min)
            acq_max = min(self.acq_max, acq_max)
            yield acq_min, acq_max

    def get_tidal_date_ranges(self):
        dt_rn = [vl.rsplit('/', 1)[1] for vl in self.date_list]
        dt_rn.sort(key=lambda date: datetime.strptime(date, '%Y-%m-%dT%H:%M:%S'))
        dt_rn = [datetime.strptime(date, "%Y-%m-%dT%H:%M:%S").date() for date in dt_rn]
        acq_min = dt_rn[0]
        acq_max = dt_rn[len(dt_rn) - 1]
        return acq_min, acq_max

    @staticmethod
    def get_supported_dataset_types():
        return DATASET_TYPE_DATABASE + DATASET_TYPE_DERIVED_NBAR

    def get_seasons(self):
        for season in self.seasons:
            yield season

    def create_all_tasks(self):
        cells = (self.x_min, self.y_min)
        _log.info(" cell values  %s", cells)
        for (acq_min, acq_max), season, band, statistic in product(self.get_epochs(), self.get_seasons(),
                                                                   self.bands, self.statistics):
            _log.info("epoch returns date min %s max %s", acq_min, acq_max)

            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(acq_min, acq_max,
                                                                                      season, extend=True)
            if season.name == "TIDAL":
                acq_min_extended = acq_min
                acq_max_extended = acq_max
            mindt = (int(acq_min_extended.strftime("%Y")), int(acq_min_extended.strftime("%m")),
                     int(acq_min_extended.strftime("%d")))
            maxdt = (int(acq_max_extended.strftime("%Y")), int(acq_max_extended.strftime("%m")),
                     int(acq_max_extended.strftime("%d")))
            _log.info("Creating task at %s for epoch stats %s %s %s %s %s crit min date %s , crit max date %s",
                      str(datetime.now()),
                      self.x_min, self.y_min, acq_min_extended, acq_max_extended, season, mindt, maxdt)

            yield EpochStatisticsTask(x_cell=self.x_min, y_cell=self.y_min, acq_min=acq_min_extended,
                                      acq_max=acq_max_extended, season=season, satellites=self.satellites,
                                      dataset_type=self.dataset_type, band=band,
                                      mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,
                                      chunk_size=self.chunk_size, statistic=statistic,
                                      statistics=self.statistics, interpolation=self.interpolation,
                                      evi_args=self.evi_args, period=self.period, date_list=self.date_list,
                                      output_directory=self.output_directory, )

    def run(self):
        self.setup_arguments()
        self.process_arguments(self.parser.parse_args())
        self.log_arguments()
        tasks = self.create_all_tasks()

        if self.local_scheduler:
            luigi.build(tasks, local_scheduler=self.local_scheduler, workers=self.workers)
        else:
            import luigi.contrib.mpi as mpi
            mpi.run(tasks)


def initialise_odata(dtype):
    shape = (4000, 4000)
    nbar = np.empty(shape, dtype=dtype)
    nbar.fill(NDV)
    return nbar


class EpochStatisticsTask(Task):  # pylint: disable=abstract-method
    """
    Main class to do stats for each epoch/season for each stats
    """
    x_cell = luigi.IntParameter()
    y_cell = luigi.IntParameter()
    acq_min = luigi.DateParameter()
    acq_max = luigi.DateParameter()
    season = luigi.Parameter()
    # epochs = luigi.Parameter(is_list=True, significant=False)
    satellites = luigi.Parameter()  # String or list of strings
    dataset_type = luigi.Parameter()
    band = luigi.Parameter()
    mask_pqa_apply = luigi.BoolParameter()
    mask_pqa_mask = luigi.Parameter()
    chunk_size = luigi.IntParameter()
    statistic = luigi.Parameter()  # It's a freaking Enum. Luigi has EnumParameter, maybe we should use it
    statistics = luigi.Parameter()
    interpolation = luigi.Parameter()
    output_directory = luigi.Parameter()
    evi_args = luigi.FloatParameter()
    period = luigi.Parameter()
    date_list = luigi.Parameter()

    def output(self):
        """
        Tell Luigi where to write the output
        """
        season = SEASONS[self.season]
        sat = "LS" + "".join([s[-1:] for s in list(self.satellites)])
        acq_min = self.acq_min
        acq_max = self.acq_max
        season_start = "{month}{day:02d}".format(month=season[0][0].name[:3], day=season[0][1])
        season_start += "_"
        season_end = "{month}{day:02d}".format(month=season[1][0].name[:3], day=season[1][1])
        if self.season.name == "TIDAL":
            season_start = 'TIDAL'
            season_end = ''
        sea = season_start + season_end
        filename = "{sat}_{prod}_{x:03d}_{y:03d}_{acq_min}_{acq_max}_{sea}_{band}_{stat}.nc" \
            .format(sat=sat, prod=str(self.dataset_type.name).upper(),
                    x=self.x_cell, y=self.y_cell, acq_min=acq_min, acq_max=acq_max, sea=sea,
                    band=self.band.name, stat=self.statistic.name)
        return luigi.LocalTarget(os.path.join
                                 (self.output_directory, filename))

    def get_stats(self, dc, prodname, dtype=np.float32):  # pylint: disable=too-many-branches,too-many-statements
        # pylint: disable=too-many-boolean-expressions,too-many-locals
        if self.season.name == "TIDAL":
            mindt = (int(self.acq_min.strftime("%Y")), int(self.acq_min.strftime("%m")),
                     int(self.acq_min.strftime("%d")), 0, 0, 0)
            maxdt = (int(self.acq_max.strftime("%Y")), int(self.acq_max.strftime("%m")),
                     int(self.acq_max.strftime("%d")), 23, 59, 59)
        else:
            acq_min_extended, acq_max_extended, criteria = build_season_date_criteria(self.acq_min, self.acq_max,
                                                                                      self.season,
                                                                                      extend=True)

            mindt = (int(criteria[0][0].strftime("%Y")), int(criteria[0][0].strftime("%m")),
                     int(criteria[0][0].strftime("%d")), 0, 0, 0)
            maxdt = (int(criteria[0][1].strftime("%Y")), int(criteria[0][1].strftime("%m")),
                     int(criteria[0][1].strftime("%d")), 23, 59, 59)

        gw = GridWorkflow(index=dc.index, product=prodname)
        data, ls_stack, cell_list_obj, origattr = fuse_data(self, gw, dtype, mindt, maxdt)

        # create a stats data variable
        odata = initialise_odata(dtype)
        odata = do_compute(self, ls_stack, odata, dtype)

        _log.info("Received calculated data %s ", odata)

        # variable name
        stats_var = str(self.statistic.name).lower()
        if stats_var == "standard_deviation":
            stats_var = "std_dev"
        if stats_var == "count_observed":
            stats_var = "count"
        stats_var = stats_var + "_" + self.acq_min.strftime("%Y")

        if self.season.name == "TIDAL":
            data = data.isel(points=0).drop('solar_day')
            data_arr = data.blue
        else:
            data = data.isel(solar_day=0).drop('solar_day')
            data_arr = data
        data_arr.data = odata
        stats_dataset = data_arr.to_dataset(name=stats_var)

        stats_var_attrs = stats_dataset.get(stats_var).attrs
        stats_var_attrs.clear()

        self.update_variable_attrs(stats_var_attrs)

        _log.info("stats is ready for %s (%d, %d) for %s %s", self.dataset_type.name, self.x_cell, self.y_cell,
                  self.band.name, self.statistic.name)
        return stats_dataset, stats_var, cell_list_obj, origattr

    def update_variable_attrs(self, stats_var_attrs):
        if self.band.name in [t.name for t in Ls57Arg25Bands]:
            stats_var_attrs['Comment1'] = 'Statistics calculated on ' + self.band.name + 'for ' + self.dataset_type.name
        else:
            stats_var_attrs['Comment1'] = 'Statistics calculated on ' + self.dataset_type.name + ' datasets'
            if self.dataset_type.name.lower() == "evi":
                stats_var_attrs['Comment'] = 'Parameters ' + str(self.evi_args) + ' for G,L,C1,C2 are used respectively'
            if self.dataset_type.name.lower() == "tci":
                stats_var_attrs['Comment'] = 'This is based on  ' + self.band.name + ' algorithm'
        if self.season:
            stats_var_attrs['long_name'] = (str(self.statistic.name).lower() +
                                            ' seasonal statistics for ' +
                                            str(self.season.name).lower() + ' of ' +
                                            str("_".join(self.satellites)).lower())
            stats_var_attrs['standard_name'] = (str(self.statistic.name).lower() +
                                                '_' + str(self.season.name).lower() + '_season_' +
                                                str("_".join(self.satellites)).lower())
        else:
            stats_var_attrs['long_name'] = (str(self.statistic.name).lower() +
                                            ' statistics for ' +
                                            str("_".join(self.satellites)).lower() +
                                            ' and duration  ' + self.acq_min.strftime("%Y%mm%dd") + '-' +
                                            self.acq_max.strftime("%Y%mm%dd"))
            stats_var_attrs['standard_name'] = (str(self.statistic.name).lower() +
                                                '_' + self.acq_min.strftime("%Y%mm%dd") + '-' +
                                                self.acq_max.strftime("%Y%mm%dd") + '_' +
                                                str("_".join(self.satellites)).lower())
        if 'PERCENTILE' in self.statistic.name:
            stats_var_attrs['Comment2'] = 'Percentile method used ' + self.interpolation.name

    def run(self):  # pylint: disable=too-many-locals
        dc = datacube.Datacube(app="stats-app")
        prodname = product_lookup(self.satellites[0], self.dataset_type.value.lower())
        if prodname is None:
            prodname = product_lookup(self.satellites[0], 'nbar')
        _log.info("Doing band [%s] statistic [%s] for product [%s] ", self.band.name, self.statistic.name, prodname)

        app_config_file = '../config/' + prodname + '.yaml'
        config = None
        if os.path.exists(app_config_file):
            config = config_loader(dc.index, app_config_file)

        filename = self.output().path

        if str(self.statistic.name) == "COUNT_OBSERVED" or self.band.name in [t.name for t in Ls57Arg25Bands]:
            dtype = np.int16
        else:
            dtype = np.float32

        stats_dataset, stats_var, cell_list_obj, origattr = self.get_stats(dc, prodname, dtype=dtype)
        if stats_dataset:
            if config:
                # keep original attribute from data
                stats_dataset.get(stats_var).attrs.update(dict(crs=origattr))
                stats_dataset.attrs = origattr

                config = make_stats_config(dc.index, config)
                _, flname = filename.rsplit('/', 1)
                # write with extra metadata and index to the database
                stats_extra_metadata(config, stats_dataset, cell_list_obj, flname)
            else:
                # This can be used for internal testing without database ingestion
                coordinate_attr_update(stats_dataset)
                descriptor = write_global_attributes(self, cell_list_obj['geobox'])
                stats_dataset.attrs.update(dict(descriptor))
                stats_dataset.get(stats_var).attrs.update(dict(_FillValue='-999', grid_mapping="crs"))

                crs_variable = {'crs': ''}
                # create crs variable
                stats_dataset = stats_dataset.assign(**crs_variable)
                stats_dataset.crs.attrs = write_crs_attributes(cell_list_obj['geobox'])
                # global attributes
                stats_dataset.to_netcdf(filename, mode='w', format='NETCDF4', engine='netcdf4',
                                        encoding={stats_var: {'zlib': True}})


def coordinate_attr_update(stats_dataset):
    """
    update x and y coordinates axis/name attributes to silence gdal warning like "Longitude/X dimension"
    """
    stats_dataset.get('x').attrs.update(dict(long_name="x coordinate of projection",
                                             standard_name="projection_x_coordinate", axis="X"))
    stats_dataset.get('y').attrs.update(dict(long_name="y coordinate of projection",
                                             standard_name="projection_y_coordinate", axis="Y"))


if __name__ == '__main__':
    '''
     This program can be used to calculate MIN,MAX,MEAN,MEDIAN,VARIANCE,STD,COUNT_OBSERVED and any PERCENTILE.
     It will apply pixel quality if the flag is set and also apply solar_day function. So that there would be no
     overlap of tiles collected on same date. The program supports multiple satellites datasets.
     It can support to ingest product into database by providing config yml file.

     To calculate generic stats pass the arguments of your choice.
     Default mask is PQ_MASK_CLEAR_ELB ie exclude land bit and apply all other bits and default is no mask apply
     unless flag --mask-pqa-apply is set.
     To calculate generic stats like -

     stats_for_all.py --x-min 3 --y-min -20  --output-directory "$ODIR" --season WINTER
     --satellite LANDSAT_5 LANDSAT_7 LANDSAT_8 --dataset-type nbar --band NDVI --chunk-size 250 --statistic MEDIAN MAX
     --epoch 1 1 --acq-min 2005 --acq-max 2007  --local-scheduler --mask-pqa-apply --workers 2 --interpolation NEAREST

     For TIDAL WORKFLOW pass a list of UTC dates and satellite name to calculate stats
     To run this workflow, tide heights, cell and date file is required. Needs to run OTPS module and get this output
     file ready. Then collect satellite name and utc dates(upto seconds) and pass onto this workflow.
     - For ex.

     stats_for_all.py --x-min -2 --y-min -14  --output-directory "$ODIR" --season TIDAL
     --satellite LANDSAT_5 LANDSAT_7 LANDSAT_8 --dataset-type nbar --band NDWI --chunk-size 250 --statistic MEDIAN
     --epoch 1 1 --acq-min 1986 --acq-max 2016  --local-scheduler --mask-pqa-apply --workers 1 --interpolation NEAREST
     --date-list ls7/2005-04-21T01:12:43 ls5/2005-02-24T01:09:30 ls8/2013-03-23T01:25:30
    '''
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    StatsRunner().run()
