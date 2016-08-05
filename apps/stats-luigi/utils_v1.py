"""
     This program is cut out, modified and reformatted. It is now compliant to PEP8.
     The original program was written by "Simon Oldfield"
     __author__ = 'u81051'
"""
from __future__ import absolute_import
import argparse
import logging
import os
from datetime import datetime, date
from collections import namedtuple
import calendar
import numpy
from dateutil.relativedelta import relativedelta
from enum import Enum
from model_v1 import get_bands, Satellite, DatasetType


_log = logging.getLogger(__name__)

_log.setLevel(logging.DEBUG)

# gdal.SetCacheMax(1024*1024*1024)


# Define PQ mask
#   This represents bits 0-13 set which means:
#       -  0 = band 10 not saturated
#       -  1 = band 20 not saturated
#       -  2 = band 30 not saturated
#       -  3 = band 40 not saturated
#       -  4 = band 50 not saturated
#       -  5 = band 61 not saturated
#       -  6 = band 62 not saturated
#       -  7 = band 70 not saturated
#       -  8 = contiguity ok (i.e. all bands present)
#       -  9 = land (not sea)
#       - 10 = not cloud (ACCA test)
#       - 11 = not cloud (FMASK test)
#       - 12 = not cloud shadow (ACCA test)
#       - 13 = not cloud shadow (FMASK test)


class PqaMask(Enum):
    __order__ = "PQ_MASK_CLEAR PQ_MASK_SATURATION PQ_MASK_SATURATION_OPTICAL PQ_MASK_SATURATION_THERMAL \
                PQ_MASK_CONTIGUITY PQ_MASK_LAND PQ_MASK_CLOUD PQ_MASK_CLOUD_ACCA PQ_MASK_CLOUD_FMASK \
                PQ_MASK_CLOUD_SHADOW_ACCA PQ_MASK_CLOUD_SHADOW_FMASK"
    PQ_MASK_CLEAR = 16383               # bits 0 - 13 set
    PQ_MASK_CLEAR_ELB = 15871           # Exclude land bit
    PQ_MASK_SATURATION = 255            # bits 0 - 7 set
    PQ_MASK_SATURATION_OPTICAL = 159    # bits 0-4 and 7 set
    PQ_MASK_SATURATION_THERMAL = 96     # bits 5,6 set
    PQ_MASK_CONTIGUITY = 256            # bit 8 set
    PQ_MASK_LAND = 512                  # bit 9 set
    PQ_MASK_CLOUD = 15360               # bits 10-13
    PQ_MASK_CLOUD_ACCA = 1024           # bit 10 set
    PQ_MASK_CLOUD_FMASK = 2048          # bit 11 set
    PQ_MASK_CLOUD_SHADOW_ACCA = 4096    # bit 12 set
    PQ_MASK_CLOUD_SHADOW_FMASK = 8192   # bit 13 set


class WofsMask(Enum):
    DRY = 0
    NO_DATA = 1
    SATURATION_CONTIGUITY = 2
    SEA_WATER = 4
    TERRAIN_SHADOW = 8
    HIGH_SLOPE = 16
    CLOUD_SHADOW = 32
    CLOUD = 64
    WET = 128


class OutputFormat(Enum):
    __order__ = "GEOTIFF ENVI"

    GEOTIFF = "GTiff"
    ENVI = "ENVI"


# Standard no data value
NDV = -999

INT16_MIN = numpy.iinfo(numpy.int16).min
INT16_MAX = numpy.iinfo(numpy.int16).max

UINT16_MIN = numpy.iinfo(numpy.uint16).min
UINT16_MAX = numpy.iinfo(numpy.uint16).max

BYTE_MIN = numpy.iinfo(numpy.ubyte).min
BYTE_MAX = numpy.iinfo(numpy.ubyte).max

NAN = numpy.nan


class PercentileInterpolation(Enum):
    __order__ = "LINEAR LOWER HIGHER NEAREST MIDPOINT"

    LINEAR = "linear"
    LOWER = "lower"
    HIGHER = "higher"
    NEAREST = "nearest"
    MIDPOINT = "midpoint"


def empty_array(shape, dtype=numpy.int16, fill=NDV):

    """
    Return an empty (i.e. filled with the no data value) array of the given shape and data type

    :param shape: shape of the array
    :param dtype: data type of the array (defaults to int32)
    :param fill: no data value (defaults to -999)
    :return: array
    """

    if fill == 0:
        a = numpy.zeros(shape=shape, dtype=dtype)
    else:
        a = numpy.empty(shape=shape, dtype=dtype)
        a.fill(fill)
    return a

DEFAULT_MASK_PQA = [PqaMask.PQ_MASK_CLEAR]


def apply_mask(data, mask, ndv=NDV):
    return numpy.ma.array(data, mask=mask).filled(ndv)

DEFAULT_MASK_WOFS = [WofsMask.WET]


class TasselCapIndex(Enum):
    __order__ = "BRIGHTNESS GREENNESS WETNESS FOURTH FIFTH SIXTH"

    BRIGHTNESS = 1
    GREENNESS = 2
    WETNESS = 3
    FOURTH = 4
    FIFTH = 5
    SIXTH = 6


def intersection(a, b):
    return list(set(a) & set(b))


def union(a, b):
    return list(set(a) | set(b))


def subset(a, b):
    return set(a) <= set(b)


def maskify_stack(stack, ndv=NDV):
    if numpy.isnan(ndv):
        return numpy.ma.masked_invalid(stack, copy=False)
    return numpy.ma.masked_equal(stack, ndv, copy=False)


def calculate_stack_statistic_count(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)
    stack_depth, stack_size_y, stack_size_x = numpy.shape(stack)
    stat = empty_array((stack_size_y, stack_size_x), dtype=dtype, fill=stack_depth)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("count is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_count_observed(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)
    stat = stack.count(axis=0)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("count observed is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_min(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)
    stat = numpy.min(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("min is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_max(stack, ndv=NDV, dtype=numpy.int16):

    stack = maskify_stack(stack=stack, ndv=ndv)
    stat = numpy.max(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("max is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_mean(stack, ndv=NDV, dtype=numpy.int16):

    if numpy.isnan(ndv):
        stat = numpy.nanmean(stack, axis=0)
    else:
        stack = maskify_stack(stack=stack, ndv=ndv)
        stat = numpy.mean(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("mean is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_variance(stack, ndv=NDV, dtype=numpy.int16):

    if numpy.isnan(ndv):
        stat = numpy.nanvar(stack, axis=0)
    else:
        stack = maskify_stack(stack=stack, ndv=ndv)
        stat = numpy.var(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("var is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_standard_deviation(stack, ndv=NDV, dtype=numpy.int16):

    if numpy.isnan(ndv):
        stat = numpy.nanstd(stack, axis=0)
    else:
        stack = maskify_stack(stack=stack, ndv=ndv)
        stat = numpy.std(stack, axis=0).filled(ndv)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    _log.debug("std is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_median(stack, ndv=NDV, dtype=numpy.int16):
    if numpy.isnan(ndv):
        stat = numpy.nanmedian(stack, axis=0)
    else:
        # stack = maskify_stack(stack=stack, ndv=ndv)
        m = numpy.ma.masked_equal(stack, ndv)
        stat = numpy.ma.median(m, axis=0).filled(0)
        # stat = numpy.median(stack, axis=0).filled(0)
    stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False)
    # _log.debug("median value is [%s]\n%s", numpy.shape(stat), stat)
    return stat


def calculate_stack_statistic_percentile(stack, percentile, interpolation=PercentileInterpolation.NEAREST,
                                         ndv=NDV, dtype=numpy.int16):

    # stack = maskify_stack(stack=stack, ndv=ndv)
    #
    # # numpy (1.9.2) currently doesn't have masked version of percentile so convert to float and use nanpercentile
    #
    # stack = numpy.ndarray.astype(stack, dtype=numpy.float16, copy=False).filled(numpy.nan)
    #
    # stat = numpy.nanpercentile(stack, percentile, axis=0, interpolation=interpolation.value)
    # stat = numpy.ma.masked_invalid(stat, copy=False)
    # stat = numpy.ndarray.astype(stat, dtype=dtype, copy=False).filled(ndv)
    #
    # _log.debug("max is [%s]\n%s", numpy.shape(stat), stat)
    #
    # return stat
    def do_percentile(data):
        d = data[data != ndv]
        # numpy.percentile has a hissy if the array is empty - aka ALL no data...
        if d.size == 0:
            return ndv
        else:
            return numpy.percentile(a=d, q=percentile, interpolation=interpolation.value)
    if numpy.isnan(ndv):
        stat = numpy.nanpercentile(a=stack, q=percentile, axis=0, interpolation=interpolation.value)
    else:
        stat = numpy.apply_along_axis(do_percentile, axis=0, arr=stack)
    _log.debug("%s is [%s]\n%s", percentile, numpy.shape(stat), stat)
    return stat


class Month(Enum):
    __order__ = "JANUARY FEBRUARY MARCH APRIL MAY JUNE JULY AUGUST SEPTEMBER OCTOBER NOVEMBER DECEMBER"

    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12


class Season(Enum):
    __order__ = "SPRING SUMMER AUTUMN WINTER CALENDAR_YEAR FINANCIAL_YEAR APR_TO_SEP QTR_1 QTR_2 QTR_3 QTR_4 TIDAL DUMMY"

    SPRING = "SPRING"
    SUMMER = "SUMMER"
    AUTUMN = "AUTUMN"
    WINTER = "WINTER"
    CALENDAR_YEAR = "CALENDAR_YEAR"
    FINANCIAL_YEAR = "FINANCIAL_YEAR"
    APR_TO_SEP = "APR_TO_SEP"
    APR_TO_OCT = "APR_TO_OCT"
    QTR_1 = "JAN_MAR"
    QTR_2 = "APR_JUN"
    QTR_3 = "JUL_SEP"
    QTR_4 = "OCT_DEC"
    TIDAL = "TIDAL"
    DUMMY = "DUMMY"


class Quarter(Enum):
    __order__ = "QTR_1 QTR_2 QTR_3 QTR_4"

    QTR_1 = "QTR_1"
    QTR_2 = "QTR_2"
    QTR_3 = "QTR_3"
    QTR_4 = "QTR_4"


SEASONS = {
    Season.SUMMER: ((Month.DECEMBER, 1), (Month.FEBRUARY, 28)),
    Season.AUTUMN: ((Month.MARCH, 1), (Month.MAY, 31)),
    Season.WINTER: ((Month.JUNE, 1), (Month.AUGUST, 31)),
    Season.SPRING: ((Month.SEPTEMBER, 1), (Month.NOVEMBER, 30)),
    Season.FINANCIAL_YEAR: ((Month.JULY, 1), (Month.JUNE, 30)),
    Season.CALENDAR_YEAR: ((Month.JANUARY, 1), (Month.DECEMBER, 31)),
    Season.APR_TO_SEP: ((Month.APRIL, 1), (Month.SEPTEMBER, 30)),
    Season.APR_TO_OCT: ((Month.APRIL, 1), (Month.OCTOBER, 31)),
    Season.QTR_1: ((Month.JANUARY, 1), (Month.MARCH, 31)),
    Season.QTR_2: ((Month.APRIL, 1), (Month.JUNE, 30)),
    Season.QTR_3: ((Month.JULY, 1), (Month.SEPTEMBER, 30)),
    Season.QTR_4: ((Month.OCTOBER, 1), (Month.DECEMBER, 31)),
    Season.TIDAL: ((Month.JANUARY, 1), (Month.JANUARY, 1)),
    Season.DUMMY: ((Month.DECEMBER, 1), (Month.DECEMBER, 31))
}


SatelliteDateCriteria = namedtuple("SatelliteDateCriteria", "satellite acq_min acq_max")

LS7_SLC_OFF_ACQ_MIN = date(2005, 5, 31)
LS7_SLC_OFF_ACQ_MAX = None

LS7_SLC_OFF_EXCLUSION = SatelliteDateCriteria(satellite=Satellite.LS7,
                                              acq_min=LS7_SLC_OFF_ACQ_MIN, acq_max=LS7_SLC_OFF_ACQ_MAX)

LS8_PRE_WRS_2_ACQ_MIN = None
LS8_PRE_WRS_2_ACQ_MAX = date(2013, 4, 10)

LS8_PRE_WRS_2_EXCLUSION = SatelliteDateCriteria(satellite=Satellite.LS8,
                                                acq_min=LS8_PRE_WRS_2_ACQ_MIN, acq_max=LS8_PRE_WRS_2_ACQ_MAX)

DateCriteria = namedtuple("DateCriteria", "acq_min acq_max")


def build_season_date_criteria(acq_min, acq_max, season, extend=True):

    seasons = SEASONS
    (month_start, day_start), (month_end, day_end) = seasons[season]
    if season.name == 'DUMMY':
        month_start = Month(acq_min.month)
        month_end = Month(acq_max.month)
        day_start = acq_min.day
        day_end = acq_max.day
    return build_date_criteria(acq_min, acq_max, month_start, day_start, month_end, day_end, extend=extend)


def build_date_criteria(acq_min, acq_max, month_start, day_start, month_end, day_end, extend=True):

    date_criteria = []

    for year in range(acq_min.year, acq_max.year+1):

        min_dt = date(year, month_start.value, 1) + relativedelta(day=day_start)
        max_dt = date(year, month_end.value, 1) + relativedelta(day=day_end)

        if min_dt > max_dt:
            max_dt = date(year+1, month_end.value, 1) + relativedelta(day=day_end)

        date_criteria.append(DateCriteria(min_dt, max_dt))

        if extend and acq_max < max_dt:
            acq_max = max_dt
    return acq_min, acq_max, date_criteria


def writeable_dir(prospective_dir):
    if not os.path.exists(prospective_dir):
        raise argparse.ArgumentTypeError("{0} doesn't exist".format(prospective_dir))
    if not os.path.isdir(prospective_dir):
        raise argparse.ArgumentTypeError("{0} is not a directory".format(prospective_dir))
    if not os.access(prospective_dir, os.W_OK):
        raise argparse.ArgumentTypeError("{0} is not writeable".format(prospective_dir))
    return prospective_dir


def pqa_mask_arg(s):
    if s in [m.name for m in PqaMask]:
        return PqaMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported PQA mask".format(s))


def wofs_mask_arg(s):
    if s in [m.name for m in WofsMask]:
        return WofsMask[s]
    raise argparse.ArgumentTypeError("{0} is not a supported WOFS mask".format(s))


def satellite_arg(s):
    if s in [sat.name for sat in Satellite]:
        return Satellite[s]
    raise argparse.ArgumentTypeError("{0} is not a supported satellite".format(s))


def parse_date_min(s):
    if s:
        if len(s) == len("YYYY"):
            return datetime.strptime(s, "%Y").date()

        elif len(s) == len("YYYY-MM"):
            return datetime.strptime(s, "%Y-%m").date()

        elif len(s) == len("YYYY-MM-DD"):
            return datetime.strptime(s, "%Y-%m-%d").date()
    return None


def parse_date_max(s):
    if s:
        if len(s) == len("YYYY"):
            d = datetime.strptime(s, "%Y").date()
            d = d.replace(month=12, day=31)
            return d

        elif len(s) == len("YYYY-MM"):
            d = datetime.strptime(s, "%Y-%m").date()

            first, last = calendar.monthrange(d.year, d.month)
            d = d.replace(day=last)
            return d

        elif len(s) == len("YYYY-MM-DD"):
            d = datetime.strptime(s, "%Y-%m-%d").date()
            return d
    return None


class Statistic(Enum):
    __order__ = "COUNT COUNT_OBSERVED MIN MAX MEAN SUM STANDARD_DEVIATION VARIANCE MEDIAN GEOMEDIAN PERCENTILE_5 \
                PERCENTILE_10 PERCENTILE_25 PERCENTILE_50 PERCENTILE_75 PERCENTILE_90 PERCENTILE_95"

    COUNT = "COUNT"
    COUNT_OBSERVED = "COUNT_OBSERVED"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"
    SUM = "SUM"
    STANDARD_DEVIATION = "STANDARD_DEVIATION"
    VARIANCE = "VARIANCE"
    MEDIAN = "MEDIAN"
    GEOMEDIAN = "GEOMEDIAN"
    PERCENTILE_5 = "PERCENTILE_5"
    PERCENTILE_10 = "PERCENTILE_10"
    PERCENTILE_25 = "PERCENTILE_25"
    PERCENTILE_50 = "PERCENTILE_50"
    PERCENTILE_75 = "PERCENTILE_75"
    PERCENTILE_90 = "PERCENTILE_90"
    PERCENTILE_95 = "PERCENTILE_95"


PERCENTILE = {
    Statistic.PERCENTILE_5:  5,
    Statistic.PERCENTILE_10: 10,
    Statistic.PERCENTILE_25: 25,
    Statistic.PERCENTILE_50: 50,
    Statistic.PERCENTILE_75: 75,
    Statistic.PERCENTILE_90: 90,
    Statistic.PERCENTILE_95: 95
}


def dataset_type_arg(s):
    if s in [t.name for t in DatasetType]:
        return DatasetType[s]
    raise argparse.ArgumentTypeError("{0} is not a supported dataset type".format(s))


def statistic_arg(s):
    if s in [t.name for t in Statistic]:
        return Statistic[s]
    raise argparse.ArgumentTypeError("{0} is not a supported statistic".format(s))


def season_arg(s):
    if s in [t.name for t in Season]:
        return Season[s]
    raise argparse.ArgumentTypeError("{0} is not a supported season".format(s))
