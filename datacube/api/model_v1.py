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

'''
     This program is cut out, modified and reformatted. It is now compliant to PEP8.
     The original program was written by "Simon Oldfield"
     __author__ = 'u81051'
'''

from __future__ import absolute_import
import logging
import os
from enum import Enum


_log = logging.getLogger(__name__)


class Satellite(Enum):
    __order__ = "LS5 LS7 LS8"

    LS5 = "LS5"
    LS7 = "LS7"
    LS8 = "LS8"


class Ls5TmBands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 THERMAL_INFRAFRED SHORT_WAVE_INFRARED_2"

    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    THERMAL_INFRAFRED = 6
    SHORT_WAVE_INFRARED_2 = 7


class Ls7EtmBands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 THERMAL_INFRAFRED SHORT_WAVE_INFRARED_2 \
                PANACHROMATIC"

    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    THERMAL_INFRAFRED = 6
    SHORT_WAVE_INFRARED_2 = 7
    PANACHROMATIC = 8


class Ls8OLiTirsBands(Enum):
    __order__ = "COASTAL_AEROSOL BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2 \
                PANACHROMATIC CIRRUS TIRS_1 TIRS_2"

    COASTAL_AEROSOL = 1
    BLUE = 2
    GREEN = 3
    RED = 4
    NEAR_INFRARED = 5
    SHORT_WAVE_INFRARED_1 = 6
    SHORT_WAVE_INFRARED_2 = 7
    PANACHROMATIC = 8
    CIRRUS = 9
    TIRS_1 = 10
    TIRS_2 = 11


class Ls57Arg25Bands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"

    BLUE = 1
    GREEN = 2
    RED = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    SHORT_WAVE_INFRARED_2 = 6


class Ls8Arg25Bands(Enum):
    __order__ = "COASTAL_AEROSOL BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"

    COASTAL_AEROSOL = 1
    BLUE = 2
    GREEN = 3
    RED = 4
    NEAR_INFRARED = 5
    SHORT_WAVE_INFRARED_1 = 6
    SHORT_WAVE_INFRARED_2 = 7


class Pq25Bands(Enum):
    __order__ = "PQ"

    PQ = 1          # pylint: disable=invalid-name


class Fc25Bands(Enum):
    __order__ = "PHOTOSYNTHETIC_VEGETATION NON_PHOTOSYNTHETIC_VEGETATION BARE_SOIL UNMIXING_ERROR"

    PHOTOSYNTHETIC_VEGETATION = 1
    NON_PHOTOSYNTHETIC_VEGETATION = 2
    BARE_SOIL = 3
    UNMIXING_ERROR = 4


class Wofs25Bands(Enum):
    __order__ = "WATER"

    WATER = 1


class NdviBands(Enum):
    __order__ = "NDVI"

    NDVI = 1


class NdfiBands(Enum):
    __order__ = "NDFI"

    NDFI = 1


class NdwiBands(Enum):
    __order__ = "NDWI"

    NDWI = 1


class MndwiBands(Enum):
    __order__ = "MNDWI"

    MNDWI = 1


class EviBands(Enum):
    __order__ = "EVI"

    EVI = 1


class NbrBands(Enum):
    __order__ = "NBR"

    NBR = 1


# TODO - duplication with TasselCapIndex!!!!

class TciBands(Enum):
    __order__ = "BRIGHTNESS GREENNESS WETNESS FOURTH FIFTH SIXTH"

    BRIGHTNESS = 1
    GREENNESS = 2
    WETNESS = 3
    FOURTH = 4
    FIFTH = 5
    SIXTH = 6


class DsmBands(Enum):
    __order__ = "ELEVATION SLOPE ASPECT"

    ELEVATION = 1
    SLOPE = 2
    ASPECT = 3


class DatasetType(Enum):
    __order__ = "nbar nbart pqa ortho FC25 DSM DEM DEM_SMOOTHED DEM_HYDROLOGICALLY_ENFORCED WATER NDVI NDFI EVI \
                SAVI TCI NBR"

    nbar = "nbar"
    nbart = "nbart"
    pqa = "pqa"
    ortho = "ortho"
    FC25 = "FC25"
    DSM = "DSM"
    DEM = "DEM"
    DEM_SMOOTHED = "DEM_SMOOTHED"
    DEM_HYDROLOGICALLY_ENFORCED = "DEM_HYDROLOGICALLY_ENFORCED"
    WATER = "WATER"
    NDVI = "NDVI"
    NDFI = "NDFI"
    EVI = "EVI"
    SAVI = "SAVI"
    TCI = "TCI"
    NBR = "NBR"
    NDWI = "NDWI"
    MNDWI = "MNDWI"


DATASET_TYPE_DATABASE = [DatasetType.nbar, DatasetType.nbart, DatasetType.pqa, DatasetType.ortho, DatasetType.FC25,
                         DatasetType.WATER,
                         DatasetType.DSM,
                         DatasetType.DEM, DatasetType.DEM_HYDROLOGICALLY_ENFORCED, DatasetType.DEM_SMOOTHED]
DATASET_TYPE_DERIVED_NBAR = [DatasetType.NDVI, DatasetType.NDFI, DatasetType.EVI, DatasetType.NBR,
                             DatasetType.TCI, DatasetType.NDWI, DatasetType.MNDWI]


BANDS = {
    (DatasetType.nbar, Satellite.LS5): Ls57Arg25Bands,
    (DatasetType.nbar, Satellite.LS7): Ls57Arg25Bands,
    (DatasetType.nbar, Satellite.LS8): Ls8Arg25Bands,

    (DatasetType.nbart, Satellite.LS5): Ls57Arg25Bands,
    (DatasetType.nbart, Satellite.LS7): Ls57Arg25Bands,
    (DatasetType.nbart, Satellite.LS8): Ls8Arg25Bands,

    (DatasetType.ortho, Satellite.LS5): Ls57Arg25Bands,
    (DatasetType.ortho, Satellite.LS7): Ls57Arg25Bands,
    (DatasetType.ortho, Satellite.LS8): Ls8Arg25Bands,

    (DatasetType.pqa, Satellite.LS5): Pq25Bands,
    (DatasetType.pqa, Satellite.LS7): Pq25Bands,
    (DatasetType.pqa, Satellite.LS8): Pq25Bands,

    (DatasetType.FC25, Satellite.LS5): Fc25Bands,
    (DatasetType.FC25, Satellite.LS7): Fc25Bands,
    (DatasetType.FC25, Satellite.LS8): Fc25Bands,

    (DatasetType.WATER, Satellite.LS5): Wofs25Bands,
    (DatasetType.WATER, Satellite.LS7): Wofs25Bands,
    (DatasetType.WATER, Satellite.LS8): Wofs25Bands,

    (DatasetType.NDVI, Satellite.LS5): NdviBands,
    (DatasetType.NDVI, Satellite.LS7): NdviBands,
    (DatasetType.NDVI, Satellite.LS8): NdviBands,

    (DatasetType.NDFI, Satellite.LS5): NdfiBands,
    (DatasetType.NDFI, Satellite.LS7): NdfiBands,
    (DatasetType.NDFI, Satellite.LS8): NdfiBands,

    (DatasetType.EVI, Satellite.LS5): EviBands,
    (DatasetType.EVI, Satellite.LS7): EviBands,
    (DatasetType.EVI, Satellite.LS8): EviBands,

    (DatasetType.NBR, Satellite.LS5): NbrBands,
    (DatasetType.NBR, Satellite.LS7): NbrBands,
    (DatasetType.NBR, Satellite.LS8): NbrBands,

    (DatasetType.TCI, Satellite.LS5): TciBands,
    (DatasetType.TCI, Satellite.LS7): TciBands,
    (DatasetType.TCI, Satellite.LS8): TciBands,

    (DatasetType.DSM, None): DsmBands,
    (DatasetType.DEM, None): DsmBands,
    (DatasetType.DEM_SMOOTHED, None): DsmBands,
    (DatasetType.DEM_HYDROLOGICALLY_ENFORCED, None): DsmBands,

    (DatasetType.NDWI, Satellite.LS5): NdwiBands,
    (DatasetType.NDWI, Satellite.LS7): NdwiBands,
    (DatasetType.NDWI, Satellite.LS8): NdwiBands,

    (DatasetType.MNDWI, Satellite.LS5): MndwiBands,
    (DatasetType.MNDWI, Satellite.LS7): MndwiBands,
    (DatasetType.MNDWI, Satellite.LS8): MndwiBands,
}


def get_bands(dataset_type, satellite):

    # Try WITH satellite

    if (dataset_type, satellite) in BANDS:
        return BANDS[(dataset_type, satellite)]

    # Try WITHOUT satellite

    elif (dataset_type, None) in BANDS:
        return BANDS[(dataset_type, None)]

    return None


def parse_datetime(s):
    from datetime import datetime
    return datetime.strptime(s[:len("YYYY-MM-DD HH:MM:SS")], "%Y-%m-%d %H:%M:%S")
    # return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
