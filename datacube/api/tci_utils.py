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

__author__ = 'u81051'

import datacube.api.utils_v1
from datacube.api.utils_v1 import TasselCapIndex, NDV
import numpy
from enum import Enum
import logging

_log = logging.getLogger()

class sat_2(Enum):
    __order__ = "LANDSAT_5 LANDSAT_8"

    LANDSAT_5 = "LANDSAT_5"
    LANDSAT_8 = "LANDSAT_8"

class six_uni_bands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"
    
    BLUE = 0
    GREEN = 1
    RED = 2
    NEAR_INFRARED = 3
    SHORT_WAVE_INFRARED_1 = 4
    SHORT_WAVE_INFRARED_2 = 5

TCI_COEFF = {
    sat_2.LANDSAT_5:
    {
        TasselCapIndex.BRIGHTNESS: {
            six_uni_bands.BLUE: 0.3037,
            six_uni_bands.GREEN: 0.2793,
            six_uni_bands.RED: 0.4743,
            six_uni_bands.NEAR_INFRARED: 0.5585,
            six_uni_bands.SHORT_WAVE_INFRARED_1: 0.5082,
            six_uni_bands.SHORT_WAVE_INFRARED_2: 0.1863},

        TasselCapIndex.GREENNESS: {
            six_uni_bands.BLUE: -0.2848,
            six_uni_bands.GREEN: -0.2435,
            six_uni_bands.RED: -0.5436,
            six_uni_bands.NEAR_INFRARED: 0.7243,
            six_uni_bands.SHORT_WAVE_INFRARED_1: 0.0840,
            six_uni_bands.SHORT_WAVE_INFRARED_2: -0.1800},

        TasselCapIndex.WETNESS: {
            six_uni_bands.BLUE: 0.1509,
            six_uni_bands.GREEN: 0.1973,
            six_uni_bands.RED: 0.3279,
            six_uni_bands.NEAR_INFRARED: 0.3406,
            six_uni_bands.SHORT_WAVE_INFRARED_1: -0.7112,
            six_uni_bands.SHORT_WAVE_INFRARED_2: -0.4572},

        TasselCapIndex.FOURTH: {
            six_uni_bands.BLUE: -0.8242,
            six_uni_bands.GREEN: 0.0849,
            six_uni_bands.RED: 0.4392,
            six_uni_bands.NEAR_INFRARED: -0.0580,
            six_uni_bands.SHORT_WAVE_INFRARED_1: 0.2012,
            six_uni_bands.SHORT_WAVE_INFRARED_2: -0.2768},

        TasselCapIndex.FIFTH: {
            six_uni_bands.BLUE: -0.3280,
            six_uni_bands.GREEN: 0.0549,
            six_uni_bands.RED: 0.1075,
            six_uni_bands.NEAR_INFRARED: 0.1855,
            six_uni_bands.SHORT_WAVE_INFRARED_1: -0.4357,
            six_uni_bands.SHORT_WAVE_INFRARED_2: 0.8085},

        TasselCapIndex.SIXTH: {
            six_uni_bands.BLUE: 0.1084,
            six_uni_bands.GREEN: -0.9022,
            six_uni_bands.RED: 0.4120,
            six_uni_bands.NEAR_INFRARED: 0.0573,
            six_uni_bands.SHORT_WAVE_INFRARED_1: -0.0251,
            six_uni_bands.SHORT_WAVE_INFRARED_2: 0.0238}
    },
    sat_2.LANDSAT_8:
    {
        TasselCapIndex.BRIGHTNESS: {
            six_uni_bands.BLUE: 0.3029,
            six_uni_bands.GREEN: 0.2786,
            six_uni_bands.RED: 0.4733,
            six_uni_bands.NEAR_INFRARED: 0.5599,
            six_uni_bands.SHORT_WAVE_INFRARED_1: 0.508,
            six_uni_bands.SHORT_WAVE_INFRARED_2: 0.1872},

        TasselCapIndex.GREENNESS: {
            six_uni_bands.BLUE: -0.2941,
            six_uni_bands.GREEN: -0.2430,
            six_uni_bands.RED: -0.5424,
            six_uni_bands.NEAR_INFRARED: 0.7276,
            six_uni_bands.SHORT_WAVE_INFRARED_1: 0.0713,
            six_uni_bands.SHORT_WAVE_INFRARED_2: -0.1608},

        TasselCapIndex.WETNESS: {
            six_uni_bands.BLUE: 0.1511,
            six_uni_bands.GREEN: 0.1973,
            six_uni_bands.RED: 0.3283,
            six_uni_bands.NEAR_INFRARED: 0.3407,
            six_uni_bands.SHORT_WAVE_INFRARED_1: -0.7117,
            six_uni_bands.SHORT_WAVE_INFRARED_2: -0.4559},

        TasselCapIndex.FOURTH: {
            six_uni_bands.BLUE: -0.8239,
            six_uni_bands.GREEN: 0.0849,
            six_uni_bands.RED: 0.4396,
            six_uni_bands.NEAR_INFRARED: -0.058,
            six_uni_bands.SHORT_WAVE_INFRARED_1: 0.2013,
            six_uni_bands.SHORT_WAVE_INFRARED_2: -0.2773},

        TasselCapIndex.FIFTH: {
            six_uni_bands.BLUE: -0.3294,
            six_uni_bands.GREEN: 0.0557,
            six_uni_bands.RED: 0.1056,
            six_uni_bands.NEAR_INFRARED: 0.1855,
            six_uni_bands.SHORT_WAVE_INFRARED_1: -0.4349,
            six_uni_bands.SHORT_WAVE_INFRARED_2: 0.8085},

        TasselCapIndex.SIXTH: {
            six_uni_bands.BLUE: 0.1079,
            six_uni_bands.GREEN: -0.9023,
            six_uni_bands.RED: 0.4119,
            six_uni_bands.NEAR_INFRARED: 0.0575,
            six_uni_bands.SHORT_WAVE_INFRARED_1: -0.0259,
            six_uni_bands.SHORT_WAVE_INFRARED_2: 0.0252}
    }
}

def calculate_tci(band, satellite, blue, green, red, nir, sw1, sw2):
    all_bands = dict()
    masked_bands = dict()
    for t in six_uni_bands:
        if t.name == "BLUE":
            all_bands[t] = blue 
        if t.name == "GREEN":
            all_bands[t] = green
        if t.name == "RED":
            all_bands[t] = red
        if t.name == "NEAR_INFRARED":
            all_bands[t] = nir 
        if t.name == "SHORT_WAVE_INFRARED_1":
            all_bands[t] = sw1
        if t.name == "SHORT_WAVE_INFRARED_2":
            all_bands[t] = sw2
    for b in all_bands.iterkeys():
        masked_bands[b] = numpy.ma.masked_equal(all_bands[b], NDV).astype(numpy.float16) 
        _log.info("mask band for %s is %s", b, masked_bands[b])
    tci = 0
    tci_cat = None
    for i in TasselCapIndex:
        if i.name == band:
            tci_cat= i
            break

    sat= satellite
    if sat == "LANDSAT_7":
       sat = "LANDSAT_5" 

    for i in sat_2:
        if i.name == sat:
            sat = i
    for b in all_bands:
        if b in TCI_COEFF[sat][tci_cat]: 
             tci +=   masked_bands[b] * TCI_COEFF[sat][tci_cat][b] 
             _log.info(" tci value for %s - %s of %s", b, tci, TCI_COEFF[sat][tci_cat])
    tci = tci.filled(numpy.nan)
    _log.info(" TCI values calculated for %s %s - %s", sat, band, tci) 
    return tci
