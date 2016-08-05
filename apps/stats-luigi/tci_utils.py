"""
__author__ = 'u81051'
"""

from __future__ import absolute_import
import logging
import numpy
from enum import Enum
from .utils_v1 import TasselCapIndex

_log = logging.getLogger()


class SatTwo(Enum):
    __order__ = "LANDSAT_5 LANDSAT_8"

    LANDSAT_5 = "LANDSAT_5"
    LANDSAT_8 = "LANDSAT_8"


class SixUniBands(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2"
    BLUE = 0
    GREEN = 1
    RED = 2
    NEAR_INFRARED = 3
    SHORT_WAVE_INFRARED_1 = 4
    SHORT_WAVE_INFRARED_2 = 5

TCI_COEFF = {
    SatTwo.LANDSAT_5:
    {
        TasselCapIndex.BRIGHTNESS: {
            SixUniBands.BLUE: 0.3037,
            SixUniBands.GREEN: 0.2793,
            SixUniBands.RED: 0.4743,
            SixUniBands.NEAR_INFRARED: 0.5585,
            SixUniBands.SHORT_WAVE_INFRARED_1: 0.5082,
            SixUniBands.SHORT_WAVE_INFRARED_2: 0.1863},

        TasselCapIndex.GREENNESS: {
            SixUniBands.BLUE: -0.2848,
            SixUniBands.GREEN: -0.2435,
            SixUniBands.RED: -0.5436,
            SixUniBands.NEAR_INFRARED: 0.7243,
            SixUniBands.SHORT_WAVE_INFRARED_1: 0.0840,
            SixUniBands.SHORT_WAVE_INFRARED_2: -0.1800},

        TasselCapIndex.WETNESS: {
            SixUniBands.BLUE: 0.1509,
            SixUniBands.GREEN: 0.1973,
            SixUniBands.RED: 0.3279,
            SixUniBands.NEAR_INFRARED: 0.3406,
            SixUniBands.SHORT_WAVE_INFRARED_1: -0.7112,
            SixUniBands.SHORT_WAVE_INFRARED_2: -0.4572},

        TasselCapIndex.FOURTH: {
            SixUniBands.BLUE: -0.8242,
            SixUniBands.GREEN: 0.0849,
            SixUniBands.RED: 0.4392,
            SixUniBands.NEAR_INFRARED: -0.0580,
            SixUniBands.SHORT_WAVE_INFRARED_1: 0.2012,
            SixUniBands.SHORT_WAVE_INFRARED_2: -0.2768},

        TasselCapIndex.FIFTH: {
            SixUniBands.BLUE: -0.3280,
            SixUniBands.GREEN: 0.0549,
            SixUniBands.RED: 0.1075,
            SixUniBands.NEAR_INFRARED: 0.1855,
            SixUniBands.SHORT_WAVE_INFRARED_1: -0.4357,
            SixUniBands.SHORT_WAVE_INFRARED_2: 0.8085},

        TasselCapIndex.SIXTH: {
            SixUniBands.BLUE: 0.1084,
            SixUniBands.GREEN: -0.9022,
            SixUniBands.RED: 0.4120,
            SixUniBands.NEAR_INFRARED: 0.0573,
            SixUniBands.SHORT_WAVE_INFRARED_1: -0.0251,
            SixUniBands.SHORT_WAVE_INFRARED_2: 0.0238}
    },
    SatTwo.LANDSAT_8:
    {
        TasselCapIndex.BRIGHTNESS: {
            SixUniBands.BLUE: 0.3029,
            SixUniBands.GREEN: 0.2786,
            SixUniBands.RED: 0.4733,
            SixUniBands.NEAR_INFRARED: 0.5599,
            SixUniBands.SHORT_WAVE_INFRARED_1: 0.508,
            SixUniBands.SHORT_WAVE_INFRARED_2: 0.1872},

        TasselCapIndex.GREENNESS: {
            SixUniBands.BLUE: -0.2941,
            SixUniBands.GREEN: -0.2430,
            SixUniBands.RED: -0.5424,
            SixUniBands.NEAR_INFRARED: 0.7276,
            SixUniBands.SHORT_WAVE_INFRARED_1: 0.0713,
            SixUniBands.SHORT_WAVE_INFRARED_2: -0.1608},

        TasselCapIndex.WETNESS: {
            SixUniBands.BLUE: 0.1511,
            SixUniBands.GREEN: 0.1973,
            SixUniBands.RED: 0.3283,
            SixUniBands.NEAR_INFRARED: 0.3407,
            SixUniBands.SHORT_WAVE_INFRARED_1: -0.7117,
            SixUniBands.SHORT_WAVE_INFRARED_2: -0.4559},

        TasselCapIndex.FOURTH: {
            SixUniBands.BLUE: -0.8239,
            SixUniBands.GREEN: 0.0849,
            SixUniBands.RED: 0.4396,
            SixUniBands.NEAR_INFRARED: -0.058,
            SixUniBands.SHORT_WAVE_INFRARED_1: 0.2013,
            SixUniBands.SHORT_WAVE_INFRARED_2: -0.2773},

        TasselCapIndex.FIFTH: {
            SixUniBands.BLUE: -0.3294,
            SixUniBands.GREEN: 0.0557,
            SixUniBands.RED: 0.1056,
            SixUniBands.NEAR_INFRARED: 0.1855,
            SixUniBands.SHORT_WAVE_INFRARED_1: -0.4349,
            SixUniBands.SHORT_WAVE_INFRARED_2: 0.8085},

        TasselCapIndex.SIXTH: {
            SixUniBands.BLUE: 0.1079,
            SixUniBands.GREEN: -0.9023,
            SixUniBands.RED: 0.4119,
            SixUniBands.NEAR_INFRARED: 0.0575,
            SixUniBands.SHORT_WAVE_INFRARED_1: -0.0259,
            SixUniBands.SHORT_WAVE_INFRARED_2: 0.0252}
    }
}


def calculate_tci(band, satellite, blue, green, red, nir, sw1, sw2):       # pylint: disable=too-many-branches
    all_bands = dict()
    masked_bands = dict()
    for t in SixUniBands:
        if t.name == "BLUE":
            all_bands[t] = blue
        elif t.name == "GREEN":
            all_bands[t] = green
        elif t.name == "RED":
            all_bands[t] = red
        elif t.name == "NEAR_INFRARED":
            all_bands[t] = nir
        elif t.name == "SHORT_WAVE_INFRARED_1":
            all_bands[t] = sw1
        elif t.name == "SHORT_WAVE_INFRARED_2":
            all_bands[t] = sw2
    for b in all_bands.keys():
        # masked_bands[b] = numpy.ma.masked_equal(all_bands[b], NDV).astype(numpy.float16)
        masked_bands[b] = all_bands[b].astype(numpy.float16)
        _log.info("mask band for %s is %s", b, masked_bands[b])
    tci = 0
    tci_cat = None
    for i in TasselCapIndex:
        if i.name == band:
            tci_cat = i
            break

    sat = satellite
    if sat == "LANDSAT_7":
        sat = "LANDSAT_5"
    for i in SatTwo:
        if i.name == sat:
            sat = i
    for b in all_bands:
        if b in TCI_COEFF[sat][tci_cat]:
            tci += masked_bands[b] * TCI_COEFF[sat][tci_cat][b]
            _log.info(" tci value for %s - %s of %s", b, tci, TCI_COEFF[sat][tci_cat])
    # tci = tci.filled(numpy.nan)
    _log.info(" TCI values calculated for %s %s - %s", sat, band, tci)
    return tci
