#    Copyright 2016 Geoscience Australia
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""
Spectral Band equivalence and filter functions
"""

from __future__ import absolute_import, division, print_function

import numpy as np
from scipy.interpolate import UnivariateSpline
from scipy import signal
from scipy import ndimage
from scipy import stats
try:
    import emd
except ImportError:
    pass


from .semantic import VariableMapper
from .landsat_srf import landsat


class SpectralBand(object):
    def __init__(self, name, srf):
        """
        A representation of a band
        :param name: display name
        :param srf: dict of {'wavelength': [1, 2, ...], 'rsr': [0.5, 0.99, 0.7, ...]}
        :return:
        """
        self.name = name
        self.srf = srf
        self.wavelength = srf['wavelength']
        self.rsr = srf['rsr']

    def like(self, **thresholds):
        return SpectralMatch(self, **thresholds)

    def __repr__(self):
        return "SpectralBand<name={}, range=({}, {})>".format(self.name,
                                                             self.srf['wavelength'][0],
                                                             self.srf['wavelength'][-1])

def get_band(platform, band):
    if platform in landsat and band in landsat[platform]:
        name = "{} {}".format(platform, band)
        return SpectralBand(name, landsat[platform][band])
    else:
        raise KeyError("Could not find a match for platform {} and band {}".format(platform, band))


def get_bands(*platform_bands):
    return [get_band(platform, band) for platform, band in platform_bands]


class SpectralMatch(VariableMapper):
    def __init__(self, band, p_correlation=None, distance=None, center=None, area=None, fwhm=None):
        super(SpectralMatch, self).__init__()
        self.band = band
        self.p_correlation = p_correlation
        self.distance = distance
        self.center = center
        self.area = area
        self.fwhm = fwhm

        lookup = dict()
        lookup['LANDSAT_5'] = {
            'band_1': 'blue',
            'band_2': 'green',
            'band_3': 'red',
            'band_4': 'nir',
            'band_5': 'swir1',
            'band_7': 'swir2',
            'band_10': 'blue',
            'band_20': 'green',
            'band_30': 'red',
            'band_40': 'nir',
            'band_50': 'swir1',
            'band_70': 'swir2',
            }
        lookup['LANDSAT_7'] = lookup['LANDSAT_5']
        lookup['LANDSAT_8'] = {
            'band_1': 'coastal',
            'band_2': 'blue',
            'band_3': 'green',
            'band_4': 'red',
            'band_5': 'nir',
            'band_6': 'swir1',
            'band_7': 'swir2',
            'band_10': 'coastal',
            'band_20': 'blue',
            'band_30': 'green',
            'band_40': 'red',
            'band_50': 'nir',
            'band_60': 'swir1',
            'band_70': 'swir2'
            }
        self.lookup = lookup

    # def product_filter(self, product):
    #     return product['match']['metadata']['platform']['code'].startswith('LANDSAT_')

    def variable_filter(self, source_name, variable, product):
        platform = product['match']['metadata']['platform']['code']
        if platform in self.lookup and source_name in self.lookup[platform]:
            lookup_name = self.lookup[platform][source_name]
        else:
            return False
        query_band = get_band(platform, lookup_name)
        results = compare_bands(query_band, self.band)
        if self.p_correlation is not None and results['p_correlation'] < self.p_correlation:
            return False
        if self.distance is not None and results['emd'] > self.distance:
            return False
        if self.center is not None and results['weighted_center_delta'] > self.center:
            return False
        if self.area is not None and results['area_delta'] > self.area:
            return False
        if self.fwhm is not None and results['fwhm_delta'] > self.fwhm:
            return False
        self._list.append(source_name)
        return True

    def __repr__(self):
        return "SpectralMatch<band={}>".format(self.band)


def like(*band_thresholds):
    return [SpectralMatch(band, **thresholds) for band, thresholds in band_thresholds]


def compare_bands(band1, band2):
    bounds = []
    bounds.extend(band_range(band1.srf))
    bounds.extend(band_range(band2.srf))

    range_start = min(bounds)
    range_end = max(bounds)
    range_width = range_end - range_start + 1

    # Interpolate rsr
    band1_wl, band1_rsr = reshape_interpolate(range_start, range_end, range_width,
                                              band1.srf['wavelength'],
                                              [v if v > 0 else np.nan for v in band1.srf['rsr']], 1)
    band2_wl, band2_rsr = reshape_interpolate(range_start, range_end, range_width,
                                              band2.srf['wavelength'],
                                              [v if v > 0 else np.nan for v in band2.srf['rsr']], 1)

    # A smoothed distrubution seems important for Earth Mover Distance
    A = ndimage.filters.gaussian_filter(band1_rsr, 10)
    B = ndimage.filters.gaussian_filter(band2_rsr, 10)

    # Earth Mover Distance
    # From - https://github.com/andreasjansson/python-emd
    try:
        # normalise - confirm with someone who has maths skills that doing this makes sense - seems to be required for EMD
        A = (A - A.min())/(A.max() - A.min())
        B = (B - B.min())/(B.max() - B.min())

        earth_mover_distance = emd.emd(range(500), range(500), signal.resample(A,500), signal.resample(B,500))
    except:
        earth_mover_distance = 0
        pass

    pearson = stats.pearsonr(band1_rsr, band2_rsr)

    p_correlation = pearson[0]

    # "Area" under each curve
    band1_trapz = np.trapz(band1_rsr, band1_wl)
    band2_trapz = np.trapz(band2_rsr, band2_wl)
    area_delta = abs(band1_trapz - band2_trapz)

    band1_mean = np.average(band1_wl, weights=band1_rsr)
    band2_mean = np.average(band2_wl, weights=band2_rsr)
    weighted_center_delta = abs(band1_mean - band2_mean)


    #FWHM as spline roots
    spline1 = UnivariateSpline(band1_wl, A-A.max() / 2, s=0)
    spline2 = UnivariateSpline(band2_wl, B-B.max() / 2, s=0)
    try:
        band1_r1, band1_r2 = spline1.roots()
        band2_r1, band2_r2 = spline2.roots()
    except:
        band1_r1 = 100.
        band1_r2 = 100.
        band2_r1 = 100.
        band2_r2 = 100.
        pass
    fwhm_delta = abs((band1_r2 - band1_r1) - (band2_r2 - band2_r1))

    results = {
        'emd': earth_mover_distance,
        'pearson': pearson,
        'p_correlation': p_correlation,
        'area_delta': area_delta,
        'weighted_center_delta': weighted_center_delta,
        'fwhm_delta': fwhm_delta,
    }

    return results


def reshape_interpolate(start, stop, samples, input1dwavelength, input1drsr, wlscalefactor):
    wavelength = np.linspace(start, stop, samples, dtype=float)
    rsr = np.nan_to_num(np.interp(wavelength, input1dwavelength*wlscalefactor, input1drsr))
    return wavelength, rsr


def band_range(band):
    valid_mask = [value > 0 for value in band['rsr']]
    start_index = valid_mask.index(True)
    valid_mask.reverse()
    end_index = - valid_mask.index(True) - 1
    return band['wavelength'][start_index], band['wavelength'][end_index]

