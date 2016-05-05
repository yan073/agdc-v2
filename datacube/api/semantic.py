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
Semantic helpers for variable name mapping
"""

import re
from collections import defaultdict

from datacube.api import API


def list_variable_matches(api, variables):
    products = api.list_products()
    for product in products:
        mappers = [v for v in variables if v.product_filter(product)]
        for v_name, v_info in product['measurements'].items():
            for mapper in mappers:
                mapper.variable_filter(v_name, v_info, product)
    matches = []
    for mapper in mappers:
        matches += mapper.list_variables()
    return matches


def _get_variable_data_for_product(api, prod, variables, **kwargs):
    requested_datasets_with_variables = []
    # Find out what filters we need to run for this product
    mappers = [v for v in variables if v.product_filter(prod)]
    product_vars = {v_name: [mapper for mapper in mappers if mapper.variable_filter(v_name, v_info, prod)]
                    for v_name, v_info in prod['measurements'].items()}
    product_vars = {v: m_list for v, m_list in product_vars.items() if len(m_list)}

    # If we aren't getting everything (ie filter==None) and we want something out of here
    if variables is not None and product_vars:
        storage_type = prod['name']
        #This should be dc.get_data_arrays
        dataset = api.get_dataset(storage_type=storage_type, variables=product_vars.keys(), **kwargs)
        for (source_name, data_array) in dataset.data_vars.items():
            if source_name in product_vars:
                data_source = {
                    'name': source_name,
                    'variable': prod['measurements'][source_name],
                    'product': prod,
                    'array': data_array,
                }
                requested_datasets_with_variables.append((data_source, product_vars[source_name]))
    return requested_datasets_with_variables


def get_variable_data(api, variables, **kwargs):
    if not hasattr(variables, '__iter__'):
        variables = [variables]
    requested_datasets_with_variables = []
    products = api.list_products()
    for product in products:
        requested_datasets_with_variables += _get_variable_data_for_product(api, product, variables, **kwargs)
    output_datasets = []
    for mapper in variables:
        input_datasets = [data_source for data_source, mapper_list in requested_datasets_with_variables
                          if mapper in mapper_list]
        output_datasets += mapper.combine(input_datasets)
    return output_datasets


class VariableMapper(object):
    """
    Base class for variable renaming, etc
    """
    def __init__(self):
        self._list = []

    def product_filter(self, product):
        """Given the `storage_type`/`product`, should the variable filter be applied.
        Return False if no variable will match the filter.
        """
        return True

    def variable_filter(self, source_name, variable, product):
        """
        Should the data of `source_name` from `product` be retrived and supplied to the `combine` function.
        """
        return True

    def list_variables(self):
        """
        Lists the accumulated variable names that have been allowed by the variable_filter.
        :return: list of variables
        """
        return self._list

    def clear_list(self):
        self._list = []

    def combine(self, source_data):
        """
        :param source_data: A list of the data obtained for each dicts in the form:
            {
                'name': <source_name>,
                'variable': <variable_object>,
                'product': <product_table>,
                'array': xarray.DataArray
            }
        :return: list of dicts, containing:
            {
                'name': <output name of the variable>,
                'array': xarray.DataArray,
            }
            If `product` is returned, variables mapped to the same name will be returned per-product name.
            All other items will be ignored, so it is valid to return `source_data`.
            The most common usage is to change the `name` value to the output name.
            This function can be used to combine multiple products variables into one:
            e.g. (Landsat_5.Red, Landsat_7.Red, Landsat_8.Red) -> Landsat.Red
            Or combine several variables from a single product into one variable:
            e.g. (Red, NIR) -> NVDI
        """
        return source_data

    def __repr__(self):
        return 'VariableMapper'


class NameMapper(VariableMapper):
    """
    Basic case-insensitive name lookup
    """
    def __init__(self, name):
        super(NameMapper, self).__init__()
        self._name = name

    def variable_filter(self, source_name, variable, product):
        is_match = source_name.lower() == self._name.lower()
        if is_match:
            self._list.append(self._name)
        return is_match

    def combine(self, source_data):
        return [merge_dicts(source_var, {'name': self._name}) for source_var in source_data]

    def __repr__(self):
        return "NameMapper<{}>".format(self._name)


def by_name(*names):
    return [NameMapper(name) for name in names]


class LandsatMapper(VariableMapper):
    """
    Hard-coded landsat name lookup
    """
    def __init__(self, name):
        super(LandsatMapper, self).__init__()
        self._name = name.lower()
        lookup = {}
        lookup['LANDSAT_5'] = {
            'blue': 'band_1',
            'green': 'band_2',
            'red': 'band_3',
            'nir': 'band_4',
            'swir1': 'band_5',
            'swir2': 'band_7'
            }
        lookup['LANDSAT_7'] = lookup['LANDSAT_5']
        lookup['LANDSAT_8'] = {
            'coastal': 'band_1',
            'blue': 'band_2',
            'green': 'band_3',
            'red': 'band_4',
            'nir': 'band_5',
            'swir1': 'band_6',
            'swir2': 'band_7'
            }
        self._lookup = lookup

    def product_filter(self, product):
        return product['match']['metadata']['platform']['code'].startswith('LANDSAT_')

    def variable_filter(self, source_name, variable, product):
        platform = product['match']['metadata']['platform']['code']
        lookup = self._lookup[platform]
        is_match = self._name in lookup and lookup[self._name] == source_name
        if is_match:
            self._list.append(self._name)
        return is_match

    def combine(self, source_data):
        return [merge_dicts(source_var, {'name': self._name}) for source_var in source_data]

    def __repr__(self):
        return "LandsatMapper<{}>".format(self._name)


def by_landsat(*names):
    return [LandsatMapper(name) for name in names]


class BandNumberMapper(VariableMapper):
    """
    Hard-coded landsat name lookup
    For now converts 'band_1' to 1, etc, but in future will use variable's attributes
    """
    def __init__(self, band_number):
        super(BandNumberMapper, self).__init__()
        self._band_number = band_number

    def variable_filter(self, source_name, variable, product):
        is_match = self._band_number == _variable_name_to_band_number(source_name)
        if is_match:
            self._list.append(self._band_number)
        return is_match

    def combine(self, source_data):
        return [merge_dicts(source_var, {'name': str(self._band_number)}) for source_var in source_data]

    def __repr__(self):
        return "BandNumberMapper<{}>".format(self._band_number)


def by_band(*bands):
    return [BandNumberMapper(band) for band in bands]


class WavelengthRangeMapper(VariableMapper):
    def __init__(self, wavelength_range, name):
        super(WavelengthRangeMapper, self).__init__()
        self._range = wavelength_range
        self._name = name

    def variable_filter(self, source_name, variable, product):
        var_range = _long_name_to_centre_wavelength(variable)
        if var_range:
            is_match = self._range[1] >= var_range[0] and var_range[1] >= self._range[0]
            if is_match:
                self._list = [self._name]
        else:
            return False

    def combine(self, source_data):
        return [merge_dicts(source_var, {'name': str(self._name)}) for source_var in source_data]

    def __repr__(self):
        return "WavelengthRangeMapper<{}>".format(self._range)


def by_wavelength(*wave_ranges):
    return [WavelengthRangeMapper(wave_range) for wave_range in wave_ranges]


class NDVIMapper(VariableMapper):
    """
    NDVI
    """

    def variable_filter(self, source_name, variable, product):
        return self._band_number == _variable_name_to_band_number(source_name)

    def combine(self, source_data):
        # for each
        return [merge_dicts(source_var, {'name': str(self._band_number)}) for source_var in source_data]

    def __repr__(self):
        return "NDVIMapper"


def by_virtual(*virtual_variables):
    known_viruals = {
        'ndvi': NDVIMapper
    }
    for virtual_variable in virtual_variables:
        if virtual_variable.lower() not in known_viruals:
            raise NotImplementedError("Unknown virtual variable name: {}".format(virtual_variable))
    return [known_viruals[virtual_variable]() for virtual_variable in virtual_variables]


class MapperMapper(VariableMapper):
    """
    NDVI
    """
    def __init__(self, input_mappers, output_mappers):
        super(MapperMapper, self).__init__(self)
        self.input_mappers = input_mappers
        self.output_mappers = output_mappers
        self.variable_mappings = defaultdict(list)

    def variable_filter(self, source_name, variable, product):
        is_match = False
        for mapper in self.input_mappers:
            use_mapper = mapper.variable_filter(source_name, variable, product)
            if use_mapper:
                data_key = (product['name'], source_name)
                self.variable_mappings[data_key].append(mapper)
                is_match = True
        return is_match

    def combine(self, source_data):
        filtered_data = []
        for mapper in self.input_mappers:
            input_mapper_data = [entry for entry in source_data
                                 if mapper in self.variable_mappings[(entry['product']['name'], entry['name'])]]
            filtered_data.append(mapper.combine(input_mapper_data))
        return self.output_mappers.combine(filtered_data)

    def __repr__(self):
        return "NDVIMapper"


def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def _variable_name_to_band_number(variable_name):
    match = re.match(r'band_(\d)', variable_name)
    return int(match.groups()[0]) if match else None


def _long_name_to_centre_wavelength(variable):
    if 'attrs' not in variable:
        return None
    if 'long_name' in variable['attrs']:
        long_name = variable['attrs']['long_name']
    elif 'wavelength' in variable['attrs']:
        long_name = variable['attrs']['wavelength']
    else:
        return None

    reg = r""".*?(\d+\.\d*)\s*-\s*(\d+\.\d*)"""
    match = re.compile(reg).match(long_name)
    return (float(match.groups()[0]), float(match.groups()[1])) if match else None
