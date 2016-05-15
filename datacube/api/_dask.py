# coding=utf-8
"""
Functions for creating a lazy-loaded array from storage units.
"""
from __future__ import absolute_import

import itertools
import operator
import collections
import functools

import dask
import dask.array as da
import numpy


def get_dask_array(storage_units, var_name, dimensions, dim_props, is_fake_array=False):
    # calculate chunks per su index
    chunk_size = (250, ) * len(dimensions)  # Bad default value
    sample_su = storage_units[0]
    if hasattr(sample_su, 'attributes'):
        if 'storage_type' in sample_su.attributes:
            if hasattr(sample_su.attributes['storage_type'], 'chunking'):
                chunk_size = sample_su.attributes['storage_type'].chunking
    storage_unit_sizes = [dim_props['sus_size'][dim] for dim in dimensions]
    chunks, offsets = _get_chunks_and_offsets(storage_unit_sizes, chunk_size)
    all_chunks = [tuple(itertools.chain(*chunk)) for chunk in chunks]
    dsk = _make_dask(storage_units, var_name, dimensions, dim_props, chunks, offsets, all_chunks) \
        if not is_fake_array else {}
    dtype = storage_units[0].variables[var_name].dtype
    dask_array = da.Array(dsk, var_name, all_chunks, dtype=dtype)
    return dask_array


def _make_dask(storage_units, var_name, dimensions, dim_props, chunks, offsets, all_chunks):
    dsk = {}
    for storage_unit in storage_units:
        dsk.update(_make_dask_for_su(storage_unit, var_name, dimensions, dim_props, chunks, offsets))

    all_keys = set(itertools.product((var_name,), *tuple(range(len(tuple(itertools.chain(*chunk))))
                                                         for chunk in chunks)))
    missing_keys = all_keys - set(dsk.keys())

    dtype, nodata = _nodata_properties(storage_units, var_name)
    dsk.update(_make_dask_for_blanks(missing_keys, all_chunks, dtype, nodata))
    return dsk


def _make_dask_for_su(storage_unit, var_name, dimensions, dim_props, chunks, offsets):
    dsk = {}
    file_index = tuple()   # Dask is indexed by a tuple of ("Name", x-index pos, y-index pos, z-index pos, ...)
    for dim in dimensions:
        ordinal = dim_props['dim_vals'][dim].index(storage_unit.coordinates[dim].begin)
        file_index += (ordinal,)
    slices = _get_slices_for_file_chunks(file_index, chunks, offsets)
    for key, su_slice in slices.items():
        dsk[(var_name,) + key] = (storage_unit.get_chunk, var_name, su_slice)
    return dsk


def _make_dask_for_blanks(missing_keys, all_chunks, dtype, nodata):
    dsk = {}
    for key in missing_keys:
        shape = tuple()
        for i, dim_chunks in enumerate(all_chunks):
            shape += (dim_chunks[key[i+1]],)
        dsk[key] = (_no_data_block, shape, dtype, nodata)
    return dsk


def _get_slices_for_file_chunks(file_index, chunks, offsets):
    keys = ()
    slices = []
    for dim, file_i in enumerate(file_index):
        offset = offsets[dim][file_i]
        chunk = chunks[dim][file_i]
        keys += (tuple(range(offset, offset+len(chunk))),)
        slices += (map(slice, numpy.cumsum((0,) + chunk[:-1]), numpy.cumsum(chunk)),)
    return dict(zip(itertools.product(*keys), itertools.product(*slices)))


def _get_chunks_and_offsets(su_size, chunk_size):
    offsets = [()] * len(su_size)
    chunks = [()] * len(su_size)
    for i, (file_lengths, chunk_length) in enumerate(zip(su_size, chunk_size)):
        prev_file_cum = 0
        for file_length in file_lengths:
            d = file_length // chunk_length
            m = file_length % chunk_length
            file_dim_chunks = (chunk_length,) * d
            if m:
                file_dim_chunks += (m,)
            chunks[i] += (file_dim_chunks,)
            offsets[i] += (prev_file_cum, )
            prev_file_cum += len(file_dim_chunks)
    return chunks, offsets


def _get_dask_for_storage_units(storage_units, var_name, dimensions, dim_vals, dsk_id):
    dsk = {}
    for storage_unit in storage_units:
        dsk_index = (dsk_id,)   # Dask is indexed by a tuple of ("Name", x-index pos, y-index pos, z-index pos, ...)
        for dim in dimensions:
            file_ordinal = dim_vals[dim].index(storage_unit.coordinates[dim].begin)
            dsk_index += (file_ordinal,)
        # TODO: Wrap in a chunked dask for sub-file dask chunks
        dsk[dsk_index] = (storage_unit.get_chunk, var_name, Ellipsis)
        #dsk[dsk_index] = (_get_chunked_data_func, storage_unit, var_name)
    return dsk


def _fill_in_dask_blanks(dsk, storage_units, var_name, dimensions, dim_props, dsk_id):
    all_dsk_keys = set(itertools.product((dsk_id,), *[[i for i, _ in enumerate(dim_props['dim_vals'][dim])]
                                                      for dim in dimensions]))
    missing_dsk_keys = all_dsk_keys - set(dsk.keys())

    if missing_dsk_keys:
        dtype, nodata = _nodata_properties(storage_units, var_name)
        for key in missing_dsk_keys:
            shape = _get_chunk_shape(key, dimensions, dim_props['sus_size'])
            dsk[key] = (_no_data_block, shape, dtype, nodata)
        return dsk


def _nodata_properties(storage_units, var_name):
    sample = storage_units[0]
    dtype = sample.variables[var_name].dtype
    nodata = sample.variables[var_name].nodata
    return dtype, nodata


def _get_chunk_shape(key, dimensions, chunksize):
    coords = list(key)[1:]
    shape = tuple(operator.getitem(chunksize[dim], i) for dim, i in zip(dimensions, coords))
    return shape


def _no_data_block(shape, dtype, fill):
    arr = numpy.empty(shape, dtype)
    if fill is None:
        fill = numpy.NaN
    arr.fill(fill)
    return arr


# def _get_chunked_data_func(storage_unit, var_name):
#     storage_array = NDArrayProxy(storage_unit, var_name)
#     # TODO: Chunk along chunk direction
#     chunks = (1000,) * storage_array.ndim
#     return da.from_array(storage_array, chunks=chunks)
#
#
# # TODO: Move into storage.access.StorageUnitBase
# class NDArrayProxy(object):
#     def __init__(self, storage_unit, var_name):
#         self._storage_unit = storage_unit
#         self._var_name = var_name
#
#     @property
#     def ndim(self):
#         return len(self._storage_unit.variables[self._var_name].dimensions)
#
#     @property
#     def size(self):
#         return functools.reduce(operator.mul, self.shape)
#
#     @property
#     def dtype(self):
#         return self._storage_unit.variables[self._var_name].dtype
#
#     @property
#     def shape(self):
#         dims = self._storage_unit.variables[self._var_name].dimensions
#         return tuple(self._storage_unit.coordinates[dim].length for dim in dims)
#
#     def __len__(self):
#         return self.shape[0]
#
#     def __array__(self, dtype=None):
#         x = self[...]
#         if dtype and x.dtype != dtype:
#             x = x.astype(dtype)
#         if not isinstance(x, numpy.ndarray):
#             x = numpy.array(x)
#         return x
#
#     def __getitem__(self, key):
#         # if not isinstance(key, collections.Iterable):
#         #     # dealing with a single value
#         #     if not isinstance(key, slice):
#         #         key = slice(key, key + 1)
#         #     key = [key]
#         # if len(key) < self.ndim:
#         #     key = [key[i] if i < len(key) else slice(0,self.shape[i]) for i in range(self.ndim)]
#
#         return self._storage_unit.get_chunk(self._var_name, key)
#
#     def __repr__(self):
#         return '%s(array=%r)' % (type(self).__name__, self.shape)
