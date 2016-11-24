from __future__ import absolute_import, division

import cachetools

from osgeo import ogr, osr
from rasterio.coords import BoundingBox

from datacube import compat


def _buffered_boundingbox(bb, ybuff, xbuff):
    return BoundingBox(left=bb.left - xbuff, right=bb.right + xbuff, top=bb.top + ybuff, bottom=bb.bottom - ybuff)
BoundingBox.buffered = _buffered_boundingbox


class CRSProjProxy(object):
    def __init__(self, crs):
        self._crs = crs

    def __getattr__(self, item):
        return self._crs.GetProjParm(item)


@cachetools.cached({})
def _make_crs(crs_str):
    crs = osr.SpatialReference()
    crs.SetFromUserInput(crs_str)
    if not crs.ExportToProj4() or crs.IsGeographic() == crs.IsProjected():
        raise ValueError('Not a valid CRS: %s' % crs_str)
    return crs


class CRS(object):
    """
    Wrapper around `osr.SpatialReference` providing a more pythonic interface

    >>> crs = CRS('EPSG:3577')
    >>> crs.geographic
    False
    >>> crs.projected
    True
    >>> crs.dimensions
    ('y', 'x')
    >>> crs = CRS('EPSG:4326')
    >>> crs.geographic
    True
    >>> crs.projected
    False
    >>> crs.epsg
    4326
    >>> crs.dimensions
    ('latitude', 'longitude')
    >>> crs = CRS('EPSG:3577')
    >>> crs.epsg
    3577
    >>> crs.dimensions
    ('y', 'x')
    >>> CRS('EPSG:3577') == CRS('EPSG:3577')
    True
    >>> CRS('EPSG:3577') == CRS('EPSG:4326')
    False
    >>> CRS('blah')
    Traceback (most recent call last):
        ...
    ValueError: Not a valid CRS: blah
    >>> CRS('PROJCS["unnamed",\
    ... GEOGCS["WGS 84", DATUM["WGS_1984", SPHEROID["WGS 84",6378137,298.257223563, AUTHORITY["EPSG","7030"]],\
    ... AUTHORITY["EPSG","6326"]], PRIMEM["Greenwich",0, AUTHORITY["EPSG","8901"]],\
    ... UNIT["degree",0.0174532925199433, AUTHORITY["EPSG","9122"]], AUTHORITY["EPSG","4326"]]]')
    Traceback (most recent call last):
        ...
    ValueError: Not a valid CRS: ...
    """

    def __init__(self, crs_str):
        """

        :param crs_str: string representation of a CRS, often an EPSG code like 'EPSG:4326'
        """
        if isinstance(crs_str, CRS):
            crs_str = crs_str.crs_str
        self.crs_str = crs_str
        self._crs = _make_crs(crs_str)

    def __getitem__(self, item):
        return self._crs.GetAttrValue(item)

    def __getstate__(self):
        return {'crs_str': self.crs_str}

    def __setstate__(self, state):
        self.__init__(state['crs_str'])

    @property
    def wkt(self):
        """
        WKT representation of the CRS

        :type: str
        """
        return self._crs.ExportToWkt()

    @property
    def epsg(self):
        """
        EPSG Code of the CRS

        :type: int
        """
        if self.projected:
            return int(self._crs.GetAuthorityCode('PROJCS'))

        if self.geographic:
            return int(self._crs.GetAuthorityCode('GEOGCS'))

    @property
    def proj(self):
        return CRSProjProxy(self._crs)

    @property
    def semi_major_axis(self):
        return self._crs.GetSemiMajor()

    @property
    def semi_minor_axis(self):
        return self._crs.GetSemiMinor()

    @property
    def inverse_flattening(self):
        return self._crs.GetInvFlattening()

    @property
    def geographic(self):
        """
        :type: bool
        """
        return self._crs.IsGeographic() == 1

    @property
    def projected(self):
        """
        :type: bool
        """
        return self._crs.IsProjected() == 1

    @property
    def dimensions(self):
        """
        List of dimension names of the CRS

        :type: (str,str)
        """
        if self.geographic:
            return 'latitude', 'longitude'

        if self.projected:
            return 'y', 'x'

    @property
    def units(self):
        """
        List of dimension units of the CRS

        :type: (str,str)
        """
        if self.geographic:
            return 'degrees_north', 'degrees_east'

        if self.projected:
            return self['UNIT'], self['UNIT']

    def __str__(self):
        return self.crs_str

    def __repr__(self):
        return "CRS('%s')" % self.crs_str

    def __eq__(self, other):
        if isinstance(other, compat.string_types):
            other = CRS(other)
        assert isinstance(other, self.__class__)
        canonical = lambda crs: set(crs.ExportToProj4().split() + ['+wktext'])
        return canonical(self._crs) == canonical(other._crs)  # pylint: disable=protected-access

    def __ne__(self, other):
        if isinstance(other, compat.string_types):
            other = CRS(other)
        assert isinstance(other, self.__class__)
        return self._crs.IsSame(other._crs) != 1  # pylint: disable=protected-access


def _make_point(pt):
    geom = ogr.Geometry(ogr.wkbPoint)
    geom.AddPoint_2D(*pt)
    return geom


def _make_linear(type_, coordinates):
    geom = ogr.Geometry(type_)
    for pt in coordinates:
        geom.AddPoint_2D(*pt)
    return geom


def _make_multipoint(coordinates):
    return _make_linear(ogr.wkbMultiPoint, coordinates)


def _make_line(coordinates):
    return _make_linear(ogr.wkbLineString, coordinates)


def _make_multiline(coordinates):
    geom = ogr.Geometry(ogr.wkbMultiLineString)
    for line_coords in coordinates:
        geom.AddGeometryDirectly(_make_line(line_coords))
    return geom


def _make_polygon(coordinates):
    geom = ogr.Geometry(ogr.wkbPolygon)
    for ring_coords in coordinates:
        geom.AddGeometryDirectly(_make_linear(ogr.wkbLinearRing, ring_coords))
    return geom


def _make_multipolygon(coordinates):
    geom = ogr.Geometry(ogr.wkbMultiPolygon)
    for poly_coords in coordinates:
        geom.AddGeometryDirectly(_make_polygon(poly_coords))
    return geom


def _get_coordinates(geom):
    if geom.GetGeometryType() == ogr.wkbPoint:
        return geom.GetPoint_2D(0)
    if geom.GetGeometryType() in [ogr.wkbMultiPoint, ogr.wkbLineString, ogr.wkbLinearRing]:
        return geom.GetPoints()
    else:
        return [_get_coordinates(geom.GetGeometryRef(i)) for i in range(geom.GetGeometryCount())]


class Geometry(object):
    geom_makers = {
        'Point': _make_point,
        'MultiPoint': _make_multipoint,
        'LineString': _make_line,
        'MultiLineString': _make_multiline,
        'Polygon': _make_polygon,
        'MultiPolygon': _make_multipolygon,
    }

    geom_types = {
        ogr.wkbPoint: 'Point',
        ogr.wkbMultiPoint: 'MultiPoint',
        ogr.wkbLineString: 'LineString',
        ogr.wkbMultiLineString: 'MultiLineString',
        ogr.wkbPolygon: 'Polygon',
        ogr.wkbMultiPolygon: 'MultiPolygon',
    }

    def __init__(self, type_, coordinates, crs=None):
        self.crs = crs
        self._geom = Geometry.geom_makers[type_](coordinates)

    @property
    def type(self):
        return Geometry.geom_types[self._geom.GetGeometryType()]

    @property
    def points(self):
        return self._geom.GetPoints()

    @property
    def boundingbox(self):
        minx, maxx, miny, maxy = self._geom.GetEnvelope()
        return BoundingBox(left=minx, right=maxx, bottom=miny, top=maxy)

    @property
    def wkt(self):
        return getattr(self._geom, 'ExportToIsoWkt', self._geom.ExportToWkt)()

    @property
    def __geo_interface__(self):
        return {
            'type': self.type,
            'coordinates': _get_coordinates(self._geom)
        }

    def intersects(self, other):
        assert self.crs == other.crs
        return self._geom.Intersects(other._geom)  # pylint: disable=protected-access

    def intersection(self, other):
        assert self.crs == other.crs
        geom = self._geom.Intersection(other._geom)  # pylint: disable=protected-access
        result = Geometry.__new__(Geometry)
        result._geom = geom  # pylint: disable=protected-access
        result.crs = self.crs
        return result

    def touches(self, other):
        assert self.crs == other.crs
        return self._geom.Touches(other._geom)  # pylint: disable=protected-access

    def to_crs(self, crs, resolution=None):
        if self.crs == crs:
            return self

        if resolution is None:
            resolution = 1 if self.crs.geographic else 100000

        transform = osr.CoordinateTransformation(self.crs._crs, crs._crs)  # pylint: disable=protected-access
        clone = self._geom.Clone()
        clone.Segmentize(resolution)
        clone.Transform(transform)

        transformed = Geometry.__new__(Geometry)
        transformed.crs = crs
        transformed._geom = clone  # pylint: disable=protected-access
        return transformed

    def __str__(self):
        return str(self._geom)

    def __repr__(self):
        return repr(self._geom)


def point(x, y, crs=None):
    return Geometry('Point', (x, y), crs=crs)


def line(coords, crs=None):
    return Geometry('LineString', coords, crs=crs)


def polygon(outer, crs=None, *inners):
    return Geometry('Polygon', (outer, )+inners, crs=crs)


def check_intersect(a, b):
    return a.intersects(b) and not a.touches(b)


def union_cascaded(geoms):
    geom = ogr.Geometry(ogr.wkbGeometryCollection)
    crs = None
    for g in geoms:
        if crs:
            assert crs == g.crs
        else:
            crs = g.crs

        geom.AddGeometry(g._geom)  # pylint: disable=protected-access
    geom.UnionCascaded()
    assert geom.GetGeometryCount() == 1
    result = Geometry.__new__(Geometry)
    result._geom = geom.GetGeometryRef(0).Clone()  # pylint: disable=protected-access
    result.crs = crs
    return result
