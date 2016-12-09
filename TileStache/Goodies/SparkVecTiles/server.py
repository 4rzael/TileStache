''' Provider that returns PostGIS vector tiles in GeoJSON or MVT format.

VecTiles is intended for rendering, and returns tiles with contents simplified,
precision reduced and often clipped. The MVT format in particular is designed
for use in Mapnik with the VecTiles Datasource, which can read binary MVT tiles.

For a more general implementation, try the Vector provider:
    http://tilestache.org/doc/#vector-provider
'''

import geopandas as gp

from math import pi
from urlparse import urljoin, urlparse
from urllib import urlopen
from os.path import exists

from . import geojson, topojson
from ...Geography import SphericalMercator
from ...Config import loadClassPath
from upp.processing.map_dataset import *
from upp.model.spark_adapter import SparkAdapter
from ModestMaps.Core import Point

class Provider:
    ''' VecTiles provider for PostGIS data sources.
    
        Parameters:

        Sample configuration, for a layer with no results at zooms 0-9, basic
        selection of lines with names and highway tags for zoom 10, a remote
        URL containing a query for zoom 11, and a local file for zooms 12+:
        
          "provider":
          {
            "class": "TileStache.Goodies.SparkVecTiles:Provider",
            "kwargs":
            {
                "dataset": "trees"
            }
          }

    '''
    def __init__(self, layer, **kwargs):
        '''
        '''
        self.layer = layer
        print layer
        self.dataset = kwargs['dataset']
        print self.dataset


        # TODO: init spark, transform the parquet to a geopandas dataframe
        self.ts_spark = SparkAdapter('TileStache')
        self.gp_dataframe = get_map_dataset(self.ts_spark, dataset_name=self.dataset)

    def renderTile(self, width, height, srs, coord):
        ''' Render a single tile, return a Response instance.
        '''

        # create the bounding box
        ll = self.layer.projection.coordinateProj(coord.down())
        ur = self.layer.projection.coordinateProj(coord.right())
        bounds = ll.x, ll.y, ur.x, ur.y

        return Response(self.gp_dataframe, bounds, coord.zoom)

    def getTypeByExtension(self, extension):
        ''' Get mime-type and format by file extension, one of "json" or "topojson".
        '''
        if extension.lower() == 'json':
            return 'application/json', 'JSON'
        
        elif extension.lower() == 'topojson':
            return 'application/json', 'TopoJSON'
        
        else:
            raise ValueError(extension)

class Response:
    '''
    '''
    def __init__(self, gp_dataframe, bounds, zoom):
        self.zoom = zoom
        self.gp_dataframe = gp_dataframe
        self.bounds = bounds
        
    def save(self, out, format):
        '''
        '''
        # TODO: Use the real hex_size
        gp_res = get_hexagons(self.gp_dataframe, self.bounds, hex_size=float(100) / pow(2, self.zoom))
        features = [(x[0], {'value': x[1]}) for x in gp_res.values]

        if format == 'JSON':
            geojson.encode(out, features, self.zoom, False)
        
        elif format == 'TopoJSON':
            ll = SphericalMercator().projLocation(Point(*self.bounds[0:2]))
            ur = SphericalMercator().projLocation(Point(*self.bounds[2:4]))
            topojson.encode(out, features, (ll.lon, ll.lat, ur.lon, ur.lat), False)

        else:
            raise ValueError(format)

class EmptyResponse:
    ''' Simple empty response renders valid MVT or GeoJSON with no features.
    '''
    def __init__(self, bounds, zoom):
        self.zoom = zoom
        self.bounds = bounds
    
    def save(self, out, format):
        '''
        '''
        
        if format == 'JSON':
            geojson.encode(out, [], 0, False)
        
        elif format == 'TopoJSON':
            ll = SphericalMercator().projLocation(Point(*self.bounds[0:2]))
            ur = SphericalMercator().projLocation(Point(*self.bounds[2:4]))
            topojson.encode(out, [], (ll.lon, ll.lat, ur.lon, ur.lat), False)
        
        else:
            raise ValueError(format)
