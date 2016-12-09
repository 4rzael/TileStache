'''
Sample usage in a TileStache configuration, for a layer with no results at
zooms 0-9, basic selection of lines with names and highway tags for zoom 10,
a remote URL containing a query for zoom 11, and a local file for zooms 12+:

  "provider":
  {
    "class": "TileStache.Goodies.SparkVecTiles:Provider",
    "kwargs":
    {
      "dbinfo":
      {
        "host": "localhost",
        "user": "gis",
        "password": "gis",
        "database": "gis"
      },
      "queries":
      [
        null, null, null, null, null,
        null, null, null, null, null,
        "SELECT way AS geometry, highway, name FROM planet_osm_line -- zoom 10+ ",
        "http://example.com/query-z11.pgsql",
        "query-z12-plus.pgsql"
      ]
    }
  }
'''

from .server import Provider
