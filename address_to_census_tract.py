#!/usr/bin/env python3
"""
Takes a file containing a list of addresses, one per line, and returns their
associated census tract within Washington.

To run:
    ./address_to_census_tract.py

Requirements:
    geocoder
    shapely
"""
import geocoder
import json
from shapely.geometry import shape, Point
from sys import stderr


def main():
    with open("data/test/testset.json", encoding = "UTF-8") as file:
        testset = [ json.loads(line) for line in file ]

    tracts = load_geojson("data/geojsons/Washington_2016.geojson")

    for record in testset:
        address = record["address"]

        tract = None
        latlng = address_to_latlng(address)

        if latlng:
            tract = latlng_to_polygon(latlng, tracts)

            if not tract:
                print(f"failed to find tract for {latlng}", file = stderr)
        else:
            print(f"failed to geocode {address}", file = stderr)

        # GEOID is the nationally-unique tract identifier
        result = {
            "latlng": latlng,
            "tract": tract.get("properties", {}).get("GEOID") if tract else None,
        }

        print(json.dumps({ **record, "result": result }))


def address_to_latlng(address):
    """Convert an address string to a list of latitude, longitude coordinates.

    Currently uses Google's geocoder API for geocoding, but this could be
    replaced with other geocoder implementations.
    """
    return geocoder.google(address).latlng


def load_geojson(geojson_filename):
    """Read GeoJSON file and return a list of features converted to shapes."""

    with open(geojson_filename) as file:
        geojson = json.load(file)

    return [
        { "properties": feature["properties"], "shape": shape(feature['geometry']) }
            for feature in geojson["features"]
    ]


def latlng_to_polygon(latlng, polygons):
    """
    Find the first polygon in *polygons* which contains the *latlng* and return
    the polygon, else None.
    """

    # Ye olde lat/lng vs. lng/lat schism rears its head.
    lat, lng = latlng
    point = Point(lng, lat)

    for polygon in polygons:
        if polygon["shape"].contains(point):
            return polygon

    return None


if __name__ == '__main__':
    main()
