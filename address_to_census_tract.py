#!/usr/bin/env python3
"""
Takes a file containing a list of addresses, one per line, and returns their
associated census tract within Washington.

To run:
    ./address_to_census_tract.py {filepath}

Examples:
    ./address_to_census_tract.py data/test/testset.json
    ./address_to_census_tract.py data/test/testset.csv 

Requirements:
    shapely
    pandas
    smartystreets_python_sdk
"""
import json
import os
from sys import argv
from textwrap import dedent
from shapely.geometry import shape, Point
import logging
import pandas as pd
from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup
from smartystreets_python_sdk.us_extract import Lookup as ExtractLookup

LOG = logging.getLogger(__name__)


def main():
    if len(argv) == 0:
        raise IndexError("A filename argument is required")
    if argv[1].endswith('.json'):
        process_json(argv[1])
    elif argv[1].endswith('.csv'):
        process_csv(argv[1])
    else:
        raise ValueError(dedent(f"""
        Unknown file extension for file named «{argv[1]}». 
        Please choose from one of the following file extensions:
            * .csv 
            * .json
    """))

def process_json(file_path: str):
    with open(file_path, encoding = "UTF-8") as file:
        testset = [ json.loads(line) for line in file ]

    tracts = load_geojson("data/geojsons/Washington_2016.geojson")

    for record in testset:
        address = record["address"]

        standard_address = None
        latlng = None
        tract = None
        response = lookup_address(address)

        if response and response['lat'] and response['lng']:
            standard_address = response['standard_address']
            latlng = [response['lat'], response['lng']]
            tract = latlng_to_polygon(latlng, tracts)

        else:
            LOG.warning(f"Failed to geocode {address}")

        result = {
            "address": standard_address,
            "latlng": latlng,
            "tract": tract,
        }

        print(json.dumps({ **record, "result": result }))

def process_csv(file_path: str):
    """
    Given a *file_path* to a CSV, processes the `address` column and adds 
    extra columns for a standardized address, latitude and longitude
    coordinates, and census tract. If a given address is invalid, these new 
    columns are left blank. Dumps the new table to stdout. 

    To minimize costs, an address should only be looked up once (via 
    `lookup_address()`).
    """
    df = pd.read_csv(file_path)
    tracts = load_geojson("data/geojsons/Washington_2016.geojson")

    response = pd.Series(df['address'].apply(lookup_address))

    # Parse response object to create columns of interest
    df['standard_address'] = response.apply(lambda x: x and x['standard_address'])
    df['lat'] = response.apply(lambda x: x and x['lat'])
    df['lng'] = response.apply(lambda x: x and x['lng'])

    latlng = pd.Series(list(zip(df['lat'], df['lng'])))
    df['final_tract'] = latlng.apply(lambda x: latlng_to_polygon(x, tracts))
    
    print(df.to_csv(index=False)) 

def smartystreets_client_builder():
    """
    Returns a new :class:`smartystreets_python_sdk.ClientBuilder` using
    credentials from the environment variables ``SMARTYSTREETS_AUTH_ID`` and
    ``SMARTYSTREETS_AUTH_TOKEN``.
    """
    auth_id = os.environ['SMARTYSTREETS_AUTH_ID']
    auth_token = os.environ['SMARTYSTREETS_AUTH_TOKEN']

    return ClientBuilder(StaticCredentials(auth_id, auth_token))

def lookup_address(address: str) -> dict:
    """
    Given an address, returns a dict containing a standardized address and 
    lat/long coordinates from SmartyStreet's US Street geocoding API.
    """
    client = smartystreets_client_builder().build_us_street_api_client()

    lookup = Lookup()
    lookup.street = address
    lookup.candidates = 1
    lookup.match = "Invalid"  # Most permissive
    
    try:
        client.send_lookup(lookup)
    except exceptions.SmartyException as err:
        LOG.exception(err)
        return

    result = lookup.result
    if not result:  # Invalid address
        result = extract_address(address)
        if not result:
            LOG.warning(f"Could not look up address {address}")
            return 

    first_candidate = result[0]

    return {"standard_address": standard_address(first_candidate),
            "lat": first_candidate.metadata.latitude,
            "lng": first_candidate.metadata.longitude}

def extract_address(text: str):
    """
    Given arbitrary *text*, returns a result from the SmartyStreet's US Extract
    geocoding API containing information about an address connected to the text.

    Note that this API is not consistent with the US Street API, and the lookup
    and responses must be handled differently.
    """
    client = smartystreets_client_builder().build_us_extract_api_client()
    lookup = ExtractLookup()
    lookup.text = text

    result = client.send(lookup)
    metadata = result.metadata
    
    addresses = result.addresses
    for address in addresses:
        return address.candidates

def standard_address(candidate) -> str:
    """
    Given a result object *candidate* from SmartyStreets geocoding API, return a 
    standardized address.
    """
    standard_address = candidate.delivery_line_1
    if candidate.delivery_line_2:
        standard_address += ' ' + candidate.delivery_line_2

    return standard_address + ' ' + candidate.last_line

def load_geojson(geojson_filename):
    """Read GeoJSON file and return a list of features converted to shapes."""

    with open(geojson_filename) as file:
        geojson = json.load(file)

    return [
        { "properties": feature["properties"], "shape": shape(feature['geometry']) }
            for feature in geojson["features"]
    ]

def latlng_to_polygon(latlng: list, polygons):
    """
    Find the first polygon in *polygons* (loaded from file) which contains the 
    *latlng* and return the polygon, else None.
    """
    
    # Ye olde lat/lng vs. lng/lat schism rears its head.
    lat, lng = latlng

    point = Point(lng, lat)

    for polygon in polygons:
        if polygon["shape"].contains(point):
            # GEOID is the nationally-unique tract identifier
            return polygon.get("properties", {}).get("GEOID") 

    LOG.warning(f"Failed to find tract for {latlng}")
    return None

if __name__ == '__main__':
    main()
