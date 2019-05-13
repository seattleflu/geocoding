#!/usr/bin/env python3
"""
Takes a file containing addresses, one per line, and returns the data with a new
    column or key containing an associated census tract. Removes the original
    identifying address data.

To run:
    `./src/address_to_census_tract.py {filepath} --institute {institution}`
    or
    `./src/address_to_census_tract.py {filepath} --street {street column}`

Help:
    `./src/address_to_census_tract.py --help`

Examples:
    `./src/address_to_census_tract.py data/test/testset.json --institute default`
    `./src/address_to_census_tract.py data/test/testset.csv -s address`

Requirements:
    shapely
    pandas
    xlrd (pandas Excel compatibility)
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
import config
import click

LOG = logging.getLogger(__name__)

@click.command()
@click.argument('filepath', required=True, type=click.Path(exists=True))
@click.option('-i', '--institute', type=click.Choice(['UW', 'default']), 
    default='default', help='The acronym representing the institution.')
@click.option('-s', '--street', default=None, 
    help='Key name for address street. Can also accept an entire address as free text')
@click.option('--street2', default=None, 
    help='Key name for address street second line')
@click.option('--secondary', default=None, 
    help='Key name for address secondary information (e.g. address line 3)')
@click.option('-c', '--city', default=None, 
    help='Key name for address city')
@click.option('--state', default=None, 
    help='Key name for address state')
@click.option('-z', '--zipcode', default=None, 
    help='Key name for address zipcode')


def address_to_census_tract(filepath, institute, **kwargs):
    """
    Given a *filepath* to a JSON, CSV, or Excel file (XLSX or XLS), de-identifies
    addresses contained within the data by converting them to census tracts and
    then removing the original address data. 

    An *institute*-specific configuration mapping key names of addresses to
    SmartyStreets geocoding API is loaded if the optional *kwargs* specifying
    custom configuration are all None.

    If optional *kwargs* are used, then then the program ignores the *institute*
    declaration and proceeds with the custom configuration.

    Note: If providing more than one, optional keyword argument, it is CRITICAL 
    to provide them in the following order: \n
        -s/--street \n
        --street2 \n
        --secondary \n
        -c/--city \n
        --state \n
        -z/--zipcode
    """

    # TODO validate that no two keywords have the same value 

    custom_address_config = not all(arg is None for arg in kwargs.values())

    if custom_address_config:
        address_map = kwargs
    else:
        address_map = config.ADDRESS_CONFIG[institute.lower()]
        LOG.info(f"Using «{institute}» institutional configuration.")

    if filepath.endswith('.json'):
        process_json(filepath, address_map) 
    elif filepath.endswith(('.csv', '.xlsx', '.xls')):
        process_csv_or_excel(filepath, address_map)
    else:
        raise UnsupportedFileExtensionError(dedent(f"""
        Unsupported file extension for file «{filepath}». 
        Please choose from one of the following file extensions:
            * .csv 
            * .xls
            * .xlsx
            * .json
    """))

def process_json(file_path: str, address_map: dict):
    """
    Given a *file_path* to a JSON file, processes the relevant keys containing
    address data (from *address_map*) and generates an extra key for census 
    tract data. Raises a :class:`NoAddressDataFoundError` if the address mapping
    is invalid and yields no matching keys from the address data. 
    
    If a given address is invalid, `census_tract` is left blank. 
    
    To minimize costs, an address should only be looked up once (via 
    :func:`lookup_address`).

    Dumps the generated JSON data to stdout. 
    """

    with open(file_path, encoding = "UTF-8") as file:
        data = [ json.loads(line) for line in file ]

    tracts = load_geojson("data/geojsons/Washington_2016.geojson")

    for record in data:
        # Subset to address-relevant columns only and store separately
        address = { key: record[key] for key in record if key in address_map.values() }

        if not address: 
            raise NoAddressDataFoundError(record.keys(), address_map)  
            
        response = lookup_address(address, address_map)
        latlng = None
        tract = None

        # Extract lat/lng from response object 
        if response and response['lat'] and response['lng']:
            latlng = [response['lat'], response['lng']]
            tract = latlng_to_polygon(latlng, tracts)

        else:
            LOG.warning(dedent(f"""
            Failed to geocode {address}.
            """))

        # Drop identifiable address keys 
        result = {k: record[k] for k in record if k not in address}
        result["census_tract"] = tract

        print(json.dumps(result))

def process_csv_or_excel(file_path: str, address_map: dict):
    """
    Given a *file_path* to a CSV or Excel file, processes the relevant columns
    containing address data (from *address_map*) and generates an extra column 
    census tract data. Raises a :class:`NoAddressDataFoundError` if the address
    mapping is invalid and yields no matching columns on the address data.
    
    If a given address is invalid, `census_tract` is left blank. 
    
    Saves the new table at `data/test/out.csv`. 

    To minimize costs, an address should only be looked up once (via 
    :func:`lookup_address`).
    """
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    tracts = load_geojson("data/geojsons/Washington_2016.geojson")

    # Subset to address-relevant columns only and store separately
    address_columns = [ col for col in df.columns if col in address_map.values() ]
    if not address_columns:
        raise NoAddressDataFoundError(df.columns, address_map)  
  
    address = pd.Series(df[address_columns].to_dict(orient='records'))
    response = pd.Series(address.apply(lambda x: lookup_address(x, address_map)))

    # Extract lat/lng from response object 
    df['lat'] = response.apply(lambda x: x and x['lat'])
    df['lng'] = response.apply(lambda x: x and x['lng'])
    latlng = pd.Series(list(zip(df['lat'], df['lng'])))

    df['census_tract'] = latlng.apply(lambda x: latlng_to_polygon(x, tracts))
    
    # Drop identifiable address columns
    df = df.drop(columns=address_columns + ['lat', 'lng'])
    df.to_csv('data/test/out.csv', index=False) 

def lookup_address(address: dict, address_map: dict) -> dict:
    """
    Given an *address*, returns a dict containing a standardized address and 
    lat/long coordinates from the first address candidate from SmartyStreets 
    US Street geocoding API.

    Note that this functionality works regardless of whether a given address is 
    broken into pieces (street, city, zipcode, etc.) or is a free text lookup 
    (and only the `street` parameter is used).
    """
    auth_id = os.environ['SMARTYSTREETS_AUTH_ID']
    auth_token = os.environ['SMARTYSTREETS_AUTH_TOKEN']

    credentials = StaticCredentials(auth_id, auth_token)
    client = ClientBuilder(credentials).build_us_street_api_client()
    
    lookup = us_street_lookup(address, address_map)
    if not lookup.street:
        LOG.warning(dedent(f"""
        No given street address for {address}. 
        Currently lookups are only possible with a street address.
        """))
        return

    try:
        client.send_lookup(lookup)
    except exceptions.SmartyException as err:
        LOG.exception(err)
        return

    result = lookup.result
    if not result:  # Invalid address. Try again.
        LOG.info(dedent(f"""
        No match found for given address. Extracting address from text
        """))
        address_values = ', '.join([ str(val) for val in list(address.values()) if val ])
        result = extract_address(credentials, address_values) 
        if not result:
            LOG.warning(dedent(f"""
            Could not look up address {address}.
            """))
            return 

    return {"lat": result[0].metadata.latitude,
            "lng": result[0].metadata.longitude}

def us_street_lookup(address: dict, api_map: dict) -> Lookup:
    """
    Creates and returns a SmartyStreets US Street API Lookup object for a given 
    *address*. The *address* keys are mapped to the SmartyStreets API using the
    given *api_map*. 

    Raises a AddressTranslationNotFoundError if a mapped key from *api_map* 
    does not exist in *address*.
    """
    truthy_api_map_values = set( api_map[key] for key in api_map if api_map[key] )
    if not truthy_api_map_values.issubset(set(address.keys())):
        raise AddressTranslationNotFoundError(address.keys(), api_map)

    lookup = Lookup()

    lookup.street = api_map['street'] and address[api_map['street']]
    lookup.street2 = api_map['street2'] and address[api_map['street2']]
    lookup.secondary = api_map['secondary'] and address[api_map['secondary']]
    lookup.city = api_map['city'] and address[api_map['city']]
    lookup.state = api_map['state'] and address[api_map['state']]
    lookup.zipcode = api_map['zipcode'] and address[api_map['zipcode']]

    lookup.candidates = 1
    lookup.match = "Invalid"  # Most permissive
    return lookup
    
def extract_address(credentials, text: str):
    """
    Given arbitrary *text* and *credentials* to SmartyStreets' geocoding APIs, 
    returns a result from the SmartyStreets US Extract API containing 
    information about an address connected to the text.

    Note that this API is not consistent with the US Street API, and the lookup
    and responses must be handled differently.

    Assumes addresses are given in the correct order # TODO enforce
    """
    client = ClientBuilder(credentials).build_us_extract_api_client()
    lookup = ExtractLookup()
    lookup.text = text

    result = client.send(lookup)    
    addresses = result.addresses
    for address in addresses:
        return address.candidates

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

    LOG.warning(dedent(f"""
    Failed to find tract for {latlng}.
    """))
    return None

def load_geojson(geojson_filename):
    """Read GeoJSON file and return a list of features converted to shapes."""

    with open(geojson_filename) as file:
        geojson = json.load(file)

    return [
        { "properties": feature["properties"], "shape": shape(feature['geometry']) }
            for feature in geojson["features"]
    ]


class UnsupportedFileExtensionError(ValueError):
    """
    Raised by :func:`address_to_census_tract` when the given filepath ends with 
    an unsupported extension.
    """
    pass


class InvalidAddressMappingError(KeyError):
    """
    Base class for errors related to the address mapping from config.

    *address_keys* and *address_map* can be used by child classes for custom
    error messages.

    """
    def __init__(self, address_keys: list, address_map: dict):
        self.address_keys = address_keys
        self.address_map = address_map


class AddressTranslationNotFoundError(InvalidAddressMappingError):
    """
    Raised by :func:`us_street_lookup` when a given *api_map* contains a key 
    with a truthy value but the key is not present among the given address keys.
    """
    def __str__(self):
        return dedent(f"""
            The address map contains values not present in the given address.
            
            Address keys are:
                {list(self.address_keys)}
            
            No match found for 
                {[ self.address_map[key] for key in self.address_map if self.address_map[key] 
                   and self.address_map[key] not in self.address_keys ]}

            The address mapping is:
                {json.dumps(self.address_map, indent=16)}
            """)


class NoAddressDataFoundError(InvalidAddressMappingError):
    """
    Raised by :func:`process_json` or :func:`process_csv_or_excel` when the 
    address configuration from `config` does not map to any keys or columns on 
    the given data.
    """
    def __str__(self):
        return dedent(f"""\n
            Could not find any address data using the address mapping:
                {json.dumps(self.address_map, indent=16)}
            These keys were considered when looking for an address:
                {list(self.address_keys)}

            Did you forget to provide an institution or a custom configuration?
            Please check your address mapping in `config.py` or run
                `src/address_to_census_tract.py --help` 
            and try again.
            """)


if __name__ == '__main__':
    address_to_census_tract()
