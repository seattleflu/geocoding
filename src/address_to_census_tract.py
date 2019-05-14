#!/usr/bin/env python3
"""
Takes a file containing a list of addresses, one per line, and returns their
associated census tract within Washington.

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
import pickle
from cachetools import TTLCache

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
@click.option('-o', '--output', default=None, help='Name of output file')
    # TODO validate output extension?
    # TODO allow users to enter csv and return json? and vice versa

def address_to_census_tract(filepath, institute, output, **kwargs):
    """
    """
    custom_address_config = not all(arg is None for arg in kwargs.values())

    if custom_address_config:
        address_map = kwargs
    else:
        address_map = config.ADDRESS_CONFIG[institute.lower()]
        LOG.info(f"Using «{institute}» institutional configuration.")

    if filepath.endswith('.json'):
        process_json(filepath, output, address_map) 
    elif filepath.endswith(('.csv', '.xlsx', '.xls')):
        process_csv_or_excel(filepath, output, address_map)
    else:
        raise UnsupportedFileExtensionError(dedent(f"""
        Unsupported file extension for file «{filepath}». 
        Please choose from one of the following file extensions:
            * .csv 
            * .xls
            * .xlsx
            * .json
    """))

def process_json(file_path: str, output: str, address_map: dict):
    """
    Given a *file_path* to a JSON file, processes the relevant keys containing
    address data (from *address_map*) and generates an extra key for census 
    tract data. 
    
    If a given address is invalid, `census_tract` is left blank. 
    
    Dumps the generated JSON data to stdout unless an *output* file path is
    given.

    TODO too long of a function
    """
    with open(file_path, encoding = "UTF-8") as file:
        data = [ json.loads(line) for line in file ]

    tracts = load_geojson("data/geojsons/Washington_2016.geojson")
    cache = load_or_create_cache()
    to_save = []

    for record in data:
        # Subset to address-relevant columns (from config) and store separately
        address = { key: record[key] for key in record if key in address_map.values() }
        if not address: 
            raise NoAddressDataFoundError(record.keys(), address_map)  
        
        std_address = standardize_address(address, address_map)
        response = check_cache(std_address, cache)
        
        if not response:  # Not in cache. Look up.
            response = lookup_address(std_address)

        if not response:  # Invalid address. Try again.
            LOG.info(dedent(f"""
            No match found for given address. Extracting address from text
            """))
            response = extract_address(std_address)

        if not response:
            LOG.warning(dedent(f"""
            Could not look up address {address}.
            """))
        else:
            # Store item in cache, possibly overwriting existing key
            cache[json.dumps(std_address)] = response

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

        if output:
            to_save.append(result)
        else:
            print(json.dumps(result))

    if output:
        json.dump(to_save, open(output, mode='w'))
    save_cache(cache)

def process_csv_or_excel(file_path: str, output: str, address_map: dict):
    """
    Given a *file_path* to a CSV or Excel file, processes the relevant columns
    containing address data (from *address_map*) and generates an extra column 
    census tract data. 
    
    If a given address is invalid, `census_tract` is left blank. 
    
    Dumps the data to stdout unless an *output* file path is given. 

    To minimize costs, an address should only be looked up once (via 
    `lookup_address()`).

    # TODO too long 
    """
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    tracts = load_geojson("data/geojsons/Washington_2016.geojson")
    cache = load_or_create_cache()

    # Subset to address-relevant columns (from config) and store separately
    address_columns = [ col for col in df.columns if col in address_map.values() ]
    if not address_columns:
        raise NoAddressDataFoundError(df.columns(), address_map)  
    address_data = df[address_columns]
    address = pd.Series(address_data.to_dict(orient='records'))

    response = pd.DataFrame()
    response['std_address'] = address.apply(lambda x: standardize_address(x, address_map))
    # Check in cache first
    response['response'] = response['std_address'].apply(lambda x: check_cache(x, cache))
    # Look up those not in cache 
    response['response'] = response.apply(lambda x: lookup_address(x['std_address']) 
                                            if not x['response'] else x['response'], axis=1)
    # Look up those that failed the last time
    response['response'] = response.apply(lambda x: extract_address(x['std_address'])
                                            if not x['response'] else x['response'], axis=1)

    # Extract lat/lng from response object 
    lat = response['response'].apply(lambda x: x and x['lat'])
    lng = response['response'].apply(lambda x: x and x['lng'])
    latlng = pd.Series(list(zip(lat, lng)))

    df['census_tract'] = latlng.apply(lambda x: latlng_to_polygon(x, tracts))
    
    # Drop identifiable address columns
    df = df.drop(columns=address_columns)
    
    if output:
        df.to_csv(output, index=False) 
    else:
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

def load_or_create_cache() -> TTLCache:
    """
    TODO
    """
    try:
        cache = pickle.load(open('cache.pickle', mode='rb'))
    except FileNotFoundError:
        LOG.info("Couldn't find an existing cache file. Creating new cache.")
        cache = TTLCache(maxsize=100000, ttl=6000)
    return cache

def check_cache(address: dict, cache: TTLCache) -> dict:
    """
    TODO
    """
    if cache:
        try: 
            return cache[json.dumps(address)]
        except KeyError:
            pass

def save_cache(cache: TTLCache):
    """
    TODO
    """
    pickle.dump(cache, open('cache.pickle', mode='wb'))

def lookup_address(address: dict) -> dict:
    """
    Given an *address* matching the SmartyStreets API, returns a dict containing
    a standardized address and lat/long coordinates from SmartyStreet's US 
    Street geocoding API.

    Note that this functionality works regardless of whether a given address is 
    broken into pieces (street, city, zipcode, etc.) or is a free text lookup 
    (and only the `street` parameter is used).
    """
    client = smartystreets_client_builder().build_us_street_api_client()
    result = None

    lookup = us_street_lookup(address)
    if not lookup.street:
        LOG.warning(dedent(f"""
        No given street address for {address}. 
        Currently lookups are only possible with a street address."""))
        return

    client.send_lookup(lookup)
    result = lookup.result

    if result:
        first_candidate = result[0]

        return {
            "lat": first_candidate.metadata.latitude,
            "lng": first_candidate.metadata.longitude
        }

def us_street_lookup(address: dict) -> Lookup:
    """
    Creates and returns a SmartyStreets US Street API Lookup object for a given 
    *address*. 
    """
    lookup = Lookup()

    lookup.street = address['street']
    lookup.street2 = address['street2']
    lookup.secondary = address['secondary']
    lookup.city = address['city']
    lookup.state = address['state']
    lookup.zipcode = address['zipcode']

    lookup.candidates = 1
    lookup.match = "Invalid"  # Most permissive
    return lookup

def standardize_address(address: dict, api_map: dict) -> dict:
    """
    Returns an address in a format that SmartyStreets API expects. The given 
    *address* keys are mapped to the SmartyStreets API using the given 
    *api_map*. 

    Raises a KeyError if a mapped key from *api_map* does not exist in 
    *address*.
    """
    if not set(address.keys()).issubset(api_map.values()):
        raise AddressTranslationNotFoundError(address.keys(), api_map)

    for key in address:
        address[key] = address[key].upper().strip() 

    return {
        'street': api_map['street'] and address[api_map['street']],
        'street2': api_map['street2'] and address[api_map['street2']],
        'secondary': api_map['secondary'] and address[api_map['secondary']],
        'city': api_map['city'] and address[api_map['city']],
        'state': api_map['state'] and address[api_map['state']],
        'zipcode': api_map['zipcode'] and address[api_map['zipcode']]
    }

def extract_address(address: dict):
    """
    Given an *address*, converts it to text and returns a result from
    the SmartyStreets US Extract API containing information about an address 
    connected to the text.

    Assumes that *address* keys, where present, are sorted in the following 
    order:
        * street
        * street2
        * secondary
        * city
        * state
        * zipcode

    Note that this API is not consistent with the US Street API, and the lookup
    and responses must be handled differently.
    """
    client = smartystreets_client_builder().build_us_extract_api_client()
    address_text = ', '.join([ str(val) for val in list(address.values()) if val ])

    lookup = ExtractLookup()
    lookup.text = address_text

    result = client.send(lookup)    
    addresses = result.addresses

    for address in addresses:
        first_candidate = address.candidates[0]

        return {
            'lat': first_candidate.metadata.latitude,
            'lng': first_candidate.metadata.longitude
        }

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

    LOG.warning(dedent(f"""
    Failed to find tract for {latlng}.
    """))
    return None

class UnsupportedFileExtensionError(ValueError):
    """
    Raised by `main` when the given filepath ends with an unsupported extension.
    """
    pass


class InvalidAddressMappingError(KeyError):
    """
    Raised by `process_json` or `process_csv_or_excel` when the address 
    configuration from `config` does not map to any keys or columns on the given
    data
    """
    pass


class AddressTranslationNotFoundError(InvalidAddressMappingError):
    """
    TODO 
    """
    def __init__(self, address_keys, api_map):
        self.address_keys = address_keys
        self.api_map = api_map
    
    def __str__(self):
        return dedent(f"""
            The address map contains values not present in the given address.
            
            Address keys are:
                {self.address_keys}
            
            The address mapping is:
                {self.api_map}
            """)


class NoAddressDataFoundError(InvalidAddressMappingError):
    """
    TODO
    """
    def __init__(self, data_key_names, address_map):
        self.data_key_names = data_key_names
        self.address_map = address_map

    def __str__(self):
        return dedent(f"""\n
                Could not find any address data among the following key names:
                    {list(self.address_map.values())}
                The given keys were:
                    {list(self.data_key_names)}
                Did you forget to provide an institution or a custom configuration?
                Please check your address mapping in `config.py` or run
                    `src/address_to_census_tract.py --help` 
                and try again.
                """)


if __name__ == '__main__':
    address_to_census_tract()
