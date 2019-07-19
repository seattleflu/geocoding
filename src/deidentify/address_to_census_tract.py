#!/usr/bin/env python3
"""
Takes a file containing a list of addresses, one per line, and returns their
associated census tract within Washington.

To run:
    `./src/deidentify/address_to_census_tract.py {filename} --institute {institution}`
    or
    `./src/deidentify/address_to_census_tract.py {filename} --street {street column}`

Help:
    `./src/deidentify/address_to_census_tract.py --help`

Examples:
    `./src/deidentify/address_to_census_tract.py data/test/testset.json --institute default`
    `./src/deidentify/address_to_census_tract.py data/test/testset.csv -s address`

Requirements:
    shapely
    pandas
    xlrd (pandas Excel compatibility)
    smartystreets_python_sdk
"""
import os
import json
import click
import config
import pickle
import logging
import pandas as pd
from textwrap import dedent
from cachetools import TTLCache
from shapely.geometry import shape, Point
from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
from smartystreets_python_sdk.us_street import Lookup
from smartystreets_python_sdk.us_extract import Lookup as ExtractLookup

LOG = logging.getLogger(__name__)
CACHE_TTL = 60 * 60 * 24 * 28  # 4 weeks

@click.command()
@click.argument('filename', metavar="<filename>", required=True,
    type=click.Path(exists=True))
@click.option('-i', '--institute', metavar="<institute>",
    type=click.Choice(['uw', 'sch', 'default']), default='default',
    help='The acronym (lowercase) representing the institution.')
@click.option('-s', '--street', default=None,
    help='Key name for address street. Can also accept an entire address as free text')
@click.option('--street2', default=None,
    help='Key name for address street second line')
@click.option('--secondary', default=None,
    help='Key name for address secondary information/line 3')
@click.option('-c', '--city', default=None,
    help='Key name for address city')
@click.option('--state', default=None,
    help='Key name for address state')
@click.option('-z', '--zipcode', default=None,
    help='Key name for address zipcode')
@click.option('-o', '--output', metavar="<output>", default=None,
    help='Name of output file. Defaults to None, printing results to stdout.')
    # TODO validate output extension?
    # TODO allow users to enter csv and return json? and vice versa
@click.option('--invalidate_cache', is_flag=True,
    help='Optional flag for invalidating cached responses')
@click.option('--keep_zipcode', is_flag=True,
    help='Optional flag for keeping zipcode from geocoded address')

def address_to_census_tract(filename, institute, output, invalidate_cache,
                            keep_zipcode, **kwargs):
    """
    Given a <filename>, de-identifies addresses in a CSV or XLSX document by
    converting them into census tracts. Prints a CSV or XLSX document with the
    original address information removed but with census tract added.

    Address configuration is imported from `config.py` according to the given
    <institute>. The default is to only look for one column or key named
    'address'. Institutional configurations can be modified in `config.py`.

    Address configurations can also be given on-the-fly via several keyword
    options. These options begin with the help text 'Key name for...'.

    By default, the resulting data is printed to stdout. This can be overridden
    with the <output> option for a new filename. Currently, only two possible
    <output> file extensions have been implemented: JSON and CSV. If providing
    address in a JSON file, please use a `.json` file extension in the given
    <output> option. Similarly, if providing address data in CSV or Excel
    format, please use a `.csv` file extension in the given <output> option.

    To reduce the total number of requests sent to SmartyStreets' geocoding API,
    responses (including negative response) are cached. To override the cache
    for a set of data, provide the `--invalidate_cache` flag at runtime.

    TODO keep_zipcode
    """
    address_to_census_tract_inner(filename, institute, output, invalidate_cache,
                                  keep_zipcode, **kwargs)

def address_to_census_tract_inner(filename, institute, output, invalidate_cache,
                                  keep_zipcode, **kwargs):
    """
    Raises a :class:`UnsupportedFileExtensionError` when a given *filename* is
    not supported.
    """
    custom_address_config = not all(arg is None for arg in kwargs.values())

    if custom_address_config:
        address_map = kwargs
    else:
        address_map = config.ADDRESS_CONFIG[institute.lower()]
        LOG.info(f"Using «{institute}» institutional configuration.")

    if filename.endswith('.json'):
        process_json(filename, output, address_map, invalidate_cache, keep_zipcode)
    elif filename.endswith(('.csv', '.xlsx', '.xls')):
        process_csv_or_excel(filename, output, address_map, invalidate_cache, keep_zipcode)
    else:
        raise UnsupportedFileExtensionError(dedent(f"""
        Unsupported file extension for file «{filename}».
        Please choose from one of the following file extensions:
            * .csv
            * .xls
            * .xlsx
            * .json
    """))

def process_json(filepath: str, output: str, address_map: dict,
                 invalidate_cache: bool, keep_zipcode: bool):
    """
    Given a *filepath* to a JSON file, processes the relevant keys containing
    address data (from *address_map*) and generates an extra key for census
    tract data. Raises a :class:`NoAddressDataFoundError` if the address mapping
    is invalid and yields no matching keys from the address data.

    If a given address is invalid, `census_tract` is left blank.

    If *invalidate_cache* is true, any attempt at loading cached data is
    overridden.

    Dumps the generated JSON data to stdout unless an *output* file path is
    given.

    TODO keep_zipcode

    To minimize costs, an address should only be looked up once (via
    :func:`lookup_address`).
    """
    with open(filepath, encoding = "UTF-8") as file:
        data = [ json.loads(line) for line in file ]

    tracts = load_geojson("data/geojsons/Washington_2016.geojson")
    cache = load_or_create_cache()

    to_save = []
    for record in data:
        result = process_json_record(record, address_map, tracts, cache,
                                     invalidate_cache, keep_zipcode)
        if output:
            to_save.append(result)
        else:
            print(json.dumps(result, sort_keys=True))

    save_cache(cache)

    if output:
        json.dump(to_save, open(output, mode='w'))

def process_csv_or_excel(filepath: str, output: str, address_map: dict,
                         invalidate_cache: bool, keep_zipcode: bool):
    """
    Given a *filepath* to a CSV or Excel file containing address data, generates
    an extra column for the addresses' census tract, removing the original,
    identifying address keys.

    If a given address is invalid, `census_tract` is left blank.

    If *invalidate_cache* is true, any attempt to lookup an item in the cache
    is overridden.

    Dumps the data to stdout unless an *output* file path is given.

    To minimize costs, an address should only be looked up once (via
    `lookup_address()`).

    # TODO keep_zipcode
    """
    df = load_csv_or_excel(filepath)
    tracts = load_geojson("data/geojsons/Washington_2016.geojson")
    cache = load_or_create_cache()

    address = address_data_csv_or_excel(df, address_map)
    address['std_address'] = address.apply(lambda x: standardize_address(x, address_map))

    response = geocode_address_csv_or_excel(address, cache, invalidate_cache)

    response.apply(lambda x: save_to_cache(x['std_address'], x['response'], cache), axis=1)

    # Drop identifiable address columns
    drop_columns = list(address_map.values())
    keep_zipcode and drop_columns.remove(address_map['zipcode'])

    df = df[[ col for col in list(df) if col not in drop_columns ]]
    df['census_tract'] = census_tract_csv_or_excel(response, tracts)

    dump_csv_or_excel(df, output)
    save_cache(cache)

def process_json_record(record: dict, address_map: dict, tracts,
                        cache: TTLCache, invalidate_cache: bool,
                        keep_zipcode: bool) -> dict:
    """
    Given a *record* dictionary representing a line from a JSON file of data,
    processes the relevant keys containing address data (from *address_map*)
    and generates an extra key for census tract data. Drops identifiable address
    keys from the original data.

    If a given address is invalid, `census_tract` is left blank.

    If *invalidate_cache* is true, any attempt at loading cached data is
    overridden.

    Dumps the generated JSON data to stdout unless an *output* file path is
    given.

    TODO keep_zipcode
    """
    address = address_data_json_record(record, address_map)
    LOG.info(f"Currently geocoding address {address}.")

    std_address = standardize_address(address, address_map)
    response = None
    if not invalidate_cache:
        response = check_cache(std_address, cache)
    response = geocode_uncached_address(response, std_address)
    save_to_cache(std_address, response, cache)

    # Drop identifiable address keys and add census tract
    drop_keys = list(address_map.values())
    keep_zipcode and drop_keys.remove(address_map['zipcode'])

    result = {k: record[k] for k in record if k not in drop_keys}
    result["census_tract"] = census_tract_json_record(response, tracts)
    return result

def address_data_json_record(record: dict, address_map: dict) -> dict:
    """
    Given a *record* from a JSON file, subset to address-relevant keys
    noted by the *address_map* and return these relevant keys separately.

    Raises a :class:`AddressTranslationError` if the *address_map* contains
    keys not present in the given *record*.
    Raises a :class:`NoAddressDataFoundError` if the data can not be subset.
    """
    address_keys = list(filter(None, address_map.values()))
    try:
        address = { key: record[key] for key in address_keys }
    except KeyError:
        raise AddressTranslationError(record.keys(), address_map)

    if not address:
        raise NoAddressDataFoundError(record.keys(), address_map)

    return address

def census_tract_json_record(response: dict, tracts) -> str:
    """
    Extract lat/lng from *response* object and return the affiliated census
    tract from the given *tracts* file of polygons
    """
    if not response or not (response['lat'] or response['lng']):
        LOG.warning("Failed to geocode address.")
        return

    latlng = [response['lat'], response['lng']]

    return latlng_to_polygon(latlng, tracts)

def load_csv_or_excel(filename: str) -> pd.DataFrame:
    """
    Given a *filename* to a CSV or XLS/XLSX file, returns it as a DataFrame.
    """
    if filename.endswith('.csv'):
        df = pd.read_csv(filename)
    else:
        df = pd.read_excel(filename)
    return df

def address_data_csv_or_excel(df: pd.DataFrame, address_map: dict) -> pd.Series:
    """
    Given a pandas DataFrame *df*, subset to address-relevant columns
    noted by the *address_map* and return these data separately.

    Raises a :class:`NoAddressDataFoundError` if no columns resulted from
    mapping the given data *df* to the API.

    Raises a :class:`AddressTranslationNotFoundError` if the *address_map*
    contains keys not present in the given data *df*.
    """
    address_columns = list(filter(None, address_map.values()))
    if not address_columns:
        raise NoAddressDataFoundError(df.columns, address_map)

    try:
        address_data = df[address_columns]
    except KeyError:
        raise AddressTranslationError(list(df), address_map)

    return pd.Series(address_data.to_dict(orient='records'))

def geocode_address_csv_or_excel(address: pd.DataFrame, cache: TTLCache,
                                 invalidate_cache: bool) -> pd.DataFrame:
    """
    # Check in cache first
    # Look up those not in cache
    """
    response = pd.DataFrame()
    response['response'] = None
    response['std_address'] = address['std_address']

    if not invalidate_cache:
        response['response'] = response['std_address'].apply(lambda x: check_cache(x, cache))

    response['response'] = response.apply(lambda x:
        geocode_uncached_address(x['response'], x['std_address']), axis=1)

    return response

def geocode_uncached_address(response: dict, std_address: dict) -> dict:
    """
    Given a an empty *response* that did not come from the cache for a given
    *std_address*, looks it up the *std_address* using the SmartyStreets US
    address API. If after this initial lookup, the API response is still empty,
    the address was considered invalid. A second attempt is then made to lookup
    the address using the SmartyStreets extract address API. The response,
    whether still empty or not, is returned at the end.
    """
    if type(response) == dict:  # Was stored in cache
        return response

    response = lookup_address(std_address)

    if not response:
        LOG.info(f"No match found. Extracting address from text.")
        response = extract_address(std_address)

    if not response:
       LOG.warning(f"Could not look up address.")

    return response

def census_tract_csv_or_excel(response: pd.DataFrame, tracts) -> pd.Series:
    """
    Extract lat/lng from *response* DataFrame and return a pd.Series containing
    the affiliated census tract from the given *tracts* file of polygons.
    """
    lat = response['response'].apply(lambda x: x.get('lat', None))
    lng = response['response'].apply(lambda x: x.get('lng', None))
    latlng = pd.Series(list(zip(lat, lng)))
    return latlng.apply(lambda x: latlng_to_polygon(x, tracts))

def dump_csv_or_excel(df: pd.DataFrame, output: str):
    """
    Given a DataFrame *df*, prints it to a given *output* filename. If *output*
    is empty, prints *df* to stdout.
    """
    df.to_csv(output, index=False) if output else print(df.to_csv(index=False))

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
    Tries to load a pickled cache from the filepath `cache.pickle`. If a cache
    is not found at this location, creates a new one. Returns the cache.
    """
    try:
        cache = pickle.load(open('cache.pickle', mode='rb'))
    except FileNotFoundError:
        LOG.warning("Couldn't find an existing cache file. Creating new cache.")
        cache = TTLCache(maxsize=100000, ttl=CACHE_TTL)
    return cache

def check_cache(address: dict, cache: TTLCache) -> dict:
    """
    Given an *address* and a *cache*, checks if the *cache* exists. If it does,
    returns the given value of the *address* key in the *cache*. Returns nothing
    if the *address* key does not exist in the *cache*.
    """
    if cache:
        try:
            return cache[json.dumps(address, sort_keys=True)]
        except KeyError:
            LOG.warning("Item not found in cache.")
            pass
    else:
        LOG.warning("Cache does not exist or is empty.")

def save_to_cache(standardized_address: dict, response: dict, cache: TTLCache):
    """
    Given a *standardized_address* and its related *response* from the
    SmartyStreets API, stores them as a key-value pair in the given *cache*,
    overwriting the value for the existing *standardized_address* key if it
    already existed in the *cache*.
    """
    cache[json.dumps(standardized_address, sort_keys=True)] = response

def save_cache(cache: TTLCache):
    """ Given a *cache*, saves it to a hard-coded file `cache.pickle`. """
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
    LOG.info("""Pinging SmartyStreets geocoding API""")
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

    if not result:
        return {}

    return first_candidate_data(result)

def us_street_lookup(address: dict) -> Lookup:
    """
    Creates and returns a SmartyStreets US Street API Lookup object for a given
    *address*.

    Raises a :class:`InvalidAddressMappingError` if a Lookup property from the
    SmartyStreets geocoding API is not present in the given *address* data.
    """
    lookup = Lookup()
    try:
        lookup.street = address['street']
        lookup.street2 = address['street2']
        lookup.secondary = address['secondary']
        lookup.city = address['city']
        lookup.state = address['state']
        lookup.zipcode = address['zipcode']
    except KeyError as e:
        raise InvalidAddressMappingError(e)

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
    for key in address:
        address[key] = str(address[key]).upper().strip()

    standardized_address = {}
    for key in api_map:
        if key:
            standardized_address[key] = api_map[key] and address[api_map[key]]

    return standardized_address

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
    LOG.warning("Previous lookup failed. Looking up address as free text.")

    client = smartystreets_client_builder().build_us_extract_api_client()
    address_text = ', '.join([ str(val) for val in list(address.values()) if val ])

    lookup = ExtractLookup()
    lookup.text = address_text

    result = client.send(lookup)
    addresses = result.addresses

    for address in addresses:
        if len(address.candidates) > 0:
            result = address.candidates
            return first_candidate_data(result)
    return {}

def first_candidate_data(result) -> dict:
    """
    Given an address response from SmartyStreets geocoding API, parse the
    lat/lng information and return it.
    """
    first_candidate = result[0]
    return {
        'lat': first_candidate.metadata.latitude,
        'lng': first_candidate.metadata.longitude,
        #'zipcode': first_candidate.components.zipcode,  # TODO
        #'plus4_code': first_candidate.components.plus4_code
    }

def latlng_to_polygon(latlng: list, polygons):
    """
    Find the first polygon in *polygons* (loaded from file) which contains the
    *latlng* and return the polygon GEOID (the nationally-unique census tract
    identifier), else None.
    """
    # Ye olde lat/lng vs. lng/lat schism rears its head.
    lat, lng = latlng

    point = Point(lng, lat)

    for polygon in polygons:
        if polygon["shape"].contains(point):
            return polygon.get("properties", {}).get("GEOID")

    LOG.warning(f"Failed to find tract for {latlng}.")
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
    Raised by :func:`us_street_lookup` when a an *address_key* used in the
    SmartyStreets geocoding API Lookup object is not present in the data to be
    transformed.
    """
    def __init__(self, address_key):
        self.address_key = address_key

    def __str__(self):
        return dedent(f"""
        {self.address_key} not found in the address mapping.
        Is there an error in your `config.py`?
        """)


class AddressTranslationError(InvalidAddressMappingError):
    """
    Raised by :func:`address_data_json_record` and
    :func:`address_data_csv_or_excel` when a given key in the *api_map* does not
    exist in the given *address_keys*.
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

            Did you forget to provide an institution or a custom configuration?
            Please check your address mapping in `config.py` or run
                `src/address_to_census_tract.py --help`
            and try again.
                """)


class NoAddressDataFoundError(InvalidAddressMappingError):
    """
    Raised by :func:`address_data_json_record` and
    :func:`address_data_csv_or_excel` when no keys from the given
    *data_key_names* can be mapped to values in the given *address_map*.
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
            If not, please check your address mapping in `config.py` or run
                `src/address_to_census_tract.py --help`
            and try again.
            """)


if __name__ == '__main__':
    address_to_census_tract()
