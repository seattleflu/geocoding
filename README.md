# Geocoding addresses into 2016 Census tracts

`src/deidentify/address_to_census_tract.py` prototypes address to Census tract conversion.  It uses a two-step process:

1. Geocode a given single-line address to a (latitude, longitude) pair using a
   remote (forward) geocoding service.

   We're currently using SmartyStreets' service, but this is swappable.  The
   important point is the quality and robustness of the geocoding given dirty
   data.  Even rough geocoding results can be useful though, as Census tracts
   are fairly large.  I believe that as long as we submit addresses free of
   any linkage to other data, we can use a commercially-available geocoding
   service without PHI concerns, c.f.  [discussion on
   #data-transfer](https://seattle-flu-study.slack.com/archives/CDTUFFQCU/p1544570425008700).

2. Find the Census tract polygon containing the geocoded (latitude, longitude).

   This can be done locally very easily given an off-the-shelf geospatial
   library and publicly available Census geodata.

Note that the geocoding result may have variable specificity.  If an address
can only be geocoded to "Seattle", then the geocoder may return Seattle's
centroid.  This would artificially inflate the Census tracts containing the
centroids returned for non-specific locations.

We're not using the [Census' own
geocoder](https://www.census.gov/geo/maps-data/data/geocoder.html), even
though it can go directly to a Census tract, because its address coverage
isn't great and it is not robust to bad addresses. As noted by the Census' own
documentation, this is especially for business addresses, which we will be
collecting as places of work.

**Need help?** Try running
```sh
src/deidentify/address_to_census_tract.py --help
```

## Data

Stored in the `data/` directory.

* `states.txt` contains a mapping of ANSI FIPS codes and USPS abbreviations for
  all states, as described at
  <https://www.census.gov/geo/reference/ansi_statetables.html>.  This file was
  downloaded unmodified from
  <http://www2.census.gov/geo/docs/reference/state.txt>.

* `omitted_states.txt` lists the _names_ of states we want to omit from
  downloads of census tracts, with reasons provided in comments.

* `tracts/tl_2016_${state_fips_code}_tract.{dbf,prj,shp,shp.xml,shx}` are the
  2016 Census tract Shapefiles and supporting files for each state, as
  described at <https://www.census.gov/geo/maps-data/data/tiger-line.html>.

  These are not checked into version control and must be downloaded locally by
  running `snakemake tracts`.  Do not download the files in parallel or
  repeatedly or the Census will likely ban your IP address!

* `tracts/tl_2016_${state_fips_code}_tract.geojson` are the Shapefiles
  converted to GeoJSON, which is a slightly more useful format.

  These are not checked into version control and must be converted locally by
  running `snakemake geojsons`.  `ogr2ogr` must be installed.

## Development

If you have [conda] installed, then install the project dependencies using
`conda env create -f geocoding_env_conda.yaml`. There is one additional
requirement not available through conda that needs to be installed. While
inside of your `geocoding` conda environment, please run
`pip install smartystreets-python-sdk`.

To use SmartyStreet's geocoding service, users must add an authentication key
and token as environment variables. Authentication keys are provided to users
who [register at their website][SmartyStreets]. Collaborators in the Seattle Flu
Study can request authentication keys from Thomas.

You can either [add persistent environment variables to your conda environment] or declare
environment variables at the command line via:

        export SMARTYSTREETS_AUTH_ID='UNIQUE_AUTHENTICATION_ID'
        export SMARTYSTREETS_AUTH_TOKEN='UNIQUE_AUTHENTICATION_TOKEN'

### Dummy data
During development, you may choose to use fake data stored in the `data/test`
directory for testing purposes.

* `pii_testset.csv` contains generated, personally-identifiable information from
  the API mocking tool [Mockaroo].

* `testset.json` contains ten addresses across Seattle in `ndjson` format that
  should all produce census tracts. If you cannot get the `geocoding` program to
  generate census tracts with this dataset, then there is something wrong with
  your install.

* `testset.csv` contains the same information as `testset.json` but in `csv`
  format for the `csv` workflow.
  

[conda]: https://conda.io/en/latest/
[add persistent environment variables to your conda environment]:
https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#saving-environment-variables
[SmartyStreets]: http://smartystreets.com
[Mockaroo]: http://www.mockaroo.com
