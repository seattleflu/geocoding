# Geocoding addresses into 2016 Census tracts

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
  running `snakemake tracts`.
