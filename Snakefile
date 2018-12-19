from collections import namedtuple


# Static states data
State = namedtuple("State", ("fips_code", "usps_code", "name", "gnisid"))

with open("data/omitted_states.txt", encoding = "UTF-8") as file:
    omitted_states = list(
        filter(lambda line: line and not line.startswith("#"),
            map(str.rstrip,
                file)))

with open("data/states.txt", encoding = "UTF-8") as file:
    states = list(
        filter(
            lambda state: state.name not in omitted_states, (
                State(*line.rstrip("\n").split("|"))
                   for line_number, line in enumerate(file, 1)
                    if line_number != 1)))


# Download 2016 Census tracts for all states.
rule tracts:
    message: "Downloading Census tracts for all states"
    input:
        expand("data/tracts/tl_2016_{state.fips_code}_tract.shp",
            state = states)

rule state_tracts:
    message: "Downloading tracts for {wildcards.state_code}"
    output:
        temp("data/tracts/tl_2016_{state_code}_tract.zip")
    shell:
        """
        wget -P data/tracts/ \
             -N https://www2.census.gov/geo/tiger/TIGER2016/TRACT/tl_2016_{wildcards.state_code:q}_tract.zip
        """

rule unpack_state_tracts:
    message: "Unpacking tracts for {wildcards.state_code}"
    input:
        rules.state_tracts.output
    output:
        "data/tracts/tl_2016_{state_code}_tract.cpg",
        "data/tracts/tl_2016_{state_code}_tract.dbf",
        "data/tracts/tl_2016_{state_code}_tract.prj",
        "data/tracts/tl_2016_{state_code}_tract.shp.xml",
        "data/tracts/tl_2016_{state_code}_tract.shp.iso.xml",
        "data/tracts/tl_2016_{state_code}_tract.shp.ea.iso.xml",
        "data/tracts/tl_2016_{state_code}_tract.shx",
        shapefile = "data/tracts/tl_2016_{state_code}_tract.shp"
    shell:
        """
        unzip -u -d data/tracts/ {input:q}
        chmod u=rw,go=r {output:q}
        """


# Convert to GeoJSON
rule geojsons:
    message: "Converting Census tracts for all state to GeoJSON"
    input:
        expand("data/tracts/tl_2016_{state.fips_code}_tract.geojson",
            state = states)

rule state_geojson:
    message: "Converting tracts for {wildcards.state_code} to GeoJSON"
    input:
        rules.unpack_state_tracts.output.shapefile
    output:
        "data/tracts/tl_2016_{state_code}_tract.geojson"
    shell:
        """
        ogr2ogr -f GeoJSON {output:q} {input:q}
        """
