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
    input:
        expand("data/tracts/tl_2016_{state.fips_code}_tract.shp",
            state = states)

rule state_tracts:
    output:
        temp("data/tracts/tl_2016_{state_code}_tract.zip")
    shell:
        """
        wget -P data/tracts/ \
             -N https://www2.census.gov/geo/tiger/TIGER2016/TRACT/tl_2016_{wildcards.state_code:q}_tract.zip
        """

rule unpack_state_tracts:
    input:
        rules.state_tracts.output
    output:
        "data/tracts/tl_2016_{state_code}_tract.cpg",
        "data/tracts/tl_2016_{state_code}_tract.dbf",
        "data/tracts/tl_2016_{state_code}_tract.prj",
        "data/tracts/tl_2016_{state_code}_tract.shp",
        "data/tracts/tl_2016_{state_code}_tract.shp.xml",
        "data/tracts/tl_2016_{state_code}_tract.shp.iso.xml",
        "data/tracts/tl_2016_{state_code}_tract.shp.ea.iso.xml",
        "data/tracts/tl_2016_{state_code}_tract.shx"
    shell:
        """
        unzip -u -d data/tracts/ {input:q}
        chmod u=rw,go=r {output:q}
        """
