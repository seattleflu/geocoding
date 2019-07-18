from address_to_census_tract import address_to_census_tract_inner
from pii_deidentifier import pii_deidentifier_inner
import click


@click.command()
@click.argument('filepath', required=True, type=click.Path(exists=True))
@click.option('-i', '--institute', type=click.Choice(['uw', 'sch', 'default']),
    default='default', help='The acronym (lowercase) representing the institution.')
@click.option('-o', '--output', default=None,
    help='Name of output file. Defaults to None, printing results to stdout.')
    # TODO validate output extension?
    # TODO allow users to enter csv and return json? and vice versa
@click.option('--invalidate-cache', is_flag=True,
    help='Optional flag for invalidating cached responses')

def deidentify(filepath, institute, output, invalidate_cache):
    address_to_census_tract_inner(filepath, institute, output, invalidate_cache,
                                  keep_zipcode=True)
    pii_deidentifier_inner(filepath, institute)


if __name__ == '__main__':
    deidentify()
