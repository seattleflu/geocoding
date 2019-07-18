#!/usr/bin/env python3
"""

"""
import os
import re
import json
import click
import config
import hashlib
import logging
import pandas as pd

LOG = logging.getLogger(__name__)

@click.command()
@click.argument('filepath', required=True, type=click.Path(exists=True))
@click.option('-i', '--institute', type=click.Choice(['uw', 'sch', 'default']),
    default='default', help='The acronym representing the institution.')
@click.option('-n', '--name', default=None, help='')
@click.option('-d', '--dob', default=None, help='')
@click.option('-g', '--gender', default=None, help='')
@click.option('-p', '--postal-code', default=None, help='')

def pii_deidentifier(filepath, institute, **kwargs):
    pii_deidentifier_inner(filepath, institute, **kwargs)

def pii_deidentifier_inner(filepath, institute, **kwargs):
    # TODO Check if file ends with '.csv' or '.excel'
    df = pd.read_csv(filepath)
    pii_map = config.PII_CONFIG[institute.lower()]
    print(pii_map)

    # TODO make this part a function (copied from address_to_..)
    # Subset to address-relevant columns (from config) and store separately
    pii_columns = [ col for col in df.columns if col in pii_map.values() ]
    if not pii_columns:
        pass
        #raise NoPIIDataFoundError(df.columns(), pii_map)
    pii_data = df[pii_columns]
    pii = pd.Series(pii_data.to_dict(orient='records'))
    print(pii)

    pii['std_pii'] = pii.apply(lambda x: standardize_pii(x, pii_map))
    pii['individual'] = pii['std_pii'].apply(generate_hash)
    print(pii.individual.head())

def generate_hash(pii: dict):
    """
    TODO
    """
    pii_string = ' '.join(pii.values())

    secret = os.environ['PARTICIPANT_DEIDENTIFIER_SECRET']
    new_hash = hashlib.sha256()
    new_hash.update(pii_string.encode('utf-8'))
    new_hash.update(secret.encode('utf-8'))
    return new_hash.hexdigest()


def standardize_pii(pii: dict, api_map: dict) -> dict:
    """
    TODO the bottom part can be done in a loop
    Raises a KeyError if a mapped key from *api_map* does not exist in
    *pii*.
    """
    if not set(pii.keys()).issubset(api_map.values()):
        #raise PIITranslationNotFoundError(pii.keys(), api_map)
        pass
    for key in pii:
        pii[key] = str(pii[key]).upper().strip()

    # TODO
    return {
        'name': api_map['name'] and re.sub('[^a-zA-Z]+', '', pii[api_map['name']]),  # careful not to apply this to gender (when binary) or zipcode
        'birth-date': api_map['birth-date'] and pii[api_map['birth-date']],
        'gender': api_map['gender'] and pii[api_map['gender']],
        'postal-code': api_map['postal-code'] and pii[api_map['postal-code']],
    }


if __name__ == '__main__':
    pii_deidentifier()
