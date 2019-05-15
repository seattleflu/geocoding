ADDRESS_CONFIG = {
 'uw': {
    'street': 'AddressLine1',
    'street2': 'AddressLine2',
    'secondary': 'AddressLine3',
    'city': 'City',
    'state': 'StateText',
    'zipcode': 'PostalCode'
    },
 'sch': {
     'street': 'ADD_LINE_1',
     'street2': 'ADD_LINE_2',
     'street3': 'ADD-LINE_3',
     'city': 'CITY',
     'state': 'ABBR',
     'zipcode': 'ZIP'
 },
 'kp': {},
 'default': {
     'street': 'address',
     'street2': None,
     'secondary': None,
     'city': None,
     'state': None,
     'zipcode': None
 }
}

PII_CONFIG = {
    'default': {
        'name': 'Patient Name',
        'birth-date': 'DOB',
        'gender': 'Gender',
        'postal-code': 'Postal Code'
    },
    'sch': {}
}
