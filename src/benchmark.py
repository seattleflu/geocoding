import timeit
from address_to_census_tract import lookup_address
import config

default = config.ADDRESS_CONFIG['default']

one = timeit.timeit('lookup_address({"address": "2718 14th Ave S Apt B, Seattle, WA 98144"}, default)', number=1)
print(one)
# 0.0012873420000687474
