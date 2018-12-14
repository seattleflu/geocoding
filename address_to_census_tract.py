"""address_to_census_tract.py

Takes a file containing a list of addresses, one per line, and returns their
associated census tract within Seattle.

To run:
	python address_to_census_tract.py

Requirements:

"""
import geocoder
import time

def read_addresses(fname):
	"""Read a TSV containing one address per line to a list of addresses."""
	print("Reading in addresses from {}".format(fname))
	addresses = []
	with open(fname, 'r') as f:
		# Skip the first line
		f.readline()
		for line in f.readlines():
			line = line.split('\t')
			address = line[0]
			addresses.append(address)
	print("Successfully read in {} addresses.".format(len(addresses)))
	return addresses

def read_expected(fname):
	"""Read in expected census tracts for easy comparisonself.

	This function is not necessary for the conversion to work, it is just
	helpful in bugfixing and testingself.
	"""
	exps = []
	with open(fname, 'r') as f:
		# Skip the first line
		f.readline()
		for line in f.readlines():
			line = line.split('\t')
			exp = line[1].strip()
			exps.append(exp)
	print("Successfully read in {} expected census tracts.".format(len(exps)))
	return exps

def print_latlngs(addresses):
	"""Print to screen all the lat/long values for a list of addresses."""
	latlngs = []
	for address in addresses:
		latlngs.append(address_to_latlng(address))

	for latlng in ltlngs:
		print(latlng)

# def latlng_to_census_tract(latlng, geojson):
# 	"""Convert a lat/long value to a census tract."""
# 	### No longer in use. Replaced by better optimized `latlngs_to_census_tracts`
# 	import json
# 	from shapely.geometry import shape, Point
# 	# depending on your version, use: from shapely.geometry import shape, Point
#
# 	# load GeoJSON file containing sectors
# 	with open(geojson) as f:
# 	    js = json.load(f)
#
# 	# construct point based on lon/lat returned by geocoder
# 	x, y = latlng[0], latlng[1]
# 	point = Point(y, x)
# 	print(point)
#
# 	# check each polygon to see if it contains the point
# 	for feature in js['features']:
# 		polygon = shape(feature['geometry'])
# 		if polygon.contains(point):
# 			print("Found: ", feature['properties']['NAMELSAD'])
# 			return feature['properties']['NAMELSAD']
# 	return None

def latlngs_to_census_tracts(latlngs, geojson):
	"""Convert a lat/long list to a list of census tracts."""
	import json
	from shapely.geometry import shape, Point
	# depending on your version, use: from shapely.geometry import shape, Point

	# load GeoJSON file containing sectors
	with open(geojson) as f:
	    js = json.load(f)

	# Construct a map of unique identifiers to polygons
	# In this case, unique identifires are a combination of STATE|COUNTY|TRACT
	polygons = {}
	for feature in js['features']:
		state_county_tract = "{}|{}|{}".format(feature['properties']['STATEFP'], feature['properties']['COUNTYFP'], feature['properties']['NAME'])
		polygons[state_county_tract] = shape(feature['geometry'])

	o = []

	for latlng in latlngs:
		# construct point based on lon/lat returned by geocoder
		t = True
		x, y = latlng[0], latlng[1]
		point = Point(y, x)
		for ct in polygons.keys():
			if polygons[ct].contains(point):
				t = False
				o.append(ct.split('|')[2])
		if t:
			o.append(None)
	return o

def address_to_latlng(address):
	"""Convert an address string to a list of latitude, longitude coordinates.

	Currently uses Google's geocoder API for geocoding, this could be replaced
	with other geocoder submodules
	"""
	print("Converting {} to latlng".format(address))
	g = geocoder.google(str(address))
	l = g.latlng

	print(l)

	return l

if __name__=='__main__':
	print("Beginning.")
	print('\n')
	fn = 'data/test/testset.tsv'
	adds = read_addresses(fn)
	print('\n')
	llngs = [ address_to_latlng(address) for address in adds ]
	for l in llngs:
		latlng_to_census_tract(l, 'data/geojsons/Washington_2016.geojson')
