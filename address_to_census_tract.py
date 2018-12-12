"""address_to_census_tract.py

Takes a file containing a list of addresses, one per line, and returns their
associated census tract within Seattle.

To run:
	python address_to_census_tract.py -i INPUT_FILE -o OUTPUT_FILE

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

def print_latlngs(addresses):
	"""Print to screen all the lat/long values for a list of addresses."""
	latlngs = []
	for address in addresses:
		latlngs.append(address_to_latlng(address))

	for latlng in ltlngs:
		print(latlng)

def latlng_to_census_tract(latlng, geojson):
	"""Convert a lat/long value to a census tract."""
	import json
	from shapely.geometry import shape, Point
	# depending on your version, use: from shapely.geometry import shape, Point

	# load GeoJSON file containing sectors
	with open(geojson) as f:
	    js = json.load(f)

	# Trace for debugging
	# import pdb; pdb.set_trace()

	# construct point based on lon/lat returned by geocoder
	x, y = latlng[0], latlng[1]
	point = Point(x, y)
	print(point)

	# check each polygon to see if it contains the point
	for feature in js['features']:
		polygon = shape(feature['geometry'])
		if polygon.contains(point):
			print('Yay!')
			# print('Found containing polygon:', feature)

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
