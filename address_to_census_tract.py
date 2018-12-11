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
	ltlngs = []
	for address in addresses:
		ltlngs.append(address_to_latlng(address))

	for ltlng in ltlngs:
		print(ltlng)

def address_to_latlng(address):
	"""Convert an address string to a list of latitude, longitude coordinates.

	Currently uses Google's geocoder API for geocoding, this could be replaced
	with other geocoder submodules
	"""
	print("Converting {} to latlng".format(address))
	g = geocoder.google(str(address))
	l = g.latlng

	return l

if __name__=='__main__':
	print("Beginning.")
	print('\n')
	fn = 'data/test/testset.tsv'
	adds = read_addresses(fn)
	print('\n')
	print_latlngs(adds)
