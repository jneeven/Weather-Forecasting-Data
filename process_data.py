import os
import csv
import numpy as np
import pickle
from datetime import datetime
import matplotlib.pyplot as plt
import constants


find_text = [
			"dulles town center", "reston", "adams field", "albuquerque international airport",
			"amarillo international airport", "bangor international airport", "bemidji-beltrami county airport",
			"bismarck municipal airport", "boise air terminal", "buckley air force base", "chicago midway airport",
			"cleveland-hopkins international airport", "columbia metropolitan airport",
			"columbia owens downtown airport",
			"davis-monthan air force base", "el paso international airport", "elko municipal-harris field",
			"ellsworth air force base", "felts field", "flagstaff pulliam airport", "foss field",
			"fresno air terminal", "fulton county airport-brown field", "grand junction regional airport",
			"great falls international airport", "greater rochester international airport", "hays municipal",
			"jefferson county airport", "kansas city downtown airport", "lakefront airport",
			"lambert-st louis international airport", "las vegas  mccarran international airport",
			"mcghee tyson airport", "miami international airport", "midland - midland airpark",
			"nashville international airport", "neapolis", "nemucca", "new york", "new york city - central park",
			"pensacola regional airport", "portland international airport", "raleigh east airport",
			"raleigh-durham international airport", "reagan national airport",
			"rock springs weather reporting station",
			"sacramento executive airport", "salt lake city international airport",
			"san antonio international airport",
			"sidney-richland", "spokane international airport", "st paul international airport", "standiford field",
			"tucson international airport", "tuscaloosa municipal airport", "valdosta regional",
			"valdosta regional airport",
			"vicksburg municipal", "walker field", "washington dulles international airport",
			"yuma marine corps air station", "gs", "amsterdam airport schiphol",
			"flushing", "amsterdam", "cadzand wp", "rotterdam zestienhoven airport",
			"eindhoven airport", "amsterdam schiphol airport", "maastricht airport zuid limburg",
			"leeuwarden airport", "amsterdam airport schiphol 10-day weather forecast - the weather channel | weather.com",
			'hawthorne municipal airport', 'schiphol amsterdam airport', 'maastricht aachen airport',
			'isle of portland', 'st louis', 'north rock springs', 'mather field', 'new city',
			'new orleans international airport', 'saint louis', 'utrecht', 'vicksburg',
			'tucson  tucson international airport', 'broomfield', 'hutchinson county airport',
			'minneapolis-st paul international airport', 'portland  portland international airport',
			'rapid city regional airport', 'shelby county airport', 'williston  sloulin field international airport'
			]

replace_text = ["dulles", "dulles", "little rock", "albuquerque",
				"amarillo", "bangor", "bemidji",
				"bismarck", "boise", "denver", "chicago",
				"cleveland", "columbia", "columbia",
				"tucson", "el paso", "winnemucca",
				"rapid city", "spokane", "flagstaff", "sioux falls",
				"fresno", "atlanta", "grand junction",
				"choteau", "rochester", "hays",
				"beaumont", "kansas city", "houma",
				"st. louis", "las vegas",
				"knoxville", "miami", "sanderson",
				"nashville", "minneapolis", "winnemucca", "new york city", "new york city",
				"pensacola", "portland", "raleigh",
				"raleigh", "dulles", "rock springs",
				"sacramento", "salt lake city", "san antonio",
				"sidney", "spokane", "minneapolis", "louisville",
				"tucson", "tuscaloosa", "valdosta", "valdosta",
				"hollandale", "grand junction", "dulles", "yuma", "rock springs", "schiphol",
				"vlissingen", "schiphol", "vlissingen", "rotterdam",
				"eindhoven", "schiphol", "maastricht",
				"leeuwarden", "schiphol",
				"los angeles", 'schiphol', 'maastricht',
				'portland', 'st. louis', 'rock springs', 'sacramento', 'new york city',
				'houma', 'st. louis', 'de bilt', 'hollandale',
				'tucson', 'denver', 'amarillo',
				'minneapolis', 'portland',
				'rapid city', 'tuscaloosa', 'sidney'
				]

headers = {'site', 'location', 'description', 'max_temp', 'min_temp', 'avg_temp', 'wind_speed',
		   'wind_direction', 'humidity', 'pop', 'precipitation', 'type', 'date'}


def replace_location(loc):
	lowercase = loc.lower()
	if lowercase in find_text:
		return replace_text[find_text.index(lowercase)]
	return lowercase


def parse_actual_data_files():
	data = []

	# filenames = os.listdir('actual data gathering/')
	# filenames = ['2017-09-30_2017-12-09_actual_data.csv']  # All data except last month
	# filenames = ['2017-12-10_2018-01-10_actual_data.csv']  # Last month
	filenames = ['2017-10-1_2018-01-16_dutch_actual_data.csv']  # Dutch
	for filename in filenames:
		if ".csv" in filename:
			if filename == "2017-11-07_2017-12-01_actual_data.csv":
				print("Skipping data file")
				continue
			with open('actual data gathering/' + filename, encoding='UTF-8') as file:
				reader = csv.reader(file, delimiter=',')
				next(reader)  # Skip header row
				for row in reader:
					row[1] = replace_location(row[1])
					if 'dutch' not in filename:
						row[-1] = datetime.strptime(row[-1], '%Y-%m-%d').strftime('%d-%m-%Y')
					data.append(np.array(row))

	return np.array(data)


def parse_forecast_files():
	daily = []
	weekly = []

	filenames = os.listdir('scrapy/stack/output')

	for filename in filenames:
		if '.csv' not in filename:
			continue
		with open('scrapy/stack/output/' + filename, encoding='UTF8') as file:
			reader = csv.reader(file, delimiter=',')
			next(reader)  # Skip header row
			for row in reader:
				row[1] = replace_location(row[1])

				# if the description contains commas, this messes up the structure.
				if len(row) > 13:
					row[2] = row[2] + ',' + row[3]
					for i in range(3, constants.CSV_COLUMNS):
						row[i] = row[i + 1]

					# remove last element as all elements have shifted one to the left
					row = row[:-1]

				# If min temp is higher than max (happens in some specific cases), switch them around
				if float(row[4]) > float(row[3]):
					temp = row[4]
					row[4] = row[3]
					row[3] = temp

				if row[11] == 'day':
					daily.append(np.array(row))
				elif row[11] == 'week':
					weekly.append(np.array(row))

	return np.array(daily), np.array(weekly)


def convert_to_floats(vector, is_target=False):
	output_vec = np.zeros(len(vector) * constants.AMT_FEATURES)

	# In the target data, feature columns are different, so we use an offset. Furthermore, target data needs to be
	# converted to the correct units (i.e. m/s to km/h).
	offset = 0
	zero_entry = 0
	speed_conversion = temp_conversion = 1
	if is_target:
		offset = -1
		speed_conversion = 0.36
		temp_conversion = 0.1
		# If target data is missing, set entry to NaN. For websites, we set the value to 0.
		zero_entry = np.nan

	index = 0
	for site_entry in vector:
		# Max, Min and Avg temp for now
		output_vec[index] = float(site_entry[constants.MAX_TEMP_INDEX + offset]) * temp_conversion  # max
		output_vec[index + 1] = float(site_entry[constants.MIN_TEMP_INDEX + offset]) * temp_conversion  # min
		# If there is no average, calculate it ourselves
		if site_entry[constants.AVG_TEMP_INDEX + offset] != '':
			output_vec[index + 2] = float(site_entry[constants.AVG_TEMP_INDEX + offset]) * temp_conversion  # avg
		else:
			output_vec[index + 2] = (output_vec[index] + output_vec[index + 1]) / 2.0
		
		if site_entry[constants.WINDSPEED_INDEX + offset] != '':
			output_vec[index + 3] = float(site_entry[constants.WINDSPEED_INDEX + offset]) * speed_conversion
		else:
			output_vec[index + 3] = zero_entry

		if is_target:
			if site_entry[constants.PRECIPITATION_ACTUAL_DATA_INDEX] != '':
				output_vec[index + 4] = float(site_entry[constants.PRECIPITATION_ACTUAL_DATA_INDEX])/10
			else:
				output_vec[index + 4] = zero_entry
			# Count both snow and rain as precipitation
			if site_entry[constants.SNOW_INDEX] != '':
				output_vec[index + 4] += float(site_entry[constants.SNOW_INDEX])

		else:
			if site_entry[constants.PRECIPITATION_INDEX] != '':
				output_vec[index + 4] = float(site_entry[constants.PRECIPITATION_INDEX])
			else:
				output_vec[index + 4] = zero_entry

		index += constants.AMT_FEATURES

	return output_vec


def create_training_data(forecast_data, actual_data, locations, websites, dates, location_names=False):
	train_data = []
	target_data = []
	location_data = []
	date_data = []

	for date in dates:
		for location in locations:
			train_vector = []
			target_vector = []
			# timeanddate.com sometimes has two entries. Keep boolean so we can check whether we've already
			# taken the values from this site
			time_date = False
			for entry in forecast_data:
				if entry[-1] == date and entry[1] == location and entry[0] in websites:
					if entry[0] == 'timeanddate.com':
						if not time_date:
							train_vector.append(entry)
							time_date = True
					else:
						train_vector.append(entry)

			for entry in actual_data:
				if entry[-1] == date and entry[1] == location:
					target_vector.append(entry)

			# If not all of the selected sites contained this location at this date, or if there is no target
			# data for this location at this date, ignore it
			if len(train_vector) == constants.AMT_WEBSITES and len(target_vector) > 0:
				location_data.append(location)
				train_data.append(convert_to_floats(train_vector))
				target_data.append(convert_to_floats(target_vector, is_target=True))

	if location_names:
		return np.array(location_data), np.array(train_data), np.array(target_data)
	else:
		return np.array(train_data), np.array(target_data)


def websites_per_location(data, locations, dates):
	# Keep track of which websites a location appears on
	locations_websites = {}
	for location in locations:
		locations_websites[location] = []

	for date in dates:
		for location in locations:
			for entry in data:
				if entry[-1] == date and entry[1] == location:
					locations_websites[location].append(entry[0])

	return locations_websites


def sort_websites_by_frequency(websites, locations_websites):
	website_counter = {}
	for website in websites:
		website_counter[website] = 0

	for location in locations_websites:
		for website in locations_websites[location]:
			website_counter[website] += 1

	# Sort websites by frequency
	sorted_websites = [(k, website_counter[k]) for k in sorted(website_counter, key=website_counter.get, reverse=True)]
	return sorted_websites


def filter_locations(locations_websites, selected_websites):
	legit_locations = []
	for location in locations_websites:
		legit = True
		for website in selected_websites:
			if website not in locations_websites[location]:
				legit = False
				break
		if legit:
			legit_locations.append(location)

	return np.array(legit_locations)


def websites_locations_plot(locations_websites, sorted_websites):
	counters = [0, 0, 0, 0, 0, 0, 0, 0]
	for i in range(1, 9):
		locs = filter_locations(locations_websites, sorted_websites, i)
		counters[i - 1] = locs.size

	plt.title("Amount of locations present on at least x websites")
	plt.xlabel("Amount of websites")
	plt.ylabel("Amount of locations")
	plt.plot(range(1, 9), counters)
	plt.show()


# Create dataset from forecasts and actual data. Prediction type is either day or week, corresponding with
# the one-day and seven-day predictions.
# all_locations determine whether to use all available locations, or only those present on all websites (24).
# only_rain determines whether to use top x websites, or only those that predict precipitation.
def create_data_from_files(prediction_type, all_locations=False, only_rain=False, location_names=False, dutch_cities=False):
	daily, weekly = parse_forecast_files()
	if prediction_type == 'day':
		dataset = daily
	elif prediction_type == 'week':
		dataset = weekly
	else:
		raise ValueError('{} is not a valid prediction type! Try "week" or "day".'.format(prediction_type))

	actual_data = parse_actual_data_files()

	dates = np.intersect1d(actual_data[:, -1], dataset[:, -1])
	print("amount of days:", dates.size)
	print(dates)

	skipped_locations = np.array([])
	if not dutch_cities:
		print("Using only US cities")
		# Remove Dutch cities
		locations = np.array([])
		for location in np.unique(dataset[:, 1]):
			if location not in constants.DUTCH_LOCATIONS:
				if location in constants.US_LOCATIONS:
					locations = np.append(locations, location)
				else:
					skipped_locations = np.append(skipped_locations, location)
	else:
		print("Using only Dutch cities")
		locations = np.array(constants.DUTCH_LOCATIONS)

	print("Skipped locations:", skipped_locations)
	print("Number of used locations:", locations.size)

	websites = np.unique(dataset[:, 0])
	print("amount of websites:", websites.size)

	# Calculate amount of websites per location
	locations_websites = websites_per_location(dataset, locations, dates)
	# pickle.dump(locations_websites, open('locations_websites', 'wb'))
	# locations_websites = pickle.load(open('locations_websites', 'rb'))

	sorted_websites = sort_websites_by_frequency(websites, locations_websites)

	# As we want as many websites as possible, but also as many locations as possible, 5, 6 or 7 seems optimal.
	# Check the graph produced by websites_locations_plot for details.
	if only_rain:
		filtered_websites = ['accuweather.com', 'timeanddate.com', 'weather-forecast.com']
	else:
		filtered_websites = [website for (website, frequency) in sorted_websites[:constants.AMT_WEBSITES]]

	if all_locations:
		filtered_locations = filter_locations(locations_websites, filtered_websites)
	else:
		filtered_locations = filter_locations(locations_websites, websites)

	print("Using {} websites: {}, and {} locations.".format(len(filtered_websites), filtered_websites,
															len(filtered_locations)))
	print(filtered_locations)

	if location_names:
		location_data, training_data, target_data = create_training_data(dataset, actual_data, filtered_locations, filtered_websites,
														dates, location_names)
		return location_data, training_data, target_data
	else:
		training_data, target_data = create_training_data(dataset, actual_data, filtered_locations, filtered_websites,
															dates)
		return training_data, target_data


if __name__ == '__main__':
	locations, train, target = create_data_from_files('week', location_names=True, dutch_cities=True)
	pickle.dump([locations, train, target], open('pickle files/dutch_week_total', 'wb'))
	print(train.shape)
	print(target.shape)
	print('done')