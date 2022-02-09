from pytrends.request import TrendReq
import pandas as pd
import datetime as dt

def check_if_valid_data(df: pd.DataFrame) -> bool:
	# Check if dataframe is empty
	if df.empty:
		print('Google trends has returned no results.')
		return False
	# Validate
	else:
		return True

class GoogleTrends:
	'''
	#########################################################################################
					CONNECT TO GOOGLE - TrendRed()

	# timeout(connect, read)

	# tz (Timezone Offset)
		- For example US CST is '360' (note NOT -360, Google uses timezone this way...)

	# proxies
		- https proxies Google passed ONLY
		- list ['https://34.203.233.13:80','https://35.201.123.31:880', ..., ...]

	# retries
		- number of retries total/connect/read all represented by one scalar

	# backoff_factor
		- A backoff factor to apply between attempts after the second try (most errors are resolved immediately
		by a second try without a delay). urllib3 will sleep for:
		{backoff factor} * (2 ^ ({number of total retries} - 1)) seconds.
		If the backoff_factor is 0.1, then sleep() will sleep for [0.0s, 0.2s, 0.4s, …] between retries.
		It will never be longer than Retry.BACKOFF_MAX. By default, backoff is disabled (set to 0).

	# requests_args
		- A dict with additional parameters to pass along to the underlying requests library,
		for example verify=False to ignore SSL errors

	# hl (host language)
		- Example: 'es-MX' | 'en-MX'
		- Note: the parameter hl specifies host language for accessing Google Trends.
		- Note: only https proxies will work, and you need to add the port number after the proxy ip address


	#########################################################################################
					API PARAMETERS

	# kw_list
		- keywords to get data for
		- Example ['Pizza']
		- Up to five terms in a list: ['Pizza', 'Italian', 'Spaghetti', 'Breadsticks', 'Sausage']
		- Advanced Keywords
			- When using Google Trends dashboard Google may provide suggested narrowed search terms.
			- For example "iron" will have a drop down of "Iron Chemical Element, Iron Cross, Iron Man, etc".
			- Find the encoded topic by using the get_suggestions() function and choose the most relevant one for you.
			- For example: https://www.google.com/trends/explore#q=%2Fm%2F025rw19&cmpt=q
			- "%2Fm%2F025rw19" is the topic "Iron Chemical Element" to use this with pytrends
			- You can also use pytrends.suggestions() to automate this.

	# cat
		- Category to narrow results
		- Find available cateogies by inspecting the url when manually using Google Trends. The category starts after
		  cat= and ends before the next & or view this wiki page containing all available categories
		- For example: "https://www.google.com/trends/explore#q=pizza&cat=71"
		- '71' is the category
		- Defaults to no category

	# geo
		- Two letter country abbreviation
		- For example United States is 'US'
		- Defaults to World
		- More detail available for States/Provinces by specifying additonal abbreviations
		- For example: Alabama would be 'US-AL'
		- For example: England would be 'GB-ENG'

	# tz
		- Timezone Offset (in minutes)
		- For more information of Timezone Offset, view this wiki page containing about UCT offset
		- For example US CST is '360'

	# timeframe
		- Date to start from
		- Defaults to last 5yrs, 'today 5-y'.
		- Everything 'all'
		- Specific dates, 'YYYY-MM-DD YYYY-MM-DD' example '2016-12-14 2017-01-25'
		- Specific datetimes, 'YYYY-MM-DDTHH YYYY-MM-DDTHH' example '2017-02-06T10 2017-02-12T07'
			- Note Time component is based off UTC
		- Current Time Minus Time Pattern:
			- By Month: 'today #-m' where # is the number of months from that date to pull data for
				- For example: 'today 3-m' would get data from today to 3months ago
				- NOTE Google uses UTC date as 'today'
				- Seems to only work for 1, 2, 3 months only
			- Daily: 'now #-d' where # is the number of days from that date to pull data for
				- For example: 'now 7-d' would get data from the last week
				- Seems to only work for 1, 7 days only
			- Hourly: 'now #-H' where # is the number of hours from that date to pull data for
				- For example: 'now 1-H' would get data from the last hour
				- Seems to only work for 1, 4 hours only

	# gprop
		- What Google property to filter to
		- Example 'images'
		- Defaults to web searches
		- Can be images, news, youtube or froogle (for Google Shopping results)
	'''

	def __init__(self, keyword_list: list(), timeframe, geo, host_language, category=0, gprop=''):

		self.keyword_list = keyword_list
		self.timeframe = timeframe
		self.geo = geo
		self.host_language = host_language
		self.gprop = gprop
		self.category = category
		self.pytrends = TrendReq(host_language, tz=360, retries=2, timeout=(10,25)) # Connect to Google

		self.pytrends.build_payload(
			kw_list = keyword_list,
			cat = self.category,
			timeframe = self.timeframe,
			geo = self.geo,
			gprop = self.gprop
			)

	def interestOverTime(self):
		'''
		Description:
		------------
		The resulting numbers are scaled on a range of 0 to 100 based on a topics proportion to
		all searches. Returns historical, indexed data for when the keyword was searched most
		as shown on Google Trends' Interest Over Time section.

		Parameters:
		-----------
		__init__

		Returns:
		________
		A dataframe with results by datetime.
		'''
		interest_over_time = self.pytrends.interest_over_time()

		# data validation
		val = check_if_valid_data(interest_over_time)
		if val == True:
			interest_df = interest_over_time.copy()
			interest_df['datetime'] = interest_df.index.to_frame(index=True)
			interest_df = interest_df.reset_index(drop=True)
			return interest_df
		else:
			pass

	def historicalHourlyInterest(self, start_date, end_date, hour_start=0, hour_end=1):
		'''
		Description:
		------------
		The resulting numbers are scaled on a range of 0 to 100 based on a topics proportion
		to all searches. Returns historical, indexed, hourly data for when the keyword was
		searched most as shown on Google Trends' Interest Over Time section. It sends multiple
		requests to Google, each retrieving one week of hourly data. It seems like this would
		be the only way to get historical, hourly data.

		Parameters:
		-----------
		start_date = 'YYYY-MM-DD'
		end_date = 'YYY-MM-DD'
		hour_start
		hour_end

		Returns:
		________
		A dataframe with results by hour.
		'''
		year_start, month_start, day_start = start_date.replace('-', ' ').strip().split()
		year_end, month_end, day_end = end_date.replace('-', ' ').strip().split()

		self.pytrends.get_historical_interest(
			self.keyword_list,
			year_start = int(year_start),
			month_start = int(month_start),
			day_start = int(day_start),
			hour_start = int(hour_start),
			year_end = int(year_end),
			month_end = int(month_end),
			day_end = int(day_end),
			hour_end = int(hour_end),
			cat = self.category,
			geo = self.geo,
			gprop = self.gprop,
			sleep = 0
			)

		interest_over_time = self.pytrends.interest_over_time()

		# data validation
		val = check_if_valid_data(interest_over_time)
		if val == True:
			# Transform datetime index to column
			historical_df = interest_over_time.copy()
			historical_df['datetime'] = historical_df.index.to_frame(index=True)
			historical_df = historical_df.reset_index(drop=True)
			return historical_df
		else:
			pass

	def interestByRegion(self, region, inc_low_vol=False, inc_geo_code=True):
		'''
		Description:
		------------
		Returns data for where the keyword is most searched as shown on Google Trends' Interest by Region section.
		The values are calculated on a scale from 0 to 100, where 100 is the location with the most
		popularity as a fraction of total searches in that location, a value of 50 indicates a
		location which is half as popular. A value of 0 indicates a location where there was not enough
		data for this term.

		Parameters:
		-----------
	        resolution:
		        'CITY' returns city level data
			'COUNTRY' returns country level data
			'DMA' returns Metro level data
			'REGION' returns Region level data
        	inc_low_vol: True/False (includes google trends data for low volume countries/regions as well)
        	inc_geo_code: True/False (includes ISO codes of countries along with the names in the data)

	        Returns:
        	--------
        	A dataframe with results by region.
		'''
		interest_by_region = self.pytrends.interest_by_region(
			resolution = region,
			inc_low_vol = inc_low_vol,
			inc_geo_code = inc_geo_code
			)

		# data validation
		val = check_if_valid_data(interest_by_region)
		if val == True:
			intByRegion_df = interest_by_region.copy()
			intByRegion_df['geoName'] = intByRegion_df.index.to_frame(index=True)
			intByRegion_df = intByRegion_df.reset_index(drop=True)
			return intByRegion_df
		else:
			pass

	def relatedTopics(self):
		'''
		Description:
		------------
		Returns data for the related keywords to a provided keyword shown on Google Trends' Related
		Topics section. The resulting numbers are scaled on a range of 0 to 100 based on a topics
		proportion to all searches.

		Parameters:
		-----------
        	__init__

        	Returns:
        	--------
        	A dataframe of related_topics information. Specifically, the dataframe contains a
        	its columns related to the subkeys (‘rising’ and ‘top’).
		'''
		related_topics = self.pytrends.related_topics()

		# getting Rising & Top topics from results
		rising_df = related_topics[self.keyword_list[0]]['rising']
		top_df = related_topics[self.keyword_list[0]]['top']
		try:
			# Renaming columns
			rising_df = rising_df.rename(columns={'value': 'value_rising',
							      'formattedValue': 'formattedValue_rising',
							      'hasData': 'hasData_rising',
							      'topic_title': 'topic_title_rising',
							      'topic_type': 'topic_type_rising'})
			top_df = top_df.rename(columns={'value': 'value_top',
							'formattedValue': 'formattedValue_top',
							'hasData': 'hasData_top',
							'topic_title': 'topic_title_top',
							'topic_type': 'topic_type_top'})
			# Concatenate top and rising dataframes
			df = pd.concat([top_df, rising_df], axis=1)
			df = df.drop(['link', 'topic_mid'], axis=1)
			return df
		except:
			print('No results for Related topics')

	def relatedQueries(self, category=0):
		'''
		Description:
		------------
		Returns data for the related keywords to a provided keyword shown on Google Trends' Related
		Queries section. The resulting numbers are scaled on a range of 0 to 100 based on a topics
		proportion to all searches.

		Parameters:
		-----------
		__init__
		category

		Returns:
		--------
		A dataframe of related_queries information. Specifically, the dataframe contains a
        	its columns related to the subkeys (‘rising’ and ‘top’).
		'''
		related_queries = self.pytrends.related_queries()

		rising_df = related_queries[self.keyword_list[0]]['rising']
		top_df = related_queries[self.keyword_list[0]]['top']
		try:
			# Renaming columns
			rising_df = rising_df.rename(columns={'query': 'rising_query', 'value': 'rising_value'})
			top_df = top_df.rename(columns={'query': 'top_query', 'value': 'top_value'})
			# Concatenate top and rising dataframes
			df = pd.concat([top_df, rising_df], axis=1)
			return df
		except:
			print('No results for Related queries')

	def trendingSearches(self, country):
		'''
		Description:
		------------
		Returns data for latest trending searches shown on Google Trends' Trending Searches section.

		Parameters:
		-----------
		__init__
		country: lowercase string

		Returns:
		--------
		A dataframe.
		'''
		trending_searches_df = self.pytrends.trending_searches(pn = country)

		# data validation
		val = check_if_valid_data(trending_searches_df)
		if val == True:
			return trending_searches_df
		else:
			pass

	def topCharts(self, year):
		'''
		Description:
		------------
		Returns a dataframe with the 10 topics most shown in Google Trends' Top Charts section.
		NOTE: Current year/date might not work

		Parameters:
		-----------
		year: YYYY or YYYYMM as integer

		Returns:
		--------
		A dataframe
		'''
		top_charts_df = self.pytrends.top_charts(year, geo = self.geo)

		# data validation
		val = check_if_valid_data(top_charts_df)
		if val == True:
			return top_charts_df
		else:
			pass

	def suggestions(self):
		'''
		Description:
		------------
		Returns a dataframe with suggested keywords that can be used to refine a trend search.

		Parameters:
		-----------
		__init__
		keyword

		Returns:
		--------
		A dataframe.
		'''
		suggestions = self.pytrends.suggestions(self.keyword_list[0])
		suggestions_df = pd.DataFrame.from_dict(suggestions).drop('mid', axis=1)

		# data validation
		val = check_if_valid_data(suggestions_df)
		if val == True:
			return suggestions_df
		else:
			pass

	def categories(self):

		res = self.pytrends.categories()
		return res
