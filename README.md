# WeatherAnalyzer
For this project, we have two datasets. The first provides station information for weather stations across the world. The second provides individual recordings for the stations over a 4-year period. The goal of the project is to find out which states in the US have the most stable temperature (i.e. their hottest month and coldest month have the least difference)
Three Phase chained Map Reduce Jobs was used for this project. In the first phase it joined the two datasets, in the second phase it aggregated the data group by state. In the third state in sorts the states according to the difference between their hottest month and coldest month.


Dataset Information:
The locations dataset contains the metadata for every station across the world. Here are the fields for this dataset:
USAF = Air Force station ID. May contain a letter in the first position.
WBAN = NCDC WBAN number
CTRY = FIPS country ID
ST = State for US stations
LAT = Latitude in thousandths of decimal degrees
LON = Longitude in thousandths of decimal degrees
ELEV = Elevation in meters
BEGIN = Beginning Period Of Record (YYYYMMDD). 
END = Ending Period Of Record (YYYYMMDD). 
Sample Row:
"724920","23237","STOCKTON METROPOLITAN AIRPORT","US","CA","+37.889","-121.226","+0007.9","20050101","20140403”

The recordings dataset is contained in four files, one for each year. The “STN---“ value will match with the “USAF” field in the locations dataset. Here are the fields for this dataset:
STN---  = The station ID (USAF)
WBAN   = NCDC WBAN number
YEARMODA   = The datestamp
TEMP = The average temperature for the day, followed by the number of recordings
DEWP = Ignore for this project
SLP = Ignore for this project
STP = Ignore for this project
VISIB = Ignore for this project (Visibility)
WDSP = Ignore for this project
MXSPD = Ignore for this project
GUST = Ignore for this project    
MAX = Ignore for this project (Max Temperature for the day)
MIN = Ignore for this project (Min Temperature for the day)
PRCP = Ignore for this project (Precipitation)
NDP = Ignore for this project   
FRSHTT = Ignore for this project
Sample Row: 
997781 99999  20061121    42.4 13  9999.9  0  9999.9  0  9999.9  0  999.9  0   17.5 13   22.0  999.9    46.2*   39.0*  0.00I 999.9  000000


