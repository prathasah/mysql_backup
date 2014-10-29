import MySQLdb as sql
import numpy as np
import csv
import os
import datetime
from dateutil.relativedelta import relativedelta
######################################################################################

# Open database connection
db = sql.connect("localhost","root","bio123456","burrow_data" )
# prepare a cursor object using cursor() method

######################################################################################
def add_climate_column (filename):

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute("""ALTER TABLE """ + filename + """ change climate_temperature climate_temperature float(5,2) ;""")
	cursor.execute("""ALTER TABLE """ + filename + """ change climate_rainfall climate_rainfall float(5,2) ;""")

######################################################################################
def update_climate_info(filename, climatefile): 
	
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	os.chdir("/home/pratha/Dropbox/SB Lab_Pratha/0 Burrow Use/climate_data")

	temp_dict={}
	rain_dict={}
	with open (climatefile, 'r') as csvfile:
			fileread = csv.reader(csvfile, delimiter = ',')
			next(fileread, None) #skip header
			for row in fileread:
				year = int(row[2])
				month= int(row[3])
				temperature= [None if row[4] =='nan' else  float(row[4]) ][0]
				print temperature,
				rainfall = [None if row[5] =='nan' else  float(row[5]) ][0]
				if not temp_dict.has_key(year): temp_dict[year] = {}
				if not rain_dict.has_key(year): rain_dict[year] = {}
				temp_dict[year][month] = temperature
				rain_dict[year][month] = rainfall

	
	# adding entries#################
	for year in temp_dict.keys():
		for month in temp_dict[year].keys():
			print year, month, temp_dict[year][month], rain_dict[year][month]
			cursor = db.cursor()	
			cursor.execute( """ UPDATE """ + filename + """ SET climate_temperature = %s where year(date) = %s and month(date) =%s; """,(temp_dict[year][month], year, month))
			cursor.execute( """ UPDATE """ + filename + """ SET climate_rainfall = %s where year(date) = %s and month(date) =%s; """,(rain_dict[year][month], year, month))

	
	db.commit()
	db.close()

########################################################################################################3
if __name__ == "__main__":
	
	files = ["BSV_aggregate", "CS_aggregate", "HW_aggregate","LM_aggregate", "MC_aggregate", "PV_aggregate", "SG_aggregate", "SL_aggregate", "FI_aggregate"]
	climatefiles = ["BSV_climate.csv", "CS_climate.csv","HW_climate.csv","LM_climate.csv", "MC_climate.csv", "PV_climate.csv", "SG_climate.csv", "SL_climate.csv", "FI_climate.csv"]
	
	for filename,climatefile in zip(files, climatefiles):
		print filename

		update_climate_info(filename,climatefile) 
