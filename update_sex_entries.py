import MySQLdb as sql
import numpy as np
import collections as cl
import csv
######################################################################################
#####This script corrects the easting and northing location of burrow. It also deals###########
###### with missing location when there is enough data to fill in the coordinates#############################

######################################################################################

# Open database connection
db = sql.connect("localhost","root","bio123456","burrow_data" )
# prepare a cursor object using cursor() method

###########################################################################################
def extract_sex_info(filename, sexfile):
	sex_dict={}
	with open (sexfile, 'r') as csvfile:
		fileread = csv.reader(csvfile, delimiter = ',')
		next(fileread, None) #skip header
		for row in fileread:
			if not sex_dict.has_key(row[0].upper()): sex_dict[row[0].upper()] = row[1].upper()
		
	return sex_dict

###########################################################################################

def update_sex_info(filename, sex_dict):

	""" update sex information"""
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	print filename, sexfile
	for tort in sex_dict.keys():
		print tort, sex_dict[tort]
		cursor.execute( """ UPDATE """ + filename + """ SET Sex = %s WHERE Tortoise_number=%s""",((sex_dict[tort], tort)))
		db.commit()

###########################################################################################	

if __name__ == "__main__":
# This is the main function, where all the tasks get created

	files = ["SG_aggregate"]
	sexfiles = ["SGsex.csv"]
	
	for filename,sexfile in zip(files, sexfiles):
		print ("file="), filename
		#connect to the database
		db = sql.connect("localhost","root","bio123456","burrow_data")
		sex_dict = extract_sex_info(filename, sexfile)
		update_sex_info(filename, sex_dict)
