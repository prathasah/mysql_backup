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
def add_tort_status_column (filename):

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute("""ALTER TABLE """ + filename +""" ADD Tortoise_status VARCHAR(100) AFTER Tortoise_number;""")

######################################################################################
def update_BSV_tort_status():
	"""for BSV translocation happened on 1997-04-09. So all the "T" torts will be marked "E" on/after 1998-04-09"""
	
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	extrans = '1998-04-09'
	#cursor.execute( """ UPDATE BSV_aggregate SET Tortoise_status = 'E' where Tortoise_status ="T" and date > %s;""", (extrans))
	cursor.execute( """ UPDATE BSV_aggregate SET Tortoise_status = 'ER' where Tortoise_status ="R" and date > %s;""", (extrans))
	db.commit()
	db.close()
	

######################################################################################
def update_LM_tort_status():

	"""The translocation happened in 1/1998 but I have data from 09/1999 -- so the T animals are actually post-translocated"""
	
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	#cursor.execute( """ UPDATE LM_aggregate SET Tortoise_status = 'E' where Tortoise_status = "T";""")
	### update 10/29/2014: converting "R" torts to "ER"
	cursor.execute( """ UPDATE LM_aggregate SET Tortoise_status = 'ER' where Tortoise_status = "R";""")
	db.commit()
	db.close()
	
######################################################################################
def update_SG_tort_status():
	
	"""Translocation for shiv and pah site happenedon 1998-04. So all these torts are marked E after 1999-05. For SSM translocate= 1999-04, E= 2000-05"""
	tort_site={}
	with open ("SG_tortoise_sites.csv", 'r') as csvfile:
		fileread = csv.reader(csvfile, delimiter = ',')
		next(fileread, None) #skip header
		for row in fileread:
			tort=row[0]
			site=row[1]
			tort_site[tort]=site
	
	site1 = ["shiv", "pah"]
	sitegroup1 = [key for key, val in tort_site.items() if val in site1]
	date1 = '1999-05-01'
	sitegroup2=  [key for key, val in tort_site.items() if val =="ssm"]
	date2 = '2000-05-01'
	
	print sitegroup1
	print ("2"), sitegroup2
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	for tort in sitegroup1:
		
		cursor = db.cursor()
		cursor.execute( """ UPDATE SG_aggregate SET Tortoise_status = 'E' where Tortoise_status ="T" and Tortoise_number = %s and date> %s;""", (tort, date1))
	
	for tort in sitegroup2:
		
		cursor = db.cursor()
		cursor.execute( """ UPDATE SG_aggregate SET Tortoise_status = 'E' where Tortoise_status ="T" and Tortoise_number = %s and date> %s;""", (tort, date2))
	
	
	#db.commit()
	db.close()

######################################################################################
def update_FI_tort_status():
	
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	mindate={}
	## note I am adding 1 year to the translocation dates so the dicut is actually storing the mindate for "T" to be relabeled as "E"
	cursor = db.cursor()
	cursor.execute( """ select tortoise_number, date_add(min(date), interval 1 year) from FI_aggregate where tortoise_status="T" group by tortoise_number; """)
	results = cursor.fetchall()
	for row in results:
		mindate[row[0]] = row[1]
	
	#for tort in mindate.keys():
	#	cursor = db.cursor()
	#	cursor.execute( """ UPDATE FI_aggregate SET Tortoise_status = 'E' where Tortoise_number = %s and tortoise_status = "T" and date > %s; """,(tort, mindate[tort]))
		
	#db.commit()
	
	### update 10/29/2014: choosing a median date for all the "R" torts to become "ER"
	datelist = list(sorted(set((mindate.values()))))
	convert_r_date =  datelist[int(round(0.5*len(datelist)))]
	cursor = db.cursor()
	cursor.execute( """ UPDATE FI_aggregate SET Tortoise_status = 'ER' where Tortoise_status = "R" and date > %s; """,(convert_r_date))
	db.commit()
	db.close()
	

########################################################################################################3
if __name__ == "__main__":
	
	files = ["BSV_aggregate", "CS_aggregate", "HW_aggregate","LM_aggregate", "MC_aggregate", "PV_aggregate", "SG_aggregate", "SL_aggregate", "FI_aggregate",]
	
	#for filename in files:
		#add_tort_status_column (filename)
		###updating "E" to ET to accomodate "ER" 
		#cursor = db.cursor()
		#cursor.execute( """ UPDATE """ +filename + """ SET Tortoise_status = 'ET' where Tortoise_status = 'E'; """)
	update_LM_tort_status()
