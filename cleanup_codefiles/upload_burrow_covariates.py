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
def add_covariates_column(filename):

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	#cursor.execute("""ALTER TABLE """ + filename +""" ADD surf_text FLOAT(12,12) AFTER Burrow_azimuth;""")
	#cursor.execute("""ALTER TABLE """ + filename +""" ADD washes_pct FLOAT(12,12) AFTER surf_text;""")
	#cursor.execute("""ALTER TABLE """ + filename +""" ADD surf_ruf FLOAT(12,12) AFTER washes_pct ;""")
	#cursor.execute("""ALTER TABLE """ + filename +""" ADD top_pos FLOAT(12,12) AFTER surf_ruf;""")
	

	cursor.execute("""ALTER TABLE  """ + filename +""" MODIFY surf_text FLOAT(20,12);""")
	cursor.execute("""ALTER TABLE  """ + filename +""" MODIFY washes_pct FLOAT(20,12);""")
	cursor.execute("""ALTER TABLE  """ + filename +""" MODIFY surf_ruf FLOAT(20,12);""")
	cursor.execute("""ALTER TABLE  """ + filename +""" MODIFY top_pos FLOAT(20,12);""")
	

#############################################################################################
def read_data():

	data_add={}
	with open ("cleaned_burrowlist_with_locations_covars.csv", 'r') as csvfile:
			fileread = csv.reader(csvfile, delimiter = ',')
			next(fileread, None) #skip header
			for row in fileread:
				
				site = row[1]
				burrow = row[2]
				if len(row[5]) >0: st = float(row[5])
				else: st = None
				if len(row[6]) >0: wp = float(row[6])
				else: wp = None
				if len(row[7]) >0: sr = float(row[7])
				else: sr = None
				if len(row[8]) >0: tp = float(row[8])
				else: tp = None
				if not data_add.has_key(site): data_add[site]={}
				if not data_add[site].has_key(burrow): data_add[site][burrow]={}
				data_add[site][burrow]["surf_tex"]= st
				data_add[site][burrow]["wash_pct"]= wp
				data_add[site][burrow]["surf_rgh"]= sr
				data_add[site][burrow]["topl_pos"]= tp
				
	return data_add
				
				

#############################################################################################
def add_attributes(site, filename, data_add):
	
	""" update burrow attributes"""
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	
	
	for burrow in data_add[site].keys():
		db = sql.connect("localhost","root","bio123456","burrow_data" )
		
		cursor = db.cursor()
		cursor.execute( """ UPDATE """ + filename + """ SET surf_text = %s WHERE ucase(Burrow_number)=%s""",((data_add[site][burrow]["surf_tex"], burrow)))
		db.commit()
		cursor.execute( """ UPDATE """ + filename + """ SET washes_pct = %s WHERE ucase(Burrow_number)=%s""",((data_add[site][burrow]["wash_pct"], burrow)))
		db.commit()
		cursor.execute( """ UPDATE """ + filename + """ SET surf_ruf = %s WHERE ucase(Burrow_number)=%s""",((data_add[site][burrow]["surf_rgh"], burrow)))
		db.commit()
		
		cursor.execute( """ UPDATE """ + filename + """ SET top_pos = %s WHERE ucase(Burrow_number)=%s""",((data_add[site][burrow]["topl_pos"], burrow)))
		db.commit()
		db.close()
###########################################################################################	

if __name__ == "__main__":
# This is the main function, where all the tasks get created

	files = ["CS_aggregate", "HW_aggregate","LM_aggregate", "MC_aggregate", "PV_aggregate", "SG_aggregate", "SL_aggregate", "FI_aggregate"]
	sites = ["CS", "HW", "LM", "MC", "PV", "SG", "SL", "FI"]
	#files = ["BSV_aggregate"]
	#sites = ["BSV"]
	
	data_add = read_data()
	#print data_add
	for site, filename in zip(sites, files):
		print ("file="), filename
		#connect to the database
		db = sql.connect("localhost","root","bio123456","burrow_data")
		
		add_attributes(site, filename, data_add)
