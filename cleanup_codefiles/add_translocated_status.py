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

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	#cursor.execute( """ UPDATE BSV_aggregate SET Tortoise_status = 'T' where left(Tortoise_number, 1) = '1';""")
	#cursor.execute( """ UPDATE BSV_aggregate SET Tortoise_status = 'T' where left(Tortoise_number, 1) = '4';""")
	
	######### find the arrival date of translocated animals#####
	
	cursor.execute("""select min(date) from BSV_aggregate where tortoise_status="T" ;""")
	results = cursor.fetchall()
	mindate = results[0][0]
	print ("date of arrival is"), mindate
		
	# the residents before this timeperiod were actually controls
	cursor.execute( """ UPDATE BSV_aggregate SET Tortoise_status = 'C' where Tortoise_status ="R" and date < %s;""", (mindate))
	db.commit()
	db.close()
	

######################################################################################
def update_LM_tort_status():

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute( """ UPDATE LM_aggregate SET Tortoise_status = 'T' where left(Tortoise_number, 1) = '7';""")
	cursor.execute( """ UPDATE LM_aggregate SET Tortoise_status = 'R' where Tortoise_status is NULL;""")
	db.commit()
	db.close()
	
######################################################################################
def update_SG_tort_status():

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute( """ UPDATE SG_aggregate SET Tortoise_status = 'T' where Tortoise_status is NULL;""")
	db.commit()
	db.close()

######################################################################################
def update_SL_tort_status():

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute( """ UPDATE SL_aggregate SET Tortoise_status = 'C' where Tortoise_status = "R" ;""")
	db.commit()
	db.close()


######################################################################################
def update_MC_tort_status():

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute( """ UPDATE MC_aggregate SET Tortoise_status = 'C' where Tortoise_status = "R" ;""")
	db.commit()
	db.close()

######################################################################################
def update_PV_tort_status():

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute( """ UPDATE PV_aggregate SET Tortoise_status = 'C' where Tortoise_status = "R" ;""")
	db.commit()
	db.close()

######################################################################################
def update_HW_tort_status():

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute( """ UPDATE HW_aggregate SET Tortoise_status = 'C' where Tortoise_status = "R" ;""")
	db.commit()
	db.close()

######################################################################################
def update_CS_tort_status():

	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	cursor.execute( """ UPDATE CS_aggregate SET Tortoise_status = 'C' where Tortoise_status = "R" ;""")
	db.commit()
	db.close()
	
######################################################################################
def update_FI_tort_status():
	
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	os.chdir("/home/pratha/Dropbox/SB Lab_Pratha/0 Burrow Use/BarstowCA")
	tort_status={}
	
	with open ("FI_tort_status.csv", 'r') as csvfile:
		fileread = csv.reader(csvfile, delimiter = ',')
		next(fileread, None) #skip header
		for row in fileread:
			if row[1]=="Control": tort_status[row[0]]='C'
			elif row[1]=="Resident": tort_status[row[0]]='R'
			elif row[1] == "Trans": tort_status[row[0]]='T'

	for tort in tort_status.keys():
		cursor = db.cursor()
		print tort, tort_status[tort]
		cursor.execute( """ UPDATE FI_aggregate SET Tortoise_status = %s where Tortoise_number = %s; """,(tort_status[tort], tort))
		
	################################3
	# for those cases where the torts swtich id form control --> resident
	
	tort_status={}
	basedate=datetime.date(2008,04,01)
	with open ("FI_tort_status.csv", 'r') as csvfile:
		fileread = csv.reader(csvfile, delimiter = ',')
		next(fileread, None) #skip header
		for row in fileread: 
			if row[1]=="CtoRes": tort_status[row[0]]=basedate+relativedelta(months=int(row[2]))
	
	print tort_status		
	
	for tort in tort_status.keys():
		cursor = db.cursor()
		cursor.execute( """ UPDATE FI_aggregate SET Tortoise_status = 'C' where Tortoise_number = %s and date < %s; """,(tort, tort_status[tort]))
		cursor.execute( """ UPDATE FI_aggregate SET Tortoise_status = 'R' where Tortoise_number = %s and date >= %s; """,(tort, tort_status[tort]))
	
	
	
	
	db.commit()
	db.close()
	
#############################################################################################
def update_FI_tort_status_correction():
	"""11/05/2014. All the translocations were done on 2008-03-27. Correct T and R status according to this information"""
		
	cursor = db.cursor()
	cursor.execute( """ UPDATE FI_aggregate SET Tortoise_status = 'C' where Tortoise_status = 'T' and date <'2008-03-27'; """)
	cursor.execute( """ UPDATE FI_aggregate SET Tortoise_status = 'C' where Tortoise_status = 'R' and date < '2008-03-27'; """)
	
	db.commit()
	db.close()

########################################################################################################3
if __name__ == "__main__":
	
	files = ["BSV_aggregate", "CS_aggregate", "HW_aggregate","LM_aggregate", "MC_aggregate", "PV_aggregate", "SG_aggregate", "SL_aggregate", "FI_aggregate",]
	
	#for filename in files:
	#	add_tort_status_column (filename)
	#update_tort_status()
	#update_FI_tort_status_correction()
