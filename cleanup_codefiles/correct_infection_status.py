import MySQLdb as sql
import numpy as np
import collections as cl
######################################################################################
#####This script corrects the infection status of a tortoise. ###########
######################################################################################

# Open database connection
db = sql.connect("localhost","root","bio123456","burrow_data" )
# prepare a cursor object using cursor() method

###########################################################################################	
def find_urtd(str1):
	"""look for urtd in string. Convert the string to ucase first """
	str2 = str1.upper()
	return str2.find("URTD") > -1 
	
###########################################################################################	
def find_shell(str1):
	"""look for urtd in string. Convert the string to ucase first """
	str2 = str1.upper()
	return str2.find("SHELL") > -1 


###########################################################################################
def add_new_column(filename):
	""" Add a new column after column tort_condition """
	cursor = db.cursor()
	cursor.execute("""ALTER TABLE """ + filename +""" ADD tort_condition_clean VARCHAR(1000) AFTER tort_condition;""")

###########################################################################################

def extract_tortlist(filename):
	
	cursor = db.cursor()
	cursor.execute( """  select ucase(Tortoise_number) from """ + filename + """  group by ucase(Tortoise_number) ; """)
	results = cursor.fetchall()
	tortlist = [row[0] for row in results]
	
	return tortlist
	
###########################################################################################

def track_infection(filename, tortlist):

	for tort in tortlist:
		datedict={}
		cursor = db.cursor()
		cursor.execute( """  select date, tort_condition from """ + filename + """  where tortoise_number = %s group by date; """, (tort))
		results = cursor.fetchall()
		for row in results:
			
			is_urtd = find_urtd(row[1])
			is_shell = find_shell(row[1])
			
			if is_urtd==True and is_shell==True: datedict[row[0]] = "URTD+SHELL"
			if is_urtd==True and is_shell==False: datedict[row[0]] = "URTD"
			if is_urtd==False and is_shell==True: datedict[row[0]] = "SHELL"
			if is_urtd==False and is_shell==False: datedict[row[0]] =  "NA"
			
		### update tort_condition to urtd for all obervations after the initial date
		#if len([key for key, val in datedict.items() if val=="URTD" or val=="URTD+SHELL"]) > 0:
		#	print ("tort====="), tort
		#	print  [key for key, val in datedict.items() if val=="URTD" or val=="URTD+SHELL"]
		#	min_urtd_date = min([key for key, val in datedict.items() if val=="URTD" or val=="URTD+SHELL"])
		#	print ("minimum date"), min_urtd_date
			#for date in datedict.keys():
			#	if date>= min_urtd_date:
			#		if datedict[date]=="URTD+SHELL": datedict[date]="URTD+SHELL"
			#		if datedict[date]=="URTD": datedict[date]="URTD"
			#		if datedict[date]=="SHELL": datedict[date]="URTD+SHELL"
			#		if datedict[date]=="NA": datedict[date]="URTD"
		#update infection status
		for date in datedict.keys():		
			cursor = db.cursor()
			cursor.execute( """ UPDATE """ + filename + """ SET tort_condition_clean = %s WHERE ucase(Tortoise_number)=%s and date = %s """,(datedict[date], tort, date))
		db.commit()
				
###########################################################################################
if __name__ == "__main__":
# This is the main function, where all the tasks get created

	files = ["CS_aggregate", "FI_aggregate","HW_aggregate","MC_aggregate", "PV_aggregate", "SL_aggregate"]
	#files = ["BSV_aggregate", "LM_aggregate",  "SG_aggregate"]
	for filename in files:
		db = sql.connect("localhost","root","bio123456","burrow_data" )
		add_new_column(filename)
		#tortlist = extract_tortlist(filename)
		#print tortlist
		#track_infection(filename, tortlist)
		
		
