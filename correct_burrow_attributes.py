import MySQLdb as sql
import numpy as np
import collections as cl
######################################################################################
#####This script corrects the easting and northing location of burrow. It also deals###########
###### with missing location when there is enough data to fill in the coordinates#############################

######################################################################################

# Open database connection
db = sql.connect("localhost","root","bio123456","burrow_data" )
# prepare a cursor object using cursor() method

###########################################################################################
def extract_burrow_list(filename):
	
	# prepare a cursor object using cursor() method
	cursor = db.cursor()
	query = """ select burrow_number from """ + filename + """ where burrow_number >"" group by burrow_number;"""
	# execute SQL query using execute() method.
	cursor.execute(query)
	# Fetch all the rows in a list of lists.
	results = cursor.fetchall()
	
	return [row[0].upper()  for row in results]
	
		
		



###########################################################################################
def choose_approx_attribute(mylist):
	""" choose an attribute using a rankiing method. This method counts the 
		soil/habitat mention in each entry and assigns a rank. Attributes with more than
		rank 1 are selected to be the final attributes. If all the attributes have rank 1, 
		then an N/A is assigned to the category instead"""
	
	[mylist.remove(num) for num in mylist if num==""]
	# remove all instances of None
	mylist = filter(None, mylist)
	attr_list=[]
	if len(mylist)>0:
		for attr in mylist: 
			# all the attributes are separated by "/". Split string based on this
			if attr!='NoneType': splitstr = attr.split("/")
			# add the attributes to attr_list
			[attr_list.append(string) for string in splitstr]
		# count the frequency of attributes
		attr_counter= cl.Counter(attr_list)
		attr_mean = np.mean(attr_counter.values())
		
		# attr_counter is a dict with key = attribute name and val= frequency of the attribute
		
		###if there are attr that have high freq then pick those that are greater than or equal to the avg freq
		if attr_counter.most_common()[0][1]>1: 
			attr_final =[] # define a list which contains list of selected attributes
			for key, val in attr_counter.items(): 
				if val>= attr_mean: attr_final.append(key)
		
		###if there are burrows with single attrbute or cases with multiple single frequency, choose the/all  attribute to be the final one
		else: attr_final = attr_counter.keys()			
	
	
		attr_final.sort()
		
		attr_final = '/'.join(attr_final)
		return attr_final

###########################################################################################
def choose_approx_elevation(elevation_list):
	"""Chooses an approximate value of elevation by first removing the ooutliers and then taking an average of 
	the remaining elevations"""
	elevation_list = filter(None, elevation_list)
	if len(elevation_list)>0:
		# removing all entries that are not numeric
		[elevation_list.remove(num) for num in elevation_list if num=='None']
		avg = np.mean(elevation_list)
		std = np.std(elevation_list)
	
		for num in elevation_list: 
			# remove outliers
			if (num -avg) > 0.3* std: elevation_list.remove(num)
		return int(np.mean(elevation_list))
	
	

#########################################################################################################
def check_burrow_habitat_consistency(filename, burrow_list):
	
	habitat={}
	for burrow in burrow_list:
		# prepare a cursor object using cursor() method
		cursor = db.cursor()
		cursor.execute(""" select habitat from """ + filename + """ where ucase(burrow_number) =%s ;""",(burrow))
		# Fetch all the rows in a list of lists.
		results = cursor.fetchall()
		# create a dictionay called burr and store all the easting nd northing location as tuple
		habitat[burrow] =[row[0] for row in results]
	
	# habitat_final is a dict with key= burrow id  and elevation as val
	habitat_final={}
	for burrow in habitat.keys():			
		habitat_final[burrow] = choose_approx_attribute(habitat[burrow])
		#print ("habitat match not found for"), burrow, habitat[burrow], habitat_final[burrow] 
		
	return habitat_final
	

#########################################################################################################
def check_burrow_soil_consistency(filename, burrow_list):
	
	soil={}
	for burrow in burrow_list:
		# prepare a cursor object using cursor() method
		cursor = db.cursor()
		cursor.execute(""" select soil from """ + filename + """ where ucase(burrow_number) =%s ;""",(burrow))
		
		# Fetch all the rows in a list of lists.
		results = cursor.fetchall()
		# create a dictionay called burr and store all the easting nd northing location as tuple
		soil[burrow] =[row[0] for row in results]
	
	# soil_final is a dict with key= burrow id  and elevation as val
	soil_final ={}
	for burrow in burrow_list:	
		soil_final[burrow] = choose_approx_attribute(soil[burrow])
		#print ("soil match not found for"), burrow, soil[burrow], soil_final[burrow]
	
	

	return soil_final
	

#########################################################################################################
def check_burrow_elevation_consistency(filename, burrow_list):
	
	elev={}
	for burrow in burrow_list:
		# prepare a cursor object using cursor() method
		cursor = db.cursor()
		cursor.execute(""" select elevation from """ + filename + """ where ucase(burrow_number) =%s ;""",(burrow))
		# Fetch all the rows in a list of lists.
		results = cursor.fetchall()
		# create a dictionay called burr and store all the easting nd northing location as tuple
		elev[burrow] =[row[0] for row in results]
			
	# elev_final is a dict with key= burrow id  and elevation as val
	elev_final={}
	
	for burrow in elev.keys():			
		elev_counter= cl.Counter(elev[burrow])
		# if the most frequent elevation occurs more than 1, accept that coordinate to be correct:
		if elev_counter.most_common()[0][1]>1: 
			elev_final[burrow] =  elev_counter.most_common()[0][0]
			#print ("burrow elevation matched!"), burrow, elev_final[burrow] 
		
		else: 
			#print ("approximation elevation value for"), burrow, elev[burrow]
			elev_final[burrow] = choose_approx_elevation(elev[burrow])
	
	
	
	
	return elev_final

###########################################################################################

def update_locations(filename, elev_final, soil_final, habitat_final, burrow_list):
	
	""" update elevation entries"""
	db = sql.connect("localhost","root","bio123456","burrow_data" )
	cursor = db.cursor()
	#cursor.execute("""ALTER TABLE """ + filename +""" ADD soil_clean VARCHAR(100) AFTER soil;""")
	#cursor.execute("""ALTER TABLE """ + filename +""" ADD habitat_clean VARCHAR(100) AFTER habitat;""")
	#cursor.execute("""ALTER TABLE """ + filename +""" ADD elevation_clean VARCHAR(100) AFTER elevation;""")
	for burr in burrow_list:
		db = sql.connect("localhost","root","bio123456","burrow_data" )
		#prepare a cursor object using cursor() method
		cursor = db.cursor()
		cursor.execute( """ UPDATE """ + filename + """ SET elevation_clean = %s WHERE ucase(Burrow_number)=%s""",((elev_final[burr], burr)))
		db.commit()
		cursor.execute( """ UPDATE """ + filename + """ SET soil_clean = %s WHERE ucase(Burrow_number)=%s""",((soil_final[burr], burr)))
		db.commit()
		cursor.execute( """ UPDATE """ + filename + """ SET habitat_clean = %s WHERE ucase(Burrow_number)=%s""",((habitat_final[burr], burr)))
		db.commit()
		db.close()
###########################################################################################	

if __name__ == "__main__":
# This is the main function, where all the tasks get created

	files = ["BSV_aggregate", "CS_aggregate", "HW_aggregate","LM_aggregate", "MC_aggregate", "PV_aggregate", "SG_aggregate", "SL_aggregate", "FI_aggregate",]
	files = ["SG_aggregate"]

	for filename in files:
		print ("file="), filename
		#connect to the database
		db = sql.connect("localhost","root","bio123456","burrow_data")
		burrow_list = extract_burrow_list(filename)
		elev_final = check_burrow_elevation_consistency(filename, burrow_list)
		soil_final = check_burrow_soil_consistency(filename, burrow_list)
		habitat_final = check_burrow_habitat_consistency(filename, burrow_list)
		update_locations(filename, elev_final, soil_final, habitat_final, burrow_list)
