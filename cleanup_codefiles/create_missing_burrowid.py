import MySQLdb as sql
import numpy as np
import collections as cl
import csv
######################################################################################
#####Step2: A lot of entries lack burrow id. Create new burrow id names based on location data(for the ones
################that could not be matched to known burrow ids)#############################################
######################################################################################

# Open database connection
db = sql.connect("localhost","root","bio123456","burrow_data" )
# prepare a cursor object using cursor() method

###########################################################################################
def extract_burrow_location(filename):
	"""Extract easting and northing location of known burrows and store in a dict """
	
	burloc={}
	cursor = db.cursor()
	cursor.execute(""" select burrow_number, UTM_Easting, UTM_Northing from """ + filename + """ where burrow_number >'' and utm_easting >'0' group by burrow_number""")
	results = cursor.fetchall()
	for row in results:	
		burloc[row[0]] = [row[1], row[2]]
		
	return burloc
	
###########################################################################################
def create_missing_burrowid(filename, burloc):

	locdict = {}
	count = 0	
	cursor = db.cursor()
	cursor.execute(""" select date, tortoise_number, utm_easting, utm_northing from """ + filename + """ where location="Burrow" and burrow_number='' """)
	results = cursor.fetchall()
	for row in results:
		match_found, bestbur = find_closest_burrow(burloc, row[2], row[3])
		if match_found==True:
			cursor = db.cursor()
			cursor.execute(""" update """ + filename + """ set burrow_number = %s where date = %s and tortoise_number = %s and location ='Burrow'; """, (bestbur, row[0], row[1]))
			##updating location so that all location of a particular burrow match
			cursor.execute(""" update """ + filename + """ set utm_easting = %s where date = %s and burrow_number = %s ; """, (burloc[bestbur][0], row[0], bestbur))
			cursor.execute(""" update """ + filename + """ set utm_northing = %s where date = %s and burrow_number = %s ; """, (burloc[bestbur][1], row[0], bestbur))
			db.commit()
		else:
			locdict[count] = [row[2], row[3]]
			count+=1
	return locdict

############################################################################################
	

def find_matches(locdict, burloc, site):
	"""find all the reported burrow locations that are within 5m of the focal unknown burrow id """
	
	index_resolved =[]
	
	for key, val in locdict.items():
		if key not in index_resolved:
			focaleast = val[0]
			focalnorth = val[1]
			distlist = [(np.sqrt((focaleast - val1[0])**2+(focalnorth - val1[1])**2), key1) for key1, val1 in locdict.items() if key1!=key]
			filter_distlist = sorted([num for num in distlist if num[0] <=5])
			#print ("hits for=="), key, val, distlist
			if len(filter_distlist)>0: 
				# if there are hits then assign the burrow with an unique id and delete all the keys in locdict that are present in distlist (to avoid dupliate burrow id assignment)
				print ("hits for=="), key, val, filter_distlist
				burloc["B-"+site+"-"+str(key)] = [focaleast, focalnorth]
				# appending the all keys in index_resolved list so that they are skipped for subsequent iterations
				for num in filter_distlist: index_resolved.append(num[1])
	
	
	locdict = create_missing_burrowid(filename, burloc)		
		

############################################################################################

def find_closest_burrow(burloc, target_easting, target_northing):
	"""try to find a match. Print in order of best match"""
	
	match_found = False
	bestbur = None
	distance =[(np.sqrt((burloc[burrow][0]-target_easting)**2+(burloc[burrow][1]-target_northing)**2), burrow) for burrow in burloc.keys()]
	##remove all the distances that are greater than 5 m 
	filter_distance = [(x,y) for (x,y) in distance if x<=5]
	sort_filter_distance = sorted(filter_distance)
	if len(sort_filter_distance) >0 : 
		match_found = True
		print ("match found!"), sort_filter_distance, sort_filter_distance[0][1]
		bestbur = sort_filter_distance[0][1]
	return match_found, bestbur
		



#################################################################################################3
if __name__ == "__main__":
	files = ["BSV_aggregate", "CS_aggregate", "FI_aggregate","HW_aggregate","LM_aggregate", "MC_aggregate", "PV_aggregate", "SG_aggregate", "SL_aggregate"]
	sites = ["BSV", "CS", "FI","HW","LM", "MC", "PV", "SG", "SL"]
	#files = ["PV_aggregate"]
	#sites= ["PV"]
	
	for filename,site in zip(files,sites):
		print filename
		burloc = extract_burrow_location(filename)
		locdict = create_missing_burrowid(filename, burloc)
		find_matches(locdict, burloc, site)
		 
