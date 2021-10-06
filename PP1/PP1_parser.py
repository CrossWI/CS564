
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"
NULL_CONST = 'NULL'
NULL_CONST_SEP = '|' + 'NULL'

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
		'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
	return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
	if mon in MONTHS:
		return MONTHS[mon]
	else:
		return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
	dttm = dttm.strip().split(' ')
	dt = dttm[0].split('-')
	date = '20' + dt[2] + '-'
	date += transformMonth(dt[0]) + '-' + dt[1]
	return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
	if money == None or len(money) == 0:
		return money
	return sub(r'[^\d.]', '', money)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
	with open(json_file, 'r') as f:
		items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
		for item in items:
			"""
			TODO: traverse the items dictionary to extract information from the
			given `json_file' and generate the necessary .dat files to generate
			the SQL tables based on your relation design
			"""
			# get item data:

			ItemID = item['ItemID']
			Name = item['Name']
			Started = item['Started']
			Ends = item['ends']
			SellerID = item['Seller']['UserID']
			Description = item['description']

			item_data = fillItems(ItemID, Name, Started, Ends, SellerID, Description)
			# if (ItemID != None):
			# 	item_data += ItemID
			# else: 
			# 	item_data += NULL_CONST
			# if (Name != None):
			# 	item_data += '|' + Name
			# else:
			# 	item_data += NULL_CONST_SEP
			# if (Started != None):
			# 	item_data += '|' + Started
			# else:
			# 	item_data += NULL_CONST_SEP
			# if (Ends != None):
			# 	item_data += '|' + Ends
			# else:
			# 	item_data += NULL_CONST_SEP
			# if (SellerID != None):
			# 	item_data += '|' + SellerID
			# else:
			# 	item_data += NULL_CONST_SEP
			# if (Description != None):
			# 	item_data += '|' + Description
			# else:
			# 	item_data += NULL_CONST_SEP

			f_item = open("Items.dat", "a")
			f_item.write(item_data)
			f_item.close()

			# get item_bids data:
			Currently = item['Currently']
			First_Bid = item['First_Bid']
			Number_of_Bids = item['Number_of_Bids']

			item_bids_data = fillItemBids(ItemID, Currently, First_Bid, Number_of_Bids)
			# if (ItemID != None):
			# 	item_bids_data += ItemID
			# else: 
			# 	item_bids_data += NULL_CONST
			# if (Currently != None):
			# 	item_bids_data += '|' + Currently
			# else: 
			# 	item_bids_data += NULL_CONST_SEP
			# if (First_Bid != None):
			# 	item_bids_data += '|' + First_Bid
			# else:
			# 	item_bids_data += NULL_CONST_SEP
			# if (Number_of_Bids != None):
			# 	item_bids_data += '|' + Number_of_Bids
			# else:
			# 	item_bids_data += NULL_CONST_SEP
			
			f_item_bids = open("ItemBids.dat", "a")
			f_item_bids.write(item_bids_data)
			f_item_bids.close()

			# get bid data:
			UserID = item['Bids']['Bidder']['UserID']
			Time = item['Bids']['Time']
			Amount = item['Bids']['Amount']

			bids_data = fillBidsData(ItemID, UserID, Time, Amount)
			# if (ItemID != None):
			# 	bids_data += ItemID
			# else: 
			# 	bids_data += NULL_CONST
			# if (UserID != None):
			# 	bids_data += '|' + UserID
			# else: 
			# 	bids_data += NULL_CONST_SEP
			# if (Time != None):
			# 	bids_data += '|' + Time
			# else:
			# 	bids_data += NULL_CONST_SEP
			# if (Amount != None):
			# 	bids_data += '|' + Amount
			# else:
			# 	bids_data += NULL_CONST_SEP
			
			f_bids = open("Bids.dat", "a")
			f_bids.write(bids_data)
			f_bids.close()

			# get user data:
			Location = item['Bids']['Bidder']['Location']
			Country = item['Bids']['Bidder']['Country']
			Rating = item['Bids']['Bidder']['Rating']

			user_data = fillUserData(UserID, Location, Country, Rating)
			# if (UserID != None):
			# 	user_data += UserID
			# else: 
			# 	user_data += NULL_CONST
			# if (Location != None):
			# 	user_data += '|' + Location
			# else:
			# 	user_data += NULL_CONST_SEP
			# if (Country != None):
			# 	user_data += '|' + Country
			# else:
			# 	user_data += NULL_CONST_SEP
			# if (Rating != None):
			# 	user_data += '|' + Rating
			# else:
			# 	user_data += NULL_CONST_SEP
			
			f_users = open("Users.dat", "a")
			f_users.write(user_data)
			f_users.close()

			# get category data
			Category = item['Category']

			category_data = fillCategoryData(ItemID, Category)
			# if (ItemID != None):
			# 	category_data += ItemID
			# else: 
			# 	category_data += NULL_CONST
			# if (Category != None):
			# 	category_data += Category
			# else: 
			# 	category_data += NULL_CONST_SEP

			f_category = open("Category.dat", "a")
			f_category.write(category_data)
			f_category.close()


def fillItems(ItemID, Name, Started, Ends, SellerID, Description):
	items_data = ''

	if (ItemID != None):
		item_data += ItemID
	else: 
		item_data += NULL_CONST
	if (Name != None):
		item_data += '|' + Name
	else:
		item_data += NULL_CONST_SEP
	if (Started != None):
		item_data += '|' + Started
	else:
		item_data += NULL_CONST_SEP
	if (Ends != None):
		item_data += '|' + Ends
	else:
		item_data += NULL_CONST_SEP
	if (SellerID != None):
		item_data += '|' + SellerID
	else:
		item_data += NULL_CONST_SEP
	if (Description != None):
		item_data += '|' + Description
	else:
		item_data += NULL_CONST_SEP

	return items_data

def fillItemBids(ItemID, Currently, First_Bid, Number_of_Bids):
	item_bids_data = ''
	if (ItemID != None):
		item_bids_data += ItemID
	else: 
		item_bids_data += NULL_CONST
	if (Currently != None):
		item_bids_data += '|' + Currently
	else: 
		item_bids_data += NULL_CONST_SEP
	if (First_Bid != None):
		item_bids_data += '|' + First_Bid
	else:
		item_bids_data += NULL_CONST_SEP
	if (Number_of_Bids != None):
		item_bids_data += '|' + Number_of_Bids
	else:
		item_bids_data += NULL_CONST_SEP
	return item_bids_data

def fillBidsData(ItemID, UserID, Time, Amount):
	bids_data = ''

	if (ItemID != None):
		bids_data += ItemID
	else: 
		bids_data += NULL_CONST
	if (UserID != None):
		bids_data += '|' + UserID
	else: 
		bids_data += NULL_CONST_SEP
	if (Time != None):
		bids_data += '|' + Time
	else:
		bids_data += NULL_CONST_SEP
	if (Amount != None):
		bids_data += '|' + Amount
	else:
		bids_data += NULL_CONST_SEP

	return bids_data

def fillUserData(UserID, Location, Country, Rating):
	user_data = ''
	if (UserID != None):
		user_data += UserID
	else: 
		user_data += NULL_CONST
	if (Location != None):
		user_data += '|' + Location
	else:
		user_data += NULL_CONST_SEP
	if (Country != None):
		user_data += '|' + Country
	else:
		user_data += NULL_CONST_SEP
	if (Rating != None):
		user_data += '|' + Rating
	else:
		user_data += NULL_CONST_SEP
	return user_data

def fillCategoryData(ItemID, Category):
	category_data = ''
	if (ItemID != None):
		category_data += ItemID
	else: 
		category_data += NULL_CONST
	if (Category != None):
		category_data += Category
	else: 
		category_data += NULL_CONST_SEP

	return category_data

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
	if len(argv) < 2:
		print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
		sys.exit(1)
	# loops over all .json files in the argument
	for f in argv[1:]:
		if isJson(f):
			parseJson(f)
			print("Success parsing " + f)

if __name__ == '__main__':
	main(sys.argv)
