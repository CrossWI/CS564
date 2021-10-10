
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
from collections import defaultdict

columnSeparator = "|"

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

itemsTable = {}
usersTable = {}
bidsTable = defaultdict(list)
itemCategoryTable = defaultdict(list)
categoriesTable = {}
categoryID = 0
def parseJson(json_file):
	global itemsTable
	global usersTable
	global bidsTable
	global itemCategoryTable
	global categoriesTable
	global categoryID

	with open(json_file, 'r') as f:

		items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file
		for item in items:
			"""
			TODO: traverse the items dictionary to extract information from the
			given `json_file' and generate the necessary .dat files to generate
			the SQL tables based on your relation design
			"""
			# get item data:
			itemID = item["ItemID"]
			seller = item["Seller"]
			sellerID = seller["UserID"]
			if item["Description"] is None:
				item["Description"] = "NULL"
			itemsTable[itemID] = [
				itemID,
				item["Name"],
				transformDttm(item["Started"]),
				transformDttm(item["Ends"]),
				transformDollar(item["Currently"]),
				sellerID,
				item["Description"],
				transformDollar(item["First_Bid"]),
				item["Number_of_Bids"],
				item.get("Buy_Price", "NULL")
			]
			# add sellers to users
			if sellerID in usersTable:
				# not sure if updating is currect but this should update isSeller
				usersTable[sellerID][-2] = "1"
			else:
				usersTable[sellerID] = [
					sellerID,
					item["Location"],
					item["Country"],
					seller["Rating"],
					"1",
					"0"
				]
			if item["Bids"]:
				for bid in item["Bids"]:
					info = bid["Bid"]
					bidder = info["Bidder"]
					bidderID = bidder["UserID"]
					bidsTable[itemID].append(
						(
						bidderID,
						transformDttm(info["Time"]),
						transformDollar(info["Amount"]),
						)
					)
					if bidderID in usersTable:
						usersTable[bidderID][-1] = "1"
					else:
						usersTable[bidderID] = [
							bidderID,
							bidder.get("Location", "NULL"),
							bidder.get("Country", "NULL"),
							bidder["Rating"],
							"0",
							"1"
						]
			# Categories
			for category in item["Category"]:
				if category not in categoriesTable:
					categoryID += 1
					categoriesTable[category] = str(categoryID)
				category_info = categoriesTable[category]
				if category_info not in itemCategoryTable[itemID]:
					itemCategoryTable[itemID].append(category_info)



def writeToDat():
	replace_quote = lambda string: '"' + string.replace("'", "''").replace('"', '""') + '"'
	write_line = lambda string: columnSeparator.join(replace_quote(s) for s in string) + "\n"

	with open("items.dat", "w") as items:
		items.writelines(write_line(item) for item in itemsTable.values())
	with open("users.dat", "w") as users:
		users.writelines(write_line(user) for user in usersTable.values())
	with open("bids.dat", "w") as bids:
		bids.writelines(itemID + columnSeparator + write_line(bid)
		for itemID, itemBid in bidsTable.items()
		for bid in itemBid)
	with open("categories.dat", "w") as categories:
		categories.writelines(replace_quote(name) + columnSeparator + ID + "\n"
		for name, ID in categoriesTable.items())
	with open("itemcategories.dat", "w") as itemcategories:
		itemcategories.writelines("\n".join(itemID + columnSeparator + categorID for categorID in categoryIDs) + "\n"
		for itemID, categoryIDs in itemCategoryTable.items())
	


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
	writeToDat()

if __name__ == '__main__':
	main(sys.argv)
