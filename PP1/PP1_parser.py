
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

items_tab = {}
users = {}
bids = defaultdict(list)
item_categories = defaultdict(list)
categories = {}
categoryID = 0
def parseJson(json_file):
	global items_tab
	global users
	global bids
	global item_categories
	global categories
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
			ItemID = item['ItemID']
			Name = item['Name']
			Started = item['Started']
			Ends = item['Ends']
			Currently = item['Currently']
			First_Bid = item['First_Bid']
			Number_of_Bids = item['Number_of_Bids']
			Seller = item['Seller']
			SellerID = Seller['UserID']
			Buy_Price = ''

			if (Seller == None):
				SellerID = 'NULL'
			if ('Buy_Price' in item.keys()):
				Buy_Price = item['Buy_Price']
			else:
				Buy_Price = 'NULL'
			
			if (item['Description'] == None):
				item['Description'] = 'NULL'
			Description = item['Description']

			items_tab[ItemID] = [ItemID, Name, transformDttm(Started), transformDttm(Ends), transformDollar(Currently), SellerID, Description, transformDollar(First_Bid), Number_of_Bids, Buy_Price]

			# get user and bid data:
			Location = item['Location']
			Country = item['Country']
			Rating = item['Seller']['Rating']
			Bids = item['Bids']

			if (SellerID in users):
				users[SellerID][-2] = '1'
			else: 
				users[SellerID] = [SellerID, Location, Country, Rating, '1', '0']
			
			if (Bids != None):
				for bid in Bids:
					Bidder = bid['Bid']['Bidder']
					BidderID = Bidder['UserID']
					Location = ''
					Country = ''
					Rating = ''

					Time = bid['Bid']['Time']
					Amount = bid['Bid']['Amount']

					if ('Location' in Bidder.keys()):
						Location = Bidder['Location']
					else:
						Location = 'NULL'
					if ('Country' in Bidder.keys()):
						Country = Bidder['Country']
					else:
						Country = 'NULL'
					if ('Rating' in Bidder.keys()):
						Rating = Bidder['Rating']
					else:
						Rating = 'NULL'

					bids[ItemID].append((BidderID, transformDttm(Time), transformDollar(Amount)))

					if (BidderID in users):
						users[BidderID][-1] = 1
					else:
						users[BidderID] = [BidderID, Location, Country, Rating, '0', '1']

			for category in item["Category"]:
				if (category not in categories):
					categoryID += 1
					categories[category] = str(categoryID)

				category_info = categories[category]
				if (category_info not in item_categories[ItemID]):
					item_categories[ItemID].append(category_info)
	
def writeToFiles():
	replace_quote = lambda string: '"' + string.replace("'", "''").replace('"', '""') + '"'
	write_line = lambda string: columnSeparator.join(replace_quote(str(s)) for s in string) + "\n"

	with open("Items.dat", "w") as f1:
		f1.writelines(write_line(item) for item in items_tab.values())

	with open("Users.dat", "w") as f2:
		f2.writelines(write_line(user) for user in users.values())

	with open("Bids.dat", "w") as f3:
		f3.writelines(itemID + '|' + write_line(bid)
		for itemID, itemBid in bids.items()
		for bid in itemBid)

	with open("ItemCategories.dat", "w") as f4:
		f4.writelines("\n".join(itemID + '|' + categorID for categorID in categoryIDs) + "\n"
		for itemID, categoryIDs in item_categories.items())
	with open("Categories.dat", "w") as f5:
		f5.writelines(replace_quote(name) + '|' + categoryID + "\n"
		for name, categoryID in categories.items())

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
	writeToFiles()

if __name__ == '__main__':
	main(sys.argv)
