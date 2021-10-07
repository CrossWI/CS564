DROP TABLE IF EXISTS Items;
DROP TABLE IF EXISTS ItemBids;
DROP TABLE IF EXISTS Bids;
DROP TABLE IF EXISTS Users
DROP TABLE IF EXISTS Category;

CREATE TABLE Items(ItemID PRIMARY KEY INTEGER,
				   Name CHAR(300),
				   Started TIMESTAMP,
				   Ends TIMESTAMP,
				   SellerID CHAR(300),
				   Description TEXT);

CREATE TABLE ItemBids(ItemID PRIMARY KEY INTEGER,
					  Currently DECIMAL,
					  First_Bid DECIMAL,
					  Number_of_Bids INTEGER
					  FOREIGN KEY (ItemID) REFERENCES Items(ItemID));

CREATE TABLE Bids(ItemID INTEGER,
				  UserID CHAR(300),
				  Time TIMESTAMP,
				  Amount DECIMAL,
				  FOREIGN KEY (ItemID) REFERENCES Items(ItemID),
				  FOREIGN KEY (UserID) REFERENCES Users(UserID));

CREATE TABLE Users(UserID CHAR(300),
				   Location CHAR(300),
				   Country CHAR(300),
				   Rating INTEGER);

CREATE TABLE Category(ItemID INTEGER,
					  Category CHAR(300),
					  FOREIGN KEY (ItemID) REFERENCES Items(ItemID));