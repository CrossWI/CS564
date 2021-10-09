DROP TABLE IF EXISTS Items;
DROP TABLE IF EXISTS ItemBids;
DROP TABLE IF EXISTS Bids;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Category;

CREATE TABLE Items(ItemID INTEGER PRIMARY KEY,
				   Name CHAR(300),
				   Started TIMESTAMP,
				   Ends TIMESTAMP,
				   SellerID CHAR(300),
				   Description TEXT,
				   FOREIGN KEY (SellerID) REFERENCES Users(UserID));

CREATE TABLE ItemBids(ItemID INTEGER PRIMARY KEY,
					  Currently DECIMAL,
					  First_Bid DECIMAL,
					  Number_of_Bids INTEGER,
					  FOREIGN KEY (ItemID) REFERENCES Items(ItemID));

CREATE TABLE Bids(ItemID INTEGER,
				  UserID CHAR(300),
				  Time TIMESTAMP,
				  Amount DECIMAL,
				  FOREIGN KEY (ItemID) REFERENCES Items(ItemID),
				  FOREIGN KEY (UserID) REFERENCES Users(UserID),
				  PRIMARY KEY (ItemID, UserID, Amount));

CREATE TABLE Users(UserID CHAR(300) PRIMARY KEY,
				   Location CHAR(300),
				   Country CHAR(300),
				   Rating INTEGER);

CREATE TABLE Category(ItemID INTEGER,
					  Category_Names CHAR(300),
					  FOREIGN KEY (ItemID) REFERENCES Items(ItemID),
					  PRIMARY KEY (ItemID, Category_Names));