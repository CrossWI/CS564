DROP TABLE IF EXISTS Items;
DROP TABLE IF EXISTS Bids;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS CategoryInfos;
DROP TABLE IF EXISTS Categories;

CREATE TABLE Items(ItemID int PRIMARY KEY,
				   Name CHAR(300),
				   Started TIMESTAMP,
				   Ends TIMESTAMP,
				   Currently DECIMAL,
				   SellerID CHAR(300),
				   Description CHAR(300),
				   First_Bid DECIMAL,
				   Number_of_Bids int,
				   BuyPrice DECIMAL,
				   FOREIGN KEY (SellerID) REFERENCES Users(UserID));

CREATE TABLE Users(UserID CHAR(300) PRIMARY KEY,
				   Location CHAR(300),
				   Country CHAR(300),
				   Rating int,
				   isSeller int,
				   isBidder int);

CREATE TABLE Bids(ItemID int,
				  BidderID CHAR(300),
				  Time TIMESTAMP,
				  Amount DECIMAL,
				  FOREIGN KEY (ItemID) REFERENCES Items(ItemID),
				  FOREIGN KEY (BidderID) REFERENCES Users(UserID),
				  PRIMARY KEY (ItemID, Time));

CREATE TABLE CategoryInfos(ItemID int,
					  CategoryID int,
					  FOREIGN KEY (ItemID) REFERENCES Items(ItemID),
					  FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID),
					  PRIMARY KEY (ItemID, CategoryID));
					  
CREATE TABLE Categories(CategoryID int PRIMARY KEY,
						Name CHAR(300));