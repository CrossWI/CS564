SELECT COUNT(ItemID)
FROM (SELECT ItemID, COUNT(CategoryID) AS cat_count
	  FROM ItemCategories
	  GROUP BY ItemID)
WHERE cat_count == 4;