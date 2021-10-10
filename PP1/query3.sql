SELECT COUNT(ItemID)
FROM (SELECT ItemID, COUNT(CategoryID) AS cat_count
	  FROM CategoryInfos
	  GROUP BY ItemID)
WHERE cat_count == 4;