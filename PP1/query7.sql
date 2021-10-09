SELECT count(DISTINCT Category)
FROM Category
WHERE ItemID EXISTS (SELECT ItemID
    FROM ItemBids
    WHERE Currently >= 100)