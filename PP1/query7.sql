SELECT COUNT(*)
FROM (
        SELECT ItemCategories.CategoryID
        FROM ItemCategories,
            Bids
        WHERE ItemCategories.ItemID = Bids.ItemID
        GROUP BY ItemCategories.CategoryID
        HAVING MAX(Bids.Amount) > 100
    );