SELECT b.hashed_id, count(*)
FROM test_data_1 a
    JOIN test_data_3 b ON (a.medium_cardinality_col=b.id)
GROUP BY 1
ORDER BY 1;
