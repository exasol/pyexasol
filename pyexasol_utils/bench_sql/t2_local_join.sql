SELECT b.hashed_id, count(*)
FROM test_data_1 a
    JOIN test_data_2 b ON (a.low_cardinality_col=b.id)
GROUP BY 1
ORDER BY 1;
