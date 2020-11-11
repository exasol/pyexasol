SELECT high_cardinality_col, count(*)
FROM test_data_1
GROUP BY 1;
