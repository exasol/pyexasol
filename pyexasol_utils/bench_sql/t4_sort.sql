SELECT low_cardinality_col, rn
FROM (
    SELECT low_cardinality_col
        , rn
        , ROW_NUMBER() OVER (PARTITION BY low_cardinality_col ORDER BY rn ASC) AS rnum
    FROM test_data_1
)
WHERE rnum=1
;
