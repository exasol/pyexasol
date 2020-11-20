SELECT low_cardinality_col, id
FROM (
    SELECT low_cardinality_col
        , id
        , ROW_NUMBER() OVER (PARTITION BY low_cardinality_col ORDER BY id ASC) AS rnum
    FROM test_data_1
)
WHERE rnum=1
;
