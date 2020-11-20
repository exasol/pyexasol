CREATE OR REPLACE LUA SET SCRIPT emit_rows(row_count DOUBLE, row_offset DOUBLE, row_step DOUBLE)
EMITS (rn DOUBLE)
AS

function run(ctx)
        for i=ctx.row_offset, ctx.row_count-1, ctx.row_step do
                ctx.emit(i)
        end
end
;


CREATE OR REPLACE TABLE emit_base
(
    rn                      DECIMAL(18,0)
);

CREATE OR REPLACE TABLE test_data_1
(
    id                      DECIMAL(18,0),
    low_cardinality_col     DECIMAL(18,0),
    medium_cardinality_col  DECIMAL(18,0),
    high_cardinality_col    DECIMAL(18,0),
    hashed_id               VARCHAR(32)
);

CREATE OR REPLACE TABLE test_data_2
(
    id                      DECIMAL(18,0),
    hashed_id               VARCHAR(32)
);

CREATE OR REPLACE TABLE test_data_3
(
    id                      DECIMAL(18,0),
    hashed_id               VARCHAR(32)
);

INSERT INTO emit_base
SELECT emit_rows(nproc() * 40, 0, 1);


INSERT INTO test_data_1
SELECT rn AS id
    , MOD(rn, FLOOR(10000 * nproc() * {scale!d})) AS low_cardinality_col
    , MOD(rn, FLOOR(10000000 * nproc() * {scale!d})) AS medium_cardinality_col
    , MOD(rn, FLOOR(100000000 * nproc() * {scale!d})) AS high_cardinality_col
    , hash_md5(rn) AS hashed_id
FROM (
        SELECT emit_rows(FLOOR(1000000000 * nproc() * {scale!f}), 0 + rn, nproc() * 40)
        FROM emit_base
        GROUP BY rn
)
;


INSERT INTO test_data_2
SELECT rn AS id
        , hash_md5(rn) AS hashed_id
FROM (
    SELECT emit_rows(FLOOR(10000 * nproc() * {scale!d}), 0, 1)
)
;


INSERT INTO test_data_3
SELECT rn AS id
        , hash_md5(rn) AS hashed_id
FROM (
        SELECT emit_rows(FLOOR(10000000 * nproc() * {scale!d}), 0, 1)
)
;
