# 0.21.2 - 2021-11-11

- Fixed a bug in `ExaStatement` when no rows were fetched. It could happen when data set has less than 1000 rows, but the amount of data exceeds maximum chunk size.

