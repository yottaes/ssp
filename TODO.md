# TODO

## Issues

- when parquet file is too small, for example if we are running incremental snapshot parsing
  and second writer have nothing to write -> we'll get panic from duckdb. Need to handle that.
