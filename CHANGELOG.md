pyzef-0.15.7
============

Breaking changes:

- SimpleGQL: no longer takes the schema_file and graph as regular arguments, but
  as `--schema-file` and `--data-tag` instead.
  
Minor changes:

- SimpleGQL: all main module arguments can be passed as environment variables,
  e.g. instead of `--schema-file` you can use `SIMPLEGQL_SCHEMA_FILE`.
  Commandline args take priority.

pyzef-0.15.6
============

- `schema` zefop
- Preliminary Windows support.