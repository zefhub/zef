pyzef-0.15.7
============

Breaking changes:

- SimpleGQL: no longer takes the schema_file and graph as regular arguments, but
  as `--schema-file` and `--data-tag` instead.
  
New features:

- SimpleGQL:
    - all main module arguments can be passed as environment variables,
    e.g. instead of `--schema-file` you can use `SIMPLEGQL_SCHEMA_FILE`.
    Commandline args take priority.
    - `--init-hook` option to run a hook on every startup
    - `@dynamic(hook: "...")` option for field resolvers


pyzef-0.15.6
============

- `schema` zefop
- Preliminary Windows support.