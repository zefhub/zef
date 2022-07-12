pyzef-0.15.8
============

Fixes:
------

- SimpleGQL graphs are kept alive by default.

Improvements:
-------------

- SimpleGQL:
  - Faster queries/updates with explicit IDs.
  - Queries work in consistent graph slice.

pyzef-0.15.7
============

Breaking changes:

- SimpleGQL: no longer takes the schema_file and graph as regular arguments, but
  as `--schema-file` and `--data-tag` instead.
- Rename `ZEFDB_CONFIG_PATH` to `ZEFDB_SESSION_PATH`
- `schema` zefop renamed to `blueprint`
  
New features:

- SimpleGQL:
    - all main module arguments can be passed as environment variables,
    e.g. instead of `--schema-file` you can use `SIMPLEGQL_SCHEMA_FILE`.
    Commandline args take priority.
    - `--init-hook` option to run a hook on every startup
    - `@dynamic(hook: "...")` option for field resolvers
    
- Tokens are cached in the session directory

- Dictionary syntax for graph wishes: `{ET.Something: {RT.A: 3, RT.B: 4}}`

- `set_field` operation for atomically updating "fields" 

Fixes:

- `login` auth flow was broken for the new default of statically built libzef.

pyzef-0.15.6
============

- `schema` zefop
- Preliminary Windows support.