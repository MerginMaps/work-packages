
-- describes what work packages to create and how are they determined
CREATE TABLE wp_names (
    name TEXT,
    value TEXT,
    mergin_project TEXT
);

-- describes which tables will get filtered
CREATE TABLE wp_tables (
  table_name TEXT,
  filter_column_name TEXT
);
