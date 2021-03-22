"""
Module for remapping primary key values between master table and "work package" table
which only contains a subset of data. The "work package" table may have completely
different numbering of rows (features), because different work packages may end up
using conflicting feature IDs. So for each table and for each work package we keep
an auxiliary table that records pairs of IDs for features: master ID + work package ID.
Any newly seen feature IDs in master table are assigned corresponding feature IDs
in the work package table (and vice versa).
"""


def remap_table_name(table_name, wp_name):
    """ Returns name of the mapping table used for a particular table name and work package """
    return f'"remap"."{table_name}_{wp_name}"'


def _create_remap_table_if_not_exists(cursor, remap_table):
    """ Creates mapping table with the expected structure if we don't have it yet """
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {remap_table} (
            master_fid INTEGER PRIMARY KEY, wp_fid INTEGER UNIQUE);
    """
    cursor.execute(create_sql)


def _table_pkey(cursor, table_name):
    """
    Finds out what is the name of the primary key's column.
    If the table has multi-column primary key, it raises an exception
    as this is currently not supported.
    """
    pkey_column_name = None
    for row in cursor.execute(f"pragma table_info('{table_name}')"):
        if row[5]:
            if pkey_column_name is None:
                pkey_column_name = row[1]
            else:
                raise ValueError(f"table {table_name} has multi-column primary key")
    return pkey_column_name


def remap_table_master_to_wp(cursor, table_name, wp_name):
    """
    Update primary key values from "master" fids to "WP" fids.

    For each row:
    - remap row exists for master_fid -> use wp_fid
    - remap does not exist for master_fid -> insert (master_fid, 1000000+master_fid)
    """
    remap_table = remap_table_name(table_name, wp_name)
    _create_remap_table_if_not_exists(cursor, remap_table)

    pkey_column = _table_pkey(cursor, table_name)

    # 1. find missing mapped ids
    master_fids_missing = set()
    sql = (
        f"""SELECT "{pkey_column}" FROM "{table_name}" """
        f"""LEFT JOIN {remap_table} AS mapped ON fid = mapped.master_fid WHERE mapped.wp_fid IS NULL"""
    )
    for row in cursor.execute(sql):
        master_fids_missing.add(row[0])

    # 2. insert missing mapped ids
    cursor.execute(f"""SELECT max(wp_fid) FROM {remap_table}""")
    new_wp_fid = cursor.fetchone()[0]
    if new_wp_fid is None:
        new_wp_fid = 1000000  # empty table so far
    else:
        new_wp_fid += 1

    for master_fid in master_fids_missing:
        cursor.execute(f"""INSERT INTO {remap_table} VALUES (?, ?)""", (master_fid, new_wp_fid))
        new_wp_fid += 1

    # 3. remap master ids to WP ids
    mapping = []
    sql = (
        f"""SELECT "{pkey_column}", mapped.wp_fid FROM "{table_name}" """
        f"""LEFT JOIN {remap_table} AS mapped ON fid = mapped.master_fid"""
    )
    for row in cursor.execute(sql):
        mapping.append((row[0], row[1]))

    # hack to hopefully avoid possible pkey violations ... who would use negative ids? :-)
    cursor.execute(f"""UPDATE "{table_name}" SET "{pkey_column}" = -"{pkey_column}";""")

    for master_fid, wp_fid in mapping:
        cursor.execute(
            f"""UPDATE "{table_name}" SET "{pkey_column}" = ? WHERE "{pkey_column}" = ?""", (wp_fid, -master_fid)
        )


def remap_table_wp_to_master(cursor, table_name, wp_name, new_master_fid):
    """
    Update primary key values from "WP" fids to "master" fids.

    For each row:
    - remap row exists for wp_fid -> use master_fid
    - remap does not exist for wp_fid -> insert ([first unused master fid], wp_fid)
    """

    remap_table = remap_table_name(table_name, wp_name)
    _create_remap_table_if_not_exists(cursor, remap_table)

    pkey_column = _table_pkey(cursor, table_name)

    # 1. find missing mapped ids
    wp_fids_missing = set()
    sql = (
        f"""SELECT "{pkey_column}" FROM "{table_name}" """
        f"""LEFT JOIN {remap_table} AS mapped ON fid = mapped.wp_fid WHERE mapped.master_fid IS NULL"""
    )
    for row in cursor.execute(sql):
        wp_fids_missing.add(row[0])

    # 2. insert missing mapped ids
    for wp_fid in wp_fids_missing:
        cursor.execute(f"""INSERT INTO {remap_table} VALUES (?, ?)""", (new_master_fid, wp_fid))
        new_master_fid += 1

    # 3. remap WP ids to master ids
    mapping = []  # list of tuples (wp_fid, master_fid)
    sql = (
        f"""SELECT "{pkey_column}", mapped.master_fid FROM "{table_name}" """
        f"""LEFT JOIN {remap_table} AS mapped ON fid = mapped.wp_fid"""
    )
    for row in cursor.execute(sql):
        mapping.append((row[0], row[1]))

    # hack to hopefully avoid possible pkey violations ... who would use negative ids? :-)
    cursor.execute(f"""UPDATE "{table_name}" SET "{pkey_column}" = -"{pkey_column}";""")

    for wp_fid, master_fid in mapping:
        cursor.execute(f"""UPDATE "{table_name}" SET "{pkey_column}" = ? WHERE fid = ?""", (master_fid, -wp_fid))
