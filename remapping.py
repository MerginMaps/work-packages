
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
    return "remap.{}_{}".format(table_name, wp_name)


def _create_remap_table_if_not_exists(cursor, remap_table):
    """ Creates mapping table with the expected structure if we don't have it yet """
    create_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            master_fid INTEGER PRIMARY KEY, wp_fid INTEGER UNIQUE);
    """.format(remap_table)
    cursor.execute(create_sql)


def remap_table_master_to_wp(cursor, table_name, wp_name):
    """
    Update primary key values from "master" fids to "WP" fids.

    For each row:
    - remap row exists for master_fid -> use wp_fid
    - remap does not exist for master_fid -> insert (master_fid, 1000000+master_fid)
    """
    print("------ remap master->wp ------")

    # TODO: proper escaping

    remap_table = remap_table_name(table_name, wp_name)
    _create_remap_table_if_not_exists(cursor, remap_table)

    # TODO 0. get table's pkey column name

    # 1. find missing mapped ids
    master_fids_missing = set()
    sql = "SELECT fid FROM {} LEFT JOIN {} AS mapped ON fid = mapped.master_fid WHERE mapped.wp_fid IS NULL".format(table_name, remap_table)
    print(sql)
    for row in cursor.execute(sql):
        master_fids_missing.add(row[0])

    # 2. insert missing mapped ids
    cursor.execute("SELECT max(wp_fid) FROM {}".format(remap_table))
    new_wp_fid = cursor.fetchone()[0]
    if new_wp_fid is None:
        new_wp_fid = 1000000   # empty table so far
    else:
        new_wp_fid += 1

    # TODO: prepare sql query
    print(master_fids_missing)
    for master_fid in master_fids_missing:
        cursor.execute("INSERT INTO {} VALUES ({}, {})".format(remap_table, master_fid, new_wp_fid))
        new_wp_fid += 1

    # 3. remap master ids to WP ids
    mapping = []
    sql = "SELECT fid, mapped.wp_fid FROM {} LEFT JOIN {} AS mapped ON fid = mapped.master_fid".format(table_name, remap_table)
    print(sql)
    for row in cursor.execute(sql):
        mapping.append((row[0], row[1]))
    print(mapping)

    # hack to hopefully avoid possible pkey violations ... who would use negative ids? :-)
    cursor.execute("UPDATE {} SET fid = -fid".format(table_name))

    # TODO: prepare sql query
    for master_fid, wp_fid in mapping:
        cursor.execute("UPDATE {} SET fid = {} WHERE fid = -{}".format(table_name, wp_fid, master_fid))


def remap_table_wp_to_master(cursor, table_name, wp_name, new_master_fid):
    """
    Update primary key values from "WP" fids to "master" fids.

    For each row:
    - remap row exists for wp_fid -> use master_fid
    - remap does not exist for wp_fid -> insert ([first unused master fid], wp_fid)
    """
    print("------ remap wp->master ------")

    remap_table = remap_table_name(table_name, wp_name)
    _create_remap_table_if_not_exists(cursor, remap_table)

    # TODO 0. get table's pkey column name

    # 1. find missing mapped ids
    wp_fids_missing = set()
    sql = "SELECT fid FROM {} LEFT JOIN {} AS mapped ON fid = mapped.wp_fid WHERE mapped.master_fid IS NULL".format(table_name, remap_table)
    print(sql)
    for row in cursor.execute(sql):
        wp_fids_missing.add(row[0])

    # 2. insert missing mapped ids
    print(wp_fids_missing)
    for wp_fid in wp_fids_missing:
        print("INSERT INTO {} VALUES ({}, {})".format(remap_table, new_master_fid, wp_fid))
        cursor.execute("INSERT INTO {} VALUES ({}, {})".format(remap_table, new_master_fid, wp_fid))
        new_master_fid += 1

    # 3. remap WP ids to master ids
    mapping = []  # list of tuples (wp_fid, master_fid)
    sql = "SELECT fid, mapped.master_fid FROM {} LEFT JOIN {} AS mapped ON fid = mapped.wp_fid".format(table_name, remap_table)
    print(sql)
    for row in cursor.execute(sql):
        mapping.append((row[0], row[1]))
    print(mapping)

    # hack to hopefully avoid possible pkey violations ... who would use negative ids? :-)
    cursor.execute("UPDATE {} SET fid = -fid".format(table_name))

    # TODO: prepare sql query
    for wp_fid, master_fid in mapping:
        cursor.execute("UPDATE {} SET fid = {} WHERE fid = -{}".format(table_name, master_fid, wp_fid))
