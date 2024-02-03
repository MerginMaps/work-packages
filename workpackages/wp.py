"""
Combined split/merge algorithm that:
1. Brings any changes from work packages to the master database
2. Regenerates work packages based on the master database
"""

import sqlite3
import os
import shutil
import pygeodiff
from pathlib import Path

import yaml

from .wp_utils import escape_double_quotes
from .remapping import remap_table_master_to_wp, remap_table_wp_to_master

# Layout of files:
#
# base/       -- geopackages as generated by previous run of this tool (must not be modified by user!)
#   master.gpkg    -- master DB containing all data
#   WP1.gpkg
#   WP2.gpkg
# input/      -- geopackages like in base/ subdir, but they may have been modified by users
#   master.gpkg
#   WP1.gpkg
#   WP2.gpkg
#
# output/   --  geopackages that have been merged (master.gpkg is the single source of truth) and then split again
#   master.gpkg
#   WP1.gpkg
#   WP2.gpkg
#   master-input-output.diff  -- difference between "input" and "output" (i.e. what are all the changes in WPs)
#   master-base-output.diff   -- difference between "base" and "output" (i.e. all master changes + all WP changes)
#   WP1-input-output.diff   -- difference between "input" and "output" (collated changes to be applied to the WP)


class WPTable(object):
    """Describes how to handle a table in work packages"""

    FILTER_METHOD_COLUMN = "filter-column"
    FILTER_METHOD_GEOMETRY = "filter-geometry"

    def __init__(self, name, filter_method, filter_column_name=None):
        """
        :param name: Name of the database table
        :param filter_column_name: Name of the column used for filtering
        """
        self.name = name
        self.filter_method = filter_method
        self.filter_column_name = filter_column_name


class WPName(object):
    """Describes configuration of a single work package"""

    def __init__(self, name, value, mergin_project):
        """
        :param name: Name of work package (user-defined), e.g. Team_A
        :param value: Accepted value (or values) for filtering
        :param mergin_project: Associated Mergin project (full name, e.g. lutraconsulting/survey-team-a)
        """
        self.name = name
        self.value = value
        self.mergin_project = mergin_project


class WPConfig(object):
    """Full configuration of the work packaging algorithm"""

    def __init__(self, master_gpkg, wp_names, wp_tables):
        """
        :param wp_names: List of WPName objects
        :param wp_tables: List of WPTable objects
        """
        self.master_gpkg = master_gpkg
        self.wp_names = wp_names
        self.wp_tables = wp_tables


def load_config_from_yaml(config_yaml: str) -> WPConfig:
    """
    Reads configuration of work packages from YAML config file.
    Returns WPConfig instance or raises an exception if there was a parsing error.
    """

    with open(config_yaml, "r") as stream:
        try:
            root_yaml = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError("Unable to parse config YAML:\n" + str(exc))
        master_gpkg = root_yaml["file"]
        wp_names, wp_tables = [], []
        for name_yaml in root_yaml["work-packages"]:
            wp_names.append(WPName(name_yaml["name"], name_yaml["value"], name_yaml["mergin-project"]))
        for table_yaml in root_yaml["tables"]:
            wp_tables.append(WPTable(table_yaml["name"], table_yaml["method"], table_yaml.get("filter-column-name")))

        return WPConfig(master_gpkg, wp_names, wp_tables)


def make_work_packages(data_dir: str, wp_config: WPConfig) -> None:
    """
    This is the core part of the algorithm for merging and splitting data for work packages.
    It expects a data directory with layout of directories and files as described in the header
    of this file.

    The first stage collects changes from the master DB and the work package DBs and
    combines them together, resolving any conflicts. At the end of the first stage we have
    updated master database. The second stage then re-creates individual work package DBs.
    """

    base_dir = os.path.join(data_dir, "base")  # where the non-modified GPKGs from the last run should be
    input_dir = os.path.join(data_dir, "input")  # where the existing GPKG for each existing WP should be
    output_dir = os.path.join(data_dir, "output")  # !!!! we are deleting this directory and recreating it every time!
    tmp_dir = os.path.join(data_dir, "tmp")  # for any temporary stuff (also deleted + recreated)

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir)

    old_wp_names = (
        []
    )  # names of WPs that have been processed before (and we expect their GPKGs exist and may be modified)
    if os.path.exists(base_dir):
        for path in Path(base_dir).iterdir():
            filename = path.name
            if filename == "master.gpkg":
                continue  # skip the master file - it's not a work package
            if filename.endswith(".gpkg"):
                wp_name = filename[:-5]  # strip the suffix
                old_wp_names.append(wp_name)
    print("existing WPs: " + str(old_wp_names))

    def _logger_callback(level, text_bytes):
        text = text_bytes.decode()  # convert bytes to str
        print("GEODIFF: ", text)

    geodiff = pygeodiff.GeoDiff()

    # set up logging to get extra info from geodiff.
    # geodiff.LevelDebug may be useful for debugging but it's too much info in most cases
    geodiff.set_maximum_logger_level(geodiff.LevelInfo)
    geodiff.set_logger_callback(_logger_callback)

    master_gpkg_base = os.path.join(base_dir, "master.gpkg")  # should not have been modified
    master_gpkg_input = os.path.join(input_dir, "master.gpkg")  # this could have been modified by users
    master_gpkg_output = os.path.join(output_dir, "master.gpkg")  # does not exist yet

    if os.path.exists(master_gpkg_base):
        # summarize changes that have happened in master (base master VS input master)
        # (this is not needed anywhere in the code, but may be useful for debugging)
        master_base_to_input = os.path.join(tmp_dir, "master-base-input.diff")
        master_base_to_input_json = os.path.join(tmp_dir, "master-base-input.json")
        geodiff.create_changeset(master_gpkg_base, master_gpkg_input, master_base_to_input)
        geodiff.list_changes(master_base_to_input, master_base_to_input_json)

    # create new master_gpkg in the output directory
    shutil.copy(master_gpkg_input, master_gpkg_output)

    # copy "base" remapping DB to "output" where we may be adding some more entries
    remap_db_base = os.path.join(base_dir, "remap.db")
    remap_db_output = os.path.join(output_dir, "remap.db")
    if old_wp_names and not os.path.exists(remap_db_base):
        raise ValueError("remap.db should exist!")
    if not old_wp_names and os.path.exists(remap_db_base):
        raise ValueError("remap.db should not exist yet!")
    if os.path.exists(remap_db_base):
        shutil.copy(remap_db_base, remap_db_output)

    # STAGE 1: Bring the changes from WPs to master
    # (remap WP database + create changeset + rebase changeset)
    for wp_name in old_wp_names:
        print("WP " + wp_name)

        # get max. fids for tables (so that we know where to start when remapping)
        db = sqlite3.connect(master_gpkg_output)
        c = db.cursor()
        new_master_fids = {}
        for wp_table in wp_config.wp_tables:
            wp_table_name = wp_table.name
            wp_tab_name_esc = escape_double_quotes(wp_table_name)
            c.execute(f"""SELECT max(fid) FROM {wp_tab_name_esc};""")
            new_master_fid = c.fetchone()[0]
            if new_master_fid is None:
                new_master_fid = 1  # empty table so far
            else:
                new_master_fid += 1
            new_master_fids[wp_table_name] = new_master_fid
        c = None
        db = None

        # TODO: check whether the changes in the DB are allowed (matching the deciding column)

        wp_gpkg_base_wp_fids = os.path.join(base_dir, wp_name + ".gpkg")  # should not have been modified by user
        wp_gpkg_input_wp_fids = os.path.join(input_dir, wp_name + ".gpkg")  # may have been modified by user

        wp_gpkg_base = os.path.join(tmp_dir, wp_name + "-base.gpkg")  # should not have been modified by user
        wp_gpkg_input = os.path.join(tmp_dir, wp_name + "-input.gpkg")  # may have been modified by user
        shutil.copy(wp_gpkg_base_wp_fids, wp_gpkg_base)
        shutil.copy(wp_gpkg_input_wp_fids, wp_gpkg_input)

        # re-map local fids of the WP gpkg to master fids (based on previously created mapping DB)
        for x in [wp_gpkg_base, wp_gpkg_input]:
            db = sqlite3.connect(x)
            db.enable_load_extension(True)  # for spatialite
            c = db.cursor()
            c.execute("SELECT load_extension('mod_spatialite');")  # TODO: how to deal with it?
            c.execute("ATTACH ? AS remap", (remap_db_output,))
            c.execute("BEGIN")
            for wp_table in wp_config.wp_tables:
                remap_table_wp_to_master(c, wp_table.name, wp_name, new_master_fids[wp_table.name])
            c.execute("COMMIT")
            db.close()

        wp_changeset_base_input = os.path.join(tmp_dir, wp_name + "-base-input.diff")
        wp_changeset_base_input_json = os.path.join(tmp_dir, wp_name + "-base-input.json")

        wp_changeset_base_output = os.path.join(tmp_dir, wp_name + "-base-output.diff")
        wp_changeset_base_output_json = os.path.join(tmp_dir, wp_name + "-base-output.json")

        wp_rebased_changeset = os.path.join(tmp_dir, wp_name + "-rebased.diff")
        wp_rebased_changeset_json = os.path.join(tmp_dir, wp_name + "-rebased.json")

        wp_rebased_changeset_conflicts = os.path.join(tmp_dir, wp_name + "-rebased-conflicts.json")

        # create changeset using pygeodiff using wp_gpkg_base + wp_gpkg_input
        # print("--- create changeset")
        geodiff.create_changeset(wp_gpkg_base, wp_gpkg_input, wp_changeset_base_input)
        geodiff.create_changeset(wp_gpkg_base, master_gpkg_output, wp_changeset_base_output)
        # summarize changes that have happened in master (base master VS input master)
        # (this is not needed anywhere in the code, but may be useful for debugging)
        geodiff.list_changes(wp_changeset_base_input, wp_changeset_base_input_json)
        geodiff.list_changes(wp_changeset_base_output, wp_changeset_base_output_json)
        # Create rebased changeset
        # rebase changeset - to resolve conflicts, for example:
        # - WP1 deleted a row that WP2 also wants to delete
        # - WP1 updated a row that WP2 also updated
        # - WP1 updated a row that WP2 deleted
        # - WP1 deleted a row that WP2 updated
        # - WP1 inserted a row with FID that WP2 also wants to insert -- this should not happen
        #   because remapping should assign unique master FIDs
        geodiff.create_rebased_changeset_ex(
            "sqlite",
            "",
            master_gpkg_base,
            wp_changeset_base_input,
            wp_changeset_base_output,
            wp_rebased_changeset,
            wp_rebased_changeset_conflicts,
        )
        if os.path.isfile(wp_rebased_changeset):
            geodiff.list_changes(wp_rebased_changeset, wp_rebased_changeset_json)
        else:
            continue
        geodiff.apply_changeset(master_gpkg_output, wp_rebased_changeset)

    # summarize changes that have happened in WPs (input master VS output master)
    # (this is not needed anywhere in the code, but may be useful for debugging)
    master_input_to_output = os.path.join(output_dir, "master-input-output.diff")
    master_input_to_output_json = os.path.join(output_dir, "master-input-output.json")
    geodiff.create_changeset(master_gpkg_input, master_gpkg_output, master_input_to_output)
    geodiff.list_changes(master_input_to_output, master_input_to_output_json)

    if os.path.exists(master_gpkg_base):
        # summarize all the changes that have happened since last run (collated master changes + wp changes)
        # (this is not needed anywhere in the code, but may be useful for debugging)
        master_base_to_output = os.path.join(output_dir, "master-base-output.diff")
        master_base_to_output_json = os.path.join(output_dir, "master-base-output.json")
        geodiff.create_changeset(master_gpkg_base, master_gpkg_output, master_base_to_output)
        geodiff.list_changes(master_base_to_output, master_base_to_output_json)

    # STAGE 2: Regenerate WP databases
    # (make "new" WP database + filter database based on WP + remap DB)

    for wp in wp_config.wp_names:
        wp_name, wp_value, wp_mergin_project = wp.name, wp.value, wp.mergin_project
        wp_gpkg_base = os.path.join(base_dir, wp_name + ".gpkg")  # should not have been modified by user
        wp_gpkg_input = os.path.join(input_dir, wp_name + ".gpkg")  # may have been modified by user
        wp_gpkg_output = os.path.join(output_dir, wp_name + ".gpkg")  # does not exist yet
        wp_changeset_input_to_output = os.path.join(output_dir, wp_name + "-input-output.diff")
        wp_changeset_input_to_output_json = os.path.join(output_dir, wp_name + "-input-output.json")

        # start from a copy of the master
        shutil.copy(master_gpkg_output, wp_gpkg_output)

        # filter out data that does not belong to the WP
        # and remap fids in the DB from master to WP-local fids
        db = sqlite3.connect(wp_gpkg_output)
        db.enable_load_extension(True)  # for spatialite
        c = db.cursor()
        c.execute("SELECT load_extension('mod_spatialite');")  # TODO: how to deal with it?
        c.execute("ATTACH ? AS remap", (remap_db_output,))
        c.execute("BEGIN")
        for wp_table in wp_config.wp_tables:
            wp_table_name = wp_table.name
            wp_filter_method = wp_table.filter_method
            wp_tab_name_esc = escape_double_quotes(wp_table_name)
            if wp_filter_method == WPTable.FILTER_METHOD_GEOMETRY:
                intersects_query = f"ST_Intersects(GeomFromGPB(geometry), ST_GeomFromText('{wp_value}'))"
                c.execute(f"""delete from {wp_tab_name_esc} where not {intersects_query}""")
            else:
                wp_filter_column = wp_table.filter_column_name
                wp_filter_column_escaped = escape_double_quotes(wp_filter_column)
                c.execute(f"""delete from {wp_tab_name_esc} where {wp_filter_column_escaped} IS NULL""")
                if isinstance(wp_value, (str, int, float)):
                    c.execute(f"""delete from {wp_tab_name_esc} where {wp_filter_column_escaped} != ?""", (wp_value,))
                elif isinstance(wp_value, list):
                    values_str = ",".join(["?"] * len(wp_value))
                    c.execute(
                        f"""delete from {wp_tab_name_esc} where {wp_filter_column_escaped} not in ({values_str})""",
                        wp_value,
                    )
                else:
                    # we may want to support some custom SQL at some point too
                    raise ValueError("what?")
            remap_table_master_to_wp(c, wp_table.name, wp_name)
        # TODO: drop tables that are not listed at all (?)
        c.execute("COMMIT")

        # run VACUUM to purge anything that does not belong to the WP data
        c.execute("VACUUM")

        # explicitly close the connection to avoid possible
        # "recovered N frames from WAL file" warnings from geodiff (due to two different sqlite3 libs)
        db.close()

        # get changeset between the one received from WP and newly created GPKG
        if os.path.exists(wp_gpkg_input):
            geodiff.create_changeset(wp_gpkg_input, wp_gpkg_output, wp_changeset_input_to_output)
            geodiff.list_changes(wp_changeset_input_to_output, wp_changeset_input_to_output_json)
        else:
            # first time this WP is created...
            pass  # TODO: what to do?
