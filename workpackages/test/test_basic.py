import glob
import os
import shutil
from tempfile import TemporaryDirectory
import sqlite3

from .init_test_data import (
    create_farm_dataset,
    open_layer_and_create_feature,
    open_layer_and_update_feature,
    open_layer_and_delete_feature,
)
from ..wp import load_config_from_yaml, make_work_packages
from ..wp_utils import escape_double_quotes

this_dir = os.path.dirname(os.path.realpath(__file__))


def _assert_row_counts(gpkg_filename, expected_farms, expected_trees):
    """Raises assertion errors if tables do not have the right number of rows"""
    db = sqlite3.connect(gpkg_filename)
    c = db.cursor()
    c.execute("SELECT COUNT(*) FROM farms")
    assert c.fetchone()[0] == expected_farms
    c.execute("SELECT COUNT(*) FROM trees")
    assert c.fetchone()[0] == expected_trees


def _assert_value_equals(gpkg_filename, table_name, fid, field_name, expected_value):
    """Raises assertion error if value of a particular field of a given feature
    does not equal the expected value"""
    db = sqlite3.connect(gpkg_filename)
    c = db.cursor()
    field_name_escaped = escape_double_quotes(field_name)
    table_name_escaped = escape_double_quotes(table_name)
    c.execute(f"""SELECT {field_name_escaped} FROM {table_name_escaped} WHERE fid = ?""", (fid,))
    row = c.fetchone()
    if row is None:
        assert False, f"Missing row for fid {fid}"
    assert row[0] == expected_value


def _assert_row_missing(gpkg_filename, table_name, fid):
    """Raises assertion error if given feature is present in the table"""
    db = sqlite3.connect(gpkg_filename)
    c = db.cursor()
    table_name_escaped = escape_double_quotes(table_name)
    c.execute(f"""SELECT count(*) FROM {table_name_escaped} WHERE fid = ?""", (fid,))
    row = c.fetchone()
    assert row[0] == 0, f"Row for fid {fid} is present but it should not be"


def _assert_row_exists(gpkg_filename, table_name, fid):
    """Raises assertion error if given feature is NOT present in the table"""
    db = sqlite3.connect(gpkg_filename)
    c = db.cursor()
    table_name_escaped = escape_double_quotes(table_name)
    c.execute(f"""SELECT count(*) FROM {table_name_escaped} WHERE fid = ?""", (fid,))
    row = c.fetchone()
    assert row[0] == 1, f"Row for fid {fid} is not present but it should be"


def _make_initial_farm_work_packages(config_file):
    """
    1. create the initial "farms" dataset
    2. run the WP algorithm with the initial dataset and given config file
    Returns temporary directory object.
    """
    tmp_dir_obj = TemporaryDirectory(prefix="test-mergin-work-packages-")
    tmp_dir = tmp_dir_obj.name
    os.makedirs(os.path.join(tmp_dir, "input"))

    # get data
    create_farm_dataset(os.path.join(tmp_dir, "input", "master.gpkg"))
    # get config
    wp_config = load_config_from_yaml(config_file)
    # run alg
    make_work_packages(tmp_dir, wp_config)
    return tmp_dir_obj


def _prepare_next_run_work_packages(tmp_dir_1):
    """Creates a new temp directory with base+input files being output from the first step.
    After this, work packaging can be run using the new temp directory, which is returned.
    """
    tmp_dir_2 = TemporaryDirectory(prefix="test-mergin-work-packages-")
    os.makedirs(os.path.join(tmp_dir_2.name, "base"))
    os.makedirs(os.path.join(tmp_dir_2.name, "input"))

    shutil.copy(os.path.join(tmp_dir_1.name, "output", "remap.db"), os.path.join(tmp_dir_2.name, "base", "remap.db"))
    for file_path in glob.glob(os.path.join(tmp_dir_1.name, "output", "*.gpkg")):
        file_name = os.path.basename(file_path)
        shutil.copy(file_path, os.path.join(tmp_dir_2.name, "base", file_name))
        shutil.copy(file_path, os.path.join(tmp_dir_2.name, "input", file_name))

    return tmp_dir_2


def _keep_tmp_dir(tmp_dir, new_dir):
    """Makes a copy of a TemporaryDirectory. This is useful for debugging because
    TemporaryDirectory has no way to disable removal if needed"""
    if os.path.exists(new_dir):
        shutil.rmtree(new_dir)
    shutil.copytree(tmp_dir.name, new_dir)


def test_farm_data():
    """Check whether the test data init function returns what we expect"""
    tmp_dir_obj = TemporaryDirectory(prefix="test-mergin-work-packages-")
    farm_gpkg = os.path.join(tmp_dir_obj.name, "farms.gpkg")
    create_farm_dataset(farm_gpkg)

    _assert_row_counts(farm_gpkg, expected_farms=4, expected_trees=9)


def test_first_run():
    """Checks whether the first run correctly generates work package data with 'filter-column' method"""
    tmp_dir = _make_initial_farm_work_packages(os.path.join(this_dir, "config-farm-basic.yml"))

    # run checks
    output_dir = os.path.join(tmp_dir.name, "output")
    output_files = os.listdir(output_dir)
    assert "Emma.gpkg" in output_files
    assert "Kyle.gpkg" in output_files
    assert "master.gpkg" in output_files

    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=2)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)


def test_first_run_filtering_geom():
    """Checks whether the first run correctly generates work package data with 'filter-geometry' method"""
    tmp_dir = _make_initial_farm_work_packages(os.path.join(this_dir, "config-farm-geom.yml"))

    # run checks
    output_dir = os.path.join(tmp_dir.name, "output")
    output_files = os.listdir(output_dir)
    assert "Emma.gpkg" in output_files
    assert "Kyle.gpkg" in output_files
    assert "master.gpkg" in output_files

    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=2, expected_trees=3)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=3, expected_trees=1)


def test_update_row_wp():
    """One row has been updated in WP, no changes in master"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify a WP dataset - update a tree (master fid 8 mapped to 1000000 for Kyle)
    open_layer_and_update_feature(
        os.path.join(tmp_dir_2.name, "input", "Kyle.gpkg"), "trees", 1000000, {"age_years": 10}
    )

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be the same number of rows as initially
    # and updated age both in master + kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=2)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_value_equals(os.path.join(output_dir, "master.gpkg"), "trees", 8, "age_years", 10)
    _assert_value_equals(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000000, "age_years", 10)


def test_update_row_master():
    """One row has been updated in master, no changes in WP"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify master dataset - update a tree (master fid 9 mapped to 1000001 for Kyle)
    open_layer_and_update_feature(os.path.join(tmp_dir_2.name, "input", "master.gpkg"), "trees", 9, {"age_years": 20})

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be the same number of rows as initially
    # and updated age both in master + kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=2)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_value_equals(os.path.join(output_dir, "master.gpkg"), "trees", 9, "age_years", 20)
    _assert_value_equals(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000001, "age_years", 20)


def test_update_row_master_and_wp():
    """One row updated in master, another row in WP (no conflict)"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify a WP dataset - update a tree (master fid 8 mapped to 1000000 for Kyle)
    open_layer_and_update_feature(
        os.path.join(tmp_dir_2.name, "input", "Kyle.gpkg"), "trees", 1000000, {"age_years": 30}
    )
    # modify master dataset - update a tree (master fid 9 mapped to 1000001 for Kyle)
    open_layer_and_update_feature(os.path.join(tmp_dir_2.name, "input", "master.gpkg"), "trees", 9, {"age_years": 40})

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be the same number of rows as initially
    # and updated age both in master + kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=2)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_value_equals(os.path.join(output_dir, "master.gpkg"), "trees", 8, "age_years", 30)
    _assert_value_equals(os.path.join(output_dir, "master.gpkg"), "trees", 9, "age_years", 40)
    _assert_value_equals(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000000, "age_years", 30)
    _assert_value_equals(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000001, "age_years", 40)


def test_delete_row_wp():
    """One row deleted in WP, no changes in master"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify a WP dataset - delete a tree (master fid 8 mapped to 1000000 for Kyle)
    open_layer_and_delete_feature(os.path.join(tmp_dir_2.name, "input", "Kyle.gpkg"), "trees", 1000000)

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be one tree missing for master and for Kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=8)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=1)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_row_missing(os.path.join(output_dir, "master.gpkg"), "trees", 8)


def test_delete_row_master():
    """One row deleted in master, no changes in WP"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify a WP dataset - delete a tree (master fid 9 mapped to 1000001 for Kyle)
    open_layer_and_delete_feature(os.path.join(tmp_dir_2.name, "input", "master.gpkg"), "trees", 9)

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be one tree missing for master and for Kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=8)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=1)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_row_missing(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000001)


def test_insert_row_wp():
    """One row has been added in WP, no changes in master"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify a WP dataset - add a row
    open_layer_and_create_feature(
        os.path.join(tmp_dir_2.name, "input", "Kyle.gpkg"), "trees", "POINT(6 16)", {"tree_species_id": 1, "farm_id": 4}
    )

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be one new tree in master and one new tree for Kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=10)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=3)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_row_exists(os.path.join(output_dir, "master.gpkg"), "trees", 10)


def test_insert_row_master():
    """One row has inserted in master, no changes in WP"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify master dataset - add a row
    open_layer_and_create_feature(
        os.path.join(tmp_dir_2.name, "input", "master.gpkg"),
        "trees",
        "POINT(9 19)",
        {"tree_species_id": 1, "farm_id": 4},
    )

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be one new tree in master and one new tree for Kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=10)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=3)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_row_exists(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000002)


def test_insert_row_wp1_wp2():
    """One row inserted in WP1, one row in the same table inserted in WP2"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify WPs - add a row in two different WPs
    open_layer_and_create_feature(
        os.path.join(tmp_dir_2.name, "input", "Emma.gpkg"), "trees", "POINT(7 17)", {"tree_species_id": 2, "farm_id": 2}
    )
    open_layer_and_create_feature(
        os.path.join(tmp_dir_2.name, "input", "Kyle.gpkg"), "trees", "POINT(6 16)", {"tree_species_id": 1, "farm_id": 4}
    )

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be two new trees in master, one new tree for Kyle, one new tree for Emma
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=11)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=3)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=7)
    _assert_row_exists(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000002)
    _assert_row_exists(os.path.join(output_dir, "Emma.gpkg"), "trees", 1000006)


def test_insert_row_master_wp():
    """One row inserted in master, on row in the same table inserted in WP"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify master and WP
    open_layer_and_create_feature(
        os.path.join(tmp_dir_2.name, "input", "master.gpkg"),
        "trees",
        "POINT(9 19)",
        {"tree_species_id": 3, "farm_id": 4},
    )
    open_layer_and_create_feature(
        os.path.join(tmp_dir_2.name, "input", "Kyle.gpkg"), "trees", "POINT(6 16)", {"tree_species_id": 1, "farm_id": 4}
    )

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be two new trees in master, one new tree for Kyle, one new tree for Emma
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=11)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=4)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_row_exists(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000002)
    _assert_row_exists(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000003)


def test_delete_row_master_wp():
    """One row gets deleted in master, and the same row gets deleted in WP"""
    config_file = os.path.join(this_dir, "config-farm-basic.yml")
    tmp_dir_1 = _make_initial_farm_work_packages(config_file)
    tmp_dir_2 = _prepare_next_run_work_packages(tmp_dir_1)

    # modify master and WP dataset - delete the same tree in both (master fid 9 mapped to 1000001 for Kyle)
    open_layer_and_delete_feature(os.path.join(tmp_dir_2.name, "input", "master.gpkg"), "trees", 9)
    open_layer_and_delete_feature(os.path.join(tmp_dir_2.name, "input", "master.gpkg"), "trees", 1000001)

    # run work packaging
    wp_config = load_config_from_yaml(config_file)
    make_work_packages(tmp_dir_2.name, wp_config)
    output_dir = os.path.join(tmp_dir_2.name, "output")

    # there should be one tree missing for master and for Kyle
    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=8)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=1)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
    _assert_row_missing(os.path.join(output_dir, "master.gpkg"), "trees", 9)
    _assert_row_missing(os.path.join(output_dir, "Kyle.gpkg"), "trees", 1000001)


# TODO: more test cases
# - delete_master_update_wp  # one row deleted in master while it is updated in WP
# - update_master_delete_wp  # one row updated in master while it is deleted in WP
# - update_master_update_wp  # one row updated in master and the same row updated in WP


def test_not_using_fid_colum():
    """Special test case where data do not use `fid` column for primary key as usual in GPKG files,
    but rather custom named column `objectid`."""

    tmp_dir_obj = TemporaryDirectory(prefix="test-mergin-work-packages-")
    tmp_dir = tmp_dir_obj.name
    os.makedirs(os.path.join(tmp_dir, "input"))

    # get data
    shutil.copy(os.path.join(this_dir, "data", "farms_without_fid.gpkg"), os.path.join(tmp_dir, "input", "master.gpkg"))

    # get config
    wp_config = load_config_from_yaml(os.path.join(this_dir, "config-farm-basic.yml"))
    # switch from using `fid` (that does not exist) to `objectid`
    wp_config.wp_tables[0].filter_column_name = "objectid"

    # run alg
    make_work_packages(tmp_dir, wp_config)

    # run checks
    output_dir = os.path.join(tmp_dir, "output")
    output_files = os.listdir(output_dir)
    assert "Emma.gpkg" in output_files
    assert "Kyle.gpkg" in output_files
    assert "master.gpkg" in output_files

    _assert_row_counts(os.path.join(output_dir, "master.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(output_dir, "Kyle.gpkg"), expected_farms=1, expected_trees=2)
    _assert_row_counts(os.path.join(output_dir, "Emma.gpkg"), expected_farms=2, expected_trees=6)
