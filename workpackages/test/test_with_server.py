import pytest
import os
import shutil
import sqlite3
import tempfile

from mergin import MerginClient, ClientError, MerginProject

from ..wp_mergin import run_wp_mergin_with_context, MerginWPContext
from ..wp_utils import escape_double_quotes
from .init_test_data import create_farm_dataset, open_layer_and_update_feature

SERVER_URL = os.environ.get("TEST_MERGIN_URL")
API_USER = os.environ.get("TEST_API_USERNAME")
USER_PWD = os.environ.get("TEST_API_PASSWORD")
WORKSPACE = os.environ.get("TEST_WORKSPACE")

TMP_DIR = tempfile.gettempdir()

this_dir = os.path.dirname(os.path.realpath(__file__))


def create_client(user, pwd):
    assert SERVER_URL and SERVER_URL.rstrip("/") != "https://app.merginmaps.com" and user and pwd
    return MerginClient(SERVER_URL, login=user, password=pwd)


@pytest.fixture(scope="function")
def mc():
    return create_client(API_USER, USER_PWD)


def cleanup(mc: MerginClient, project: str, dirs: list):
    # cleanup leftovers from previous test if needed such as remote project and local directories
    try:
        mc.delete_project_now(project)
    except ClientError:
        pass
    remove_folders(dirs)


def remove_folders(dirs):
    # clean given directories
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def project_version(project_dir):
    return MerginProject(project_dir).version()


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


def test_wp_1(mc: MerginClient):
    """Do a basic test of creating WP projects from master and doing simple updates,
    to make sure that projects on the server get updated as expected."""

    project_master_name = "farms-master"
    project_kyle_name = "farms-Kyle"
    project_emma_name = "farms-Emma"
    project_master_full = WORKSPACE + "/" + project_master_name
    project_kyle_full = WORKSPACE + "/" + project_kyle_name
    project_emma_full = WORKSPACE + "/" + project_emma_name
    project_dir_master = os.path.join(TMP_DIR, "wp-edits", project_master_name)
    project_dir_kyle = os.path.join(TMP_DIR, "wp-edits", project_kyle_name)
    project_dir_emma = os.path.join(TMP_DIR, "wp-edits", project_emma_name)
    cache_dir = os.path.join(TMP_DIR, "wp-cache")

    remove_folders([cache_dir])
    cleanup(mc, project_master_full, [project_dir_master])
    cleanup(mc, project_kyle_full, [project_dir_kyle])
    cleanup(mc, project_emma_full, [project_dir_emma])

    # add WP configuration and initial farms dataset
    os.makedirs(project_dir_master, exist_ok=True)
    config_yaml = os.path.join(project_dir_master, "mergin-work-packages.yml")
    shutil.copy(os.path.join(this_dir, "config-farm-basic.yml"), config_yaml)

    with open(config_yaml, "r") as file:
        filedata = file.read()
        filedata = filedata.replace("martin/", WORKSPACE + "/")
    with open(config_yaml, "w") as file:
        file.write(filedata)

    create_farm_dataset(os.path.join(project_dir_master, "farms.gpkg"))
    _assert_row_counts(os.path.join(project_dir_master, "farms.gpkg"), expected_farms=4, expected_trees=9)

    mc.create_project_and_push(project_master_full, project_dir_master)

    ctx = MerginWPContext()
    ctx.mergin_url = SERVER_URL
    ctx.mergin_user = API_USER
    ctx.mergin_password = USER_PWD
    ctx.master_mergin_project = project_master_full
    ctx.cache_dir = cache_dir

    #
    # initial creation of the work package projects
    #

    run_wp_mergin_with_context(ctx)

    # check that the projects exist and contain the data
    # (we intentionally do not reuse cache directory)

    mc.pull_project(project_dir_master)
    mc.download_project(project_kyle_full, project_dir_kyle)
    mc.download_project(project_emma_full, project_dir_emma)

    assert project_version(project_dir_master) == "v2"
    assert project_version(project_dir_kyle) == "v1"
    assert project_version(project_dir_emma) == "v1"

    _assert_row_counts(os.path.join(project_dir_master, "farms.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(project_dir_kyle, "farms.gpkg"), expected_farms=1, expected_trees=2)
    _assert_row_counts(os.path.join(project_dir_emma, "farms.gpkg"), expected_farms=2, expected_trees=6)

    #
    # run again after no operation - nothing should change
    #

    run_wp_mergin_with_context(ctx)

    mc.pull_project(project_dir_master)
    mc.pull_project(project_dir_kyle)
    mc.pull_project(project_dir_emma)

    assert project_version(project_dir_master) == "v2"
    assert project_version(project_dir_kyle) == "v1"
    assert project_version(project_dir_emma) == "v1"

    _assert_row_counts(os.path.join(project_dir_master, "farms.gpkg"), expected_farms=4, expected_trees=9)
    _assert_row_counts(os.path.join(project_dir_kyle, "farms.gpkg"), expected_farms=1, expected_trees=2)
    _assert_row_counts(os.path.join(project_dir_emma, "farms.gpkg"), expected_farms=2, expected_trees=6)

    #
    # do a change in a WP project
    #

    open_layer_and_update_feature(os.path.join(project_dir_kyle, "farms.gpkg"), "trees", 1000000, {"age_years": 10})
    mc.push_project(project_dir_kyle)

    run_wp_mergin_with_context(ctx)

    mc.pull_project(project_dir_master)
    mc.pull_project(project_dir_kyle)
    mc.pull_project(project_dir_emma)

    assert project_version(project_dir_master) == "v3"
    assert project_version(project_dir_kyle) == "v2"
    assert project_version(project_dir_emma) == "v1"

    _assert_value_equals(os.path.join(project_dir_master, "farms.gpkg"), "trees", 8, "age_years", 10)
    _assert_value_equals(os.path.join(project_dir_kyle, "farms.gpkg"), "trees", 1000000, "age_years", 10)

    #
    # do a change in the master project
    #

    open_layer_and_update_feature(os.path.join(project_dir_master, "farms.gpkg"), "trees", 3, {"age_years": 5})
    mc.push_project(project_dir_master)
    assert project_version(project_dir_master) == "v4"

    run_wp_mergin_with_context(ctx)

    mc.pull_project(project_dir_master)
    mc.pull_project(project_dir_kyle)
    mc.pull_project(project_dir_emma)

    assert project_version(project_dir_master) == "v5"
    assert project_version(project_dir_kyle) == "v2"
    assert project_version(project_dir_emma) == "v2"

    _assert_value_equals(os.path.join(project_dir_master, "farms.gpkg"), "trees", 3, "age_years", 5)
    _assert_value_equals(os.path.join(project_dir_emma, "farms.gpkg"), "trees", 1000002, "age_years", 5)
