
import pytest
import os
import shutil
import sqlite3
import tempfile

import psycopg2
from workpackages import create_workackages, config

TMP_DIR = tempfile.gettempdir()
TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')


def prepare(working_dir, data_dir):
    """ cleanup leftovers from previous test if needed such as local directories """
    if os.path.exists(working_dir):
        shutil.rmtree(working_dir)
    shutil.copytree(data_dir, working_dir)


def test_create_folders():
    """
        Splits the stuff to workpackages
    """

    config.working_dir = TMP_DIR + "/create_folders"
    config.data_file = "data.gpkg"
    config.master_table = "MeasurementAreas"
    config.split_column = "Teams"

    prepare(config.working_dir, TEST_DATA_DIR)

    create_workackages()
