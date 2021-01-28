"""
Mergin WorkPackages - a tool for generation of the work packages

Copyright (C) 2021 Lutra Consulting

License: MIT
"""

import configparser
import getpass
import json
import os
import shutil
import subprocess
import sys
import tempfile

import psycopg2


class WorkPackagesError(Exception):
    pass


class Config:
    """ Contains configuration of the workpackages """
    def __init__(self):
        self.working_dir = None
        self.data_file = None
        self.master_table = None
        self.split_column = None

    def load(self, filename):
        cfg = configparser.ConfigParser()
        cfg.read(filename)

        self.working_dir = cfg['general']['working_dir']
        self.data_file = cfg['general']['data_file']
        self.master_table = cfg['general']['master_table']
        self.split_column = cfg['general']['split_column']

config = Config()

def _check_has_working_dir():
    if not os.path.exists(config.working_dir):
        raise WorkPackagesError("The project working directory does not exist: " + config.working_dir)


def _check_config():
    _check_has_working_dir()

def show_usage():
    print("workpackages")
    print("")
    print("    workpackages create = will split master table + create subdirectories in working directory")


def load_config(config_filename):
    if not os.path.exists(config_filename):
        raise WorkPackagesError("The configuration file does not exist: " + config_filename)
    config.load(config_filename)
    _check_config()


def create_workackages():
    pass

def main():
    if len(sys.argv) < 2:
        show_usage()
        return

    config_filename = 'config.ini'

    try:
        load_config(config_filename)

        if sys.argv[1] == 'create':
            print("Creating the workpackages")
            create_workackages()
        else:
            show_usage()
    except WorkPackagesError as e:
        print("Error: " + str(e))


if __name__ == '__main__':
    main()
