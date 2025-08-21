"""
Integration of work package algorithm into Mergin projects

For initial run we need a Mergin project with at least these files:
- data.gpkg                -- master database
- work-packages/config.db  -- how to split the database + what work packages to create

The config.db should contain two tables as defined in config.sql:
- wp_names - defines names of work packages and their corresponding Mergin projects
- wp_tables - defines which tables of the master GPKG should be filtered in work packages

After the initial run, the algorithm will add some more files:
- work-packages/remap.db
- work-packages/master.gpkg
- work-packages/<WP1>.gpkg
- work-packages/<WP2>.gpkg
- work-package/<...>.gpkg
These files are used internally by the algorithm and should not be modified (or deleted).

"""

import getpass
import glob
import json
import time
import mergin
import mergin.client_push
import os
import shutil
import tempfile
import argparse
from concurrent.futures import ThreadPoolExecutor
from .version import __version__
from .wp_utils import download_project_with_cache, ProjectPadlock
from .wp import load_config_from_yaml, make_work_packages, WPConfig
from .wp_replay import Replay


class MerginWPContext:
    """Keeps the context of the current run of the tool"""

    def __init__(self):
        self.max_workers = None
        self.dry_run = None
        self.skip_lock = None
        self.mc = None

        self.mergin_url = None
        self.mergin_user = None
        self.mergin_password = None

        self.master_mergin_project = None

        self.tmp_dir = None
        self.cache_dir = None
        self.master_dir = None
        self.master_config_yaml = None
        self.wp_alg_dir = None
        self.wp_alg_base_dir = None
        self.wp_alg_input_dir = None
        self.wp_alg_output_dir = None
        self.project_padlock = None


def parse_args() -> MerginWPContext:
    """Create context object from parsed command line arguments"""
    ctx = MerginWPContext()

    parser = argparse.ArgumentParser()
    parser.add_argument("mergin_project")
    parser.add_argument("--cache-dir", nargs="?")
    parser.add_argument("--max-workers", nargs="?", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-lock", action="store_true")
    params = parser.parse_args()
    ctx.master_mergin_project = params.mergin_project  # e.g.  martin/wp-master
    ctx.cache_dir = params.cache_dir
    ctx.max_workers = params.max_workers
    ctx.dry_run = params.dry_run
    ctx.skip_lock = params.skip_lock
    return ctx


def initialize(ctx: MerginWPContext):
    """Parse command line attributes + env vars and prepare context object"""

    if not ctx.master_mergin_project:
        raise ValueError("Need a parameter with master Mergin Maps project name")

    if ctx.mergin_user is None:
        ctx.mergin_user = os.getenv("MERGIN_USERNAME")
    if ctx.mergin_user is None:
        ctx.mergin_user = input("Mergin Maps username: ")

    if ctx.mergin_password is None:
        ctx.mergin_password = os.getenv("MERGIN_PASSWORD")
    if ctx.mergin_password is None:
        ctx.mergin_password = getpass.getpass(f"Password for {ctx.mergin_user}: ")

    if ctx.mergin_url is None:
        ctx.mergin_url = os.getenv("MERGIN_URL")
    if ctx.mergin_url is None:
        ctx.mergin_url = mergin.MerginClient.default_url()

    # this will create a directory with a random name, e.g. /tmp/mergin-work-packages-w7tbsyd7
    ctx.tmp_dir = tempfile.mkdtemp(prefix="mergin-work-packages-")

    ctx.mc = mergin.MerginClient(
        url=ctx.mergin_url,
        login=ctx.mergin_user,
        password=ctx.mergin_password,
        plugin_version=f"work-packages/{__version__}",
    )

    ctx.wp_alg_dir = os.path.join(ctx.tmp_dir, "wp")  # where we expect "base", "input" subdirs
    ctx.wp_alg_base_dir = os.path.join(ctx.wp_alg_dir, "base")
    ctx.wp_alg_input_dir = os.path.join(ctx.wp_alg_dir, "input")
    ctx.wp_alg_output_dir = os.path.join(ctx.wp_alg_dir, "output")
    os.makedirs(ctx.wp_alg_base_dir)
    os.makedirs(ctx.wp_alg_input_dir)

    ctx.master_dir = os.path.join(ctx.tmp_dir, "master")
    ctx.master_config_yaml = os.path.join(ctx.master_dir, "mergin-work-packages.yml")
    ctx.project_padlock = ProjectPadlock(ctx.mc)
    return ctx


def get_master_project_files(directory):
    """Returns list of relative file names from the master project that should be copied to the new WP projects"""
    mergin_internal_dir = os.path.join(directory, ".mergin")
    config_file = os.path.join(directory, "mergin-work-packages.yml")
    wp_dir = os.path.join(directory, "work-packages")
    files = []
    for filename in glob.iglob(os.path.join(directory, "**"), recursive=True):
        if filename.startswith(mergin_internal_dir) or filename.startswith(wp_dir) or filename == config_file:
            continue
        if not os.path.isfile(filename):
            continue
        filename_relative = filename[len(directory) + 1 :]  # remove prefix
        if len(filename_relative):
            files.append(filename_relative)
    return files


def prepare_inputs(ctx: MerginWPContext) -> (WPConfig, set, str, list):
    """
    Prepare directory with inputs:
    - fetch master mergin project, read configuration in config.db, copy base files and master input file
    - fetch WP projects and copy their input files
    """

    if ctx.cache_dir is None:
        print("No cache directory set: work packaging may be slow, it is recommended to use cache directory")

    print("Downloading master project " + ctx.master_mergin_project + "...")
    download_project_with_cache(ctx.mc, ctx.master_mergin_project, ctx.master_dir, ctx.cache_dir)
    print("Done.")

    print("Reading configuration from " + ctx.master_config_yaml)
    wp_config = load_config_from_yaml(ctx.master_config_yaml)

    # Handling removed work packages
    wp_names = {f"{wp.name}.gpkg" for wp in wp_config.wp_names}
    master_wp_dir = os.path.join(ctx.master_dir, "work-packages")
    if os.path.exists(master_wp_dir):
        for f in os.listdir(master_wp_dir):
            if f.endswith(".gpkg") and f != "master.gpkg" and f not in wp_names:
                missing_wp_name = f[:-5]  # strip the suffix
                print(f"Removing '{missing_wp_name}' work package as it's not used anymore.")
                os.remove(os.path.join(master_wp_dir, f))

    gpkg_path = wp_config.master_gpkg

    shutil.copy(os.path.join(ctx.master_dir, gpkg_path), os.path.join(ctx.wp_alg_input_dir, "master.gpkg"))

    # the master.gpkg and remap.db should exist if this is not the first run of the tool
    if os.path.exists(os.path.join(ctx.master_dir, "work-packages", "master.gpkg")):
        shutil.copy(
            os.path.join(ctx.master_dir, "work-packages", "master.gpkg"),
            os.path.join(ctx.wp_alg_base_dir, "master.gpkg"),
        )
    if os.path.exists(os.path.join(ctx.master_dir, "work-packages", "remap.db")):
        shutil.copy(
            os.path.join(ctx.master_dir, "work-packages", "remap.db"), os.path.join(ctx.wp_alg_base_dir, "remap.db")
        )

    master_project_files = get_master_project_files(ctx.master_dir)
    assert gpkg_path in master_project_files
    master_project_files.remove(gpkg_path)
    print("Master project files to copy to new projects: " + str(master_project_files))
    print("Fetching work packages projects info...")
    group_size = 50  # Maximum project names group size accepted by `get_projects_by_names`
    wp_names_groups = [wp_config.wp_names[i : i + group_size] for i in range(0, len(wp_config.wp_names), group_size)]
    wp_projects_info = {}
    for wp_names_group in wp_names_groups:
        project_group_info = ctx.mc.get_projects_by_names([wp.mergin_project for wp in wp_names_group])
        wp_projects_info.update(project_group_info)
    # list of WP names that did not exist previously (and we will have to create a new Mergin project for them)
    wp_new = set()

    def prepare_work_package(wp):
        wp_name, wp_value, wp_mergin = wp.name, wp.value, wp.mergin_project
        wp_dir = os.path.join(ctx.tmp_dir, "wp-" + wp_name)
        wp_base_file = os.path.join(ctx.master_dir, "work-packages", wp_name + ".gpkg")
        if os.path.exists(wp_base_file):  # already processed?
            print("Preparing work package " + wp_name)
            shutil.copy(wp_base_file, os.path.join(ctx.wp_alg_base_dir, wp_name + ".gpkg"))
            wp_info = wp_projects_info[wp_mergin]
            try:
                server_version = wp_info["version"]
            except KeyError:
                server_version = None
            print("Downloading work package project " + wp_mergin + "...")
            download_project_with_cache(ctx.mc, wp_mergin, wp_dir, ctx.cache_dir, server_latest_version=server_version)
            print("Done.")

            shutil.copy(os.path.join(wp_dir, gpkg_path), os.path.join(ctx.wp_alg_input_dir, wp_name + ".gpkg"))
            if not ctx.skip_lock:
                ctx.project_padlock.lock(wp_dir)
        else:
            print("First time encountered WP " + wp_name + " - not collecting input")
            wp_new.add(wp_name)

    with ThreadPoolExecutor(max_workers=ctx.max_workers) as executor:
        for result in executor.map(prepare_work_package, wp_config.wp_names):
            if result:
                print(result)

    if not ctx.skip_lock:
        ctx.project_padlock.lock(ctx.master_dir)

    # output which versions we are about to use
    replay = Replay.from_context(ctx, wp_config, wp_new)
    replay_json_dump = json.dumps(replay.to_dict(), indent=2)
    print("INPUT PROJECTS:")
    print(replay_json_dump)

    return wp_config, wp_new, gpkg_path, master_project_files


def push_mergin_project(mc, directory, max_retries=3, sleep_time=5):
    push_attempt = 1
    while True:
        try:
            job = mergin.client_push.push_project_async(mc, directory)
            if job is None:
                return False  # there is nothing to push (or we only deleted some files)
            mergin.client_push.push_project_wait(job)
            mergin.client_push.push_project_finalize(job)
            return True
        except mergin.ClientError:
            if push_attempt <= max_retries:
                print(f"Push attempt number {push_attempt} failed. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
                push_attempt += 1
            else:
                raise


def push_data_to_projects(ctx: MerginWPContext, wp_config, wp_new, gpkg_path, master_project_files):
    """Push data to all Mergin Maps projects"""

    def push_work_package(wp):
        wp_name, wp_value, wp_mergin = wp.name, wp.value, wp.mergin_project
        wp_dir = os.path.join(ctx.tmp_dir, "wp-" + wp_name)
        if wp_name in wp_new:
            # we need to create new project
            if not ctx.dry_run:
                print("Creating project: " + wp_mergin + " for work package " + wp_name)
                ctx.mc.create_project(wp_mergin, False)
                download_project_with_cache(ctx.mc, wp_mergin, wp_dir, ctx.cache_dir)
            else:
                os.makedirs(wp_dir, exist_ok=True)  # Make WP project folder that would be created by the Mergin Client
            shutil.copy(os.path.join(ctx.wp_alg_output_dir, wp_name + ".gpkg"), os.path.join(wp_dir, gpkg_path))

            # copy other files from master project
            for relative_filepath in master_project_files:
                print("Adding file from master project: " + relative_filepath)
                src_path = os.path.join(ctx.master_dir, relative_filepath)
                dst_path = os.path.join(wp_dir, relative_filepath)
                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                shutil.copy(src_path, dst_path)

        # new version of the geopackage
        shutil.copy(os.path.join(ctx.wp_alg_output_dir, wp_name + ".gpkg"), os.path.join(wp_dir, gpkg_path))

        if ctx.dry_run:
            print(f"This is a dry run - no changes pushed for work package: {wp_name}")
            return
        print("Uploading new version of the project: " + wp_mergin + " for work package " + wp_name)
        if not ctx.skip_lock:
            ctx.project_padlock.unlock(wp_dir)
        if push_mergin_project(ctx.mc, wp_dir):
            print("Uploaded a new version: " + mergin.MerginProject(wp_dir).version())
        else:
            print("No changes (not creating a new version).")

    with ThreadPoolExecutor(max_workers=ctx.max_workers) as executor:
        for result in executor.map(push_work_package, wp_config.wp_names):
            if result:
                print(result)

    # in the last step, let's update the master project
    # (update the master database file and update base files for work packages)
    shutil.copy(os.path.join(ctx.wp_alg_output_dir, "master.gpkg"), os.path.join(ctx.master_dir, gpkg_path))
    if not os.path.exists(os.path.join(ctx.master_dir, "work-packages")):
        os.makedirs(os.path.join(ctx.master_dir, "work-packages"))
    shutil.copy(
        os.path.join(ctx.wp_alg_output_dir, "master.gpkg"), os.path.join(ctx.master_dir, "work-packages", "master.gpkg")
    )
    shutil.copy(
        os.path.join(ctx.wp_alg_output_dir, "remap.db"), os.path.join(ctx.master_dir, "work-packages", "remap.db")
    )
    for wp in wp_config.wp_names:
        wp_name, wp_value, wp_mergin = wp.name, wp.value, wp.mergin_project
        shutil.copy(
            os.path.join(ctx.wp_alg_output_dir, wp_name + ".gpkg"),
            os.path.join(ctx.master_dir, "work-packages", wp_name + ".gpkg"),
        )

    if ctx.dry_run:
        print(f"This is a dry run - no changes pushed into the master project: {ctx.master_mergin_project}")
    else:
        print("Uploading new version of the master project: " + ctx.master_mergin_project)
        if not ctx.skip_lock:
            ctx.project_padlock.unlock(ctx.master_dir)
        if push_mergin_project(ctx.mc, ctx.master_dir):
            print("Uploaded a new version: " + mergin.MerginProject(ctx.master_dir).version())
        else:
            print("No changes (not creating a new version).")
    try:
        shutil.rmtree(ctx.tmp_dir)
    except (PermissionError, OSError):
        print(f"Couldn't remove temporary dir. Removing '{ctx.tmp_dir}' skipped.")
    # Release locked projects if any left - just in case
    if not ctx.skip_lock:
        ctx.project_padlock.unlock_all()
    print("Done.")


def run_wp_mergin_with_context(ctx: MerginWPContext):
    initialize(ctx)
    wp_config, wp_new, gpkg_path, master_project_files = prepare_inputs(ctx)
    make_work_packages(ctx.wp_alg_dir, wp_config)
    push_data_to_projects(ctx, wp_config, wp_new, gpkg_path, master_project_files)


def run_wp_mergin(mergin_project, cache_dir=None, dry_run=False):
    """This function can be used to run work packaging from other Python scripts"""
    ctx = MerginWPContext()
    ctx.master_mergin_project = mergin_project
    ctx.cache_dir = cache_dir
    ctx.dry_run = dry_run
    run_wp_mergin_with_context(ctx)
