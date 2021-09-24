"""
Module with useful utilities.
"""
import os
import shutil


def escape_double_quotes(name):
    escaped_name = name.replace('"', '""')
    quoted_name = f'"{escaped_name}"'
    return quoted_name


def download_project_with_cache(mc, project_path, directory, cache_dir, version=None):
    if not cache_dir:
        mc.download_project(project_path, directory, version=version)
        return
    project_namespace, project_name = project_path.split("/")
    project_cache_dir = os.path.join(cache_dir, f"{project_namespace}_{project_name}")
    if not os.path.exists(project_cache_dir):
        mc.download_project(project_path, project_cache_dir, version=version)
    else:
        mc.pull_project(project_cache_dir)
        print("Project cached - copying existing files")
    shutil.copytree(project_cache_dir, directory)
