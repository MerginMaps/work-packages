"""
Module with useful utilities.
"""
import os
import shutil
import string


def escape_double_quotes(name):
    escaped_name = name.replace('"', '""')
    quoted_name = f'"{escaped_name}"'
    return quoted_name


def download_project_with_cache(mc, project_path, directory, cache_dir, version=None):
    if not cache_dir:
        mc.download_project(project_path, directory, version=version)
        return
    string_translation = str.maketrans("", "", string.punctuation)
    project_info = mc.project_info(project_path)
    updated_timestamp = project_info["updated"]
    project_name = project_path.split("/")[-1]
    translated_timestamp = updated_timestamp.translate(string_translation)
    project_cache_dir = os.path.join(cache_dir, f"{project_name}_{translated_timestamp}")
    if not os.path.exists(project_cache_dir):
        mc.download_project(project_path, project_cache_dir, version=version)
    else:
        print("Project already checked out - copying cached files")
    shutil.copytree(project_cache_dir, directory)
