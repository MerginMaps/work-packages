"""
Module with useful utilities.
"""

import json
import hashlib
import mergin
import os
import shutil


def escape_double_quotes(name):
    escaped_name = name.replace('"', '""')
    quoted_name = f'"{escaped_name}"'
    return quoted_name


def download_project_with_cache(mc, project_path, directory, cache_dir, version=None, server_latest_version=None):
    if not cache_dir:
        mc.download_project(project_path, directory, version=version)
        return
    project_namespace, project_name = project_path.split("/")
    project_cache_dir = os.path.join(cache_dir, f"{project_namespace}_{project_name}")
    if not os.path.exists(project_cache_dir):
        mc.download_project(project_path, project_cache_dir, version=version)
    else:
        mp = mergin.MerginProject(project_cache_dir)
        if server_latest_version is None or mp.version() != server_latest_version:
            mc.pull_project(project_cache_dir)
        else:
            print(f"Local and server project versions are the same. Pulling '{project_path}' project skipped")
        print("Project cached - copying existing files")
    shutil.copytree(project_cache_dir, directory)


class ProjectPadlock:
    """
    Class for handling projects locking/unlocking on the Mergin Maps server.
    This allows to prevent editing projects by other users while mergin-work-packages script is running.
    """

    LOCK_EXPIRED_MESSAGE = (
        "The requested URL was not found on the server. "
        "If you entered the URL manually please check your spelling and try again."
    )

    def __init__(self, mc):
        self.mc = mc
        self.locked_projects = {}

    def lock(self, directory):
        print(f"--- locking dir: '{directory}'")
        mp = mergin.MerginProject(directory)
        project_path = mp.project_full_name()
        local_version = mp.version()
        size = 1
        checksum = hashlib.sha1().hexdigest()
        changes = {
            "added": [{"path": "lock.txt", "size": size, "checksum": checksum}],
            "updated": [],
            "removed": [],
        }
        data = {"version": local_version, "changes": changes}
        try:
            resp = self.mc.post(f"/v1/project/push/{project_path}", data, {"Content-Type": "application/json"})
        except mergin.ClientError as err:
            print("Error starting transaction: " + str(err))
            print("--- push aborted")
            raise
        server_resp = json.load(resp)
        locked_transaction_id = server_resp["transaction"]
        self.locked_projects[directory] = locked_transaction_id
        print(f"--- locked dir: '{directory}' (transaction ID: {locked_transaction_id}).")

    def unlock(self, directory):
        if directory in self.locked_projects:
            print(f"--- releasing locked dir: '{directory}")
            locked_transaction_id = self.locked_projects[directory]
            try:
                self.mc.post(f"/v1/project/push/cancel/{locked_transaction_id}")
            except mergin.ClientError as err:
                error_message = str(err)
                if self.LOCK_EXPIRED_MESSAGE in error_message:
                    print("--- push cancelling skipped as project lock expired automatically")
                else:
                    print("--- push cancelling failed! " + error_message)
                    raise err
            del self.locked_projects[directory]
            print(f"--- released locked dir: '{directory}' (transaction ID: {locked_transaction_id})")

    def unlock_all(self):
        print(f"Number of locked projects left: {len(self.locked_projects)}")
        for directory in list(self.locked_projects.keys()):
            self.unlock(directory)
