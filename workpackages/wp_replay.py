"""
This module contains Replay class to allow debugging of work package runs.
When data get downloaded from a MM server, we capture the data to allow
replay as a JSON, so that at some point later we can use the same project
versions.
"""

import mergin
import os

from .wp import WPConfig


class ReplayProject:
    """Contains replay information about a single project (master or WP)"""

    def __init__(self, wp_name: str, project_full_name: str, version: str):
        self.wp_name = wp_name
        self.project_full_name = project_full_name
        self.version = version  # empty string if the project does not exist yet

    def to_dict(self):
        return {
            "wp-name": self.wp_name,
            "project-full-name": self.project_full_name,
            "version": self.version,
        }

    @staticmethod
    def from_dict(project_dict: dict) -> "ReplayProject":
        return ReplayProject(project_dict["wp"], project_dict["project-full-name"], project_dict["version"])

    @staticmethod
    def from_project_directory(directory: str, wp_name: str = None) -> "ReplayProject":
        mp = mergin.MerginProject(directory)
        return ReplayProject(wp_name, mp.project_full_name(), mp.version())


class Replay:
    """Contains replay information about master project and all WP projects"""

    def __init__(self, master_project: ReplayProject, wp_projects: list[ReplayProject]):
        self.master_project = master_project
        self.wp_projects = wp_projects

    def to_dict(self) -> str:
        return {"master": self.master_project.to_dict(), "wp": [project.to_dict() for project in self.wp_projects]}

    @staticmethod
    def from_dict(replay_dict) -> "Replay":
        return Replay(
            ReplayProject.from_dict(replay_dict["master"]),
            [ReplayProject.from_dict(project_dict for project_dict in replay_dict["wp"])],
        )

    @staticmethod
    def from_context(ctx, wp_config: WPConfig, wp_new: set[str]) -> "Replay":

        master_project = ReplayProject.from_project_directory(ctx.master_dir)

        wp_projects = []
        for wp_name in wp_config.wp_names:
            wp_dir = os.path.join(ctx.tmp_dir, "wp-" + wp_name.name)
            if wp_name.name in wp_new:
                # the MM project does not exist yet
                wp_projects.append(ReplayProject(wp_name.name, wp_name.mergin_project, ""))
            else:
                wp_projects.append(ReplayProject.from_project_directory(wp_dir, wp_name.name))

        return Replay(master_project, wp_projects)
