[![Code Style](https://github.com/MerginMaps/mergin-work-packages/actions/workflows/code_style.yml/badge.svg)](https://github.com/MerginMaps/mergin-work-packages/actions/workflows/code_style.yml)
![Auto Tests](https://github.com/MerginMaps/mergin-work-packages/workflows/Auto%20Tests/badge.svg)

# mergin-work-packages

Mergin Maps Work Packages - manage field surveys for multiple teams.

The tool allows users to create Mergin Maps projects that contain only a subset
of data of the main Mergin Maps project and to set up two-way synchronization
between the main project and the dependent projects (called "work package projects"
in this context) as illustrated here:

![High level overview](img/wp-high-level.png)

The main project ("Survey") contains all data, while the work package projects
("Survey Team A", "Survey Team B") only contain partial data. Teams therefore
only can see and modify data assigned to them by the admin of the main project.

With the two-way synchronization provided by this tool:
- changes in the main project are propagated to the work package projects
- changes in the work package projects are propagated back to the main project

<div><img align="left" width="45" height="45" src="https://raw.githubusercontent.com/MerginMaps/docs/main/src/.vuepress/public/slack.svg"><a href="https://merginmaps.com/community/join">Join our community chat</a><br/>and ask questions!</div>

## Quick start

If you would like to start with a simple pre-configured project:

1. Clone [MerginMaps/work-packages-demo](https://app.merginmaps.com/projects/lutraconsulting/work-packages-demo/tree)
   Mergin Maps project (you can do it either on the web or from the plugin). As the owner
   of the cloned project, you will have write permissions which are necessary to run the tool.
   Assuming your Mergin Maps username is `john`, the cloned project could be called `john/test-work-packages`.

2. Download your cloned project and edit `mergin-work-packages.yml` configuration file.
   You will at least need to modify the `mergin_project` lines.
   You could use e.g. `john/farms-Kyle` and `john/farms-Emma` for project names of work packages.
   After your edits do not forget to sync your changes back to Mergin Maps service.

3. Install the tool  
   To install `mergin-work-packages` you need to get and unzip the latest [release from GitHub](https://github.com/MerginMaps/mergin-work-packages/releases)
   and then install the dependencies:
```
    cd mergin-work-packages
    python3 -m venv venv
    ./venv/bin/pip3 install -r requirements.txt
    ./venv/bin/python3 mergin_work_packages.py --help
```

4. Run the tool with the name of your project:
   ```bash
   $ ./venv/bin/python3 mergin_work_packages.py john/test-work-packages
   ```
   After the initial run, you should see that the work package projects `john/farms-Kyle` and `john/farms-Emma`
   got created and they are ready to be used in QGIS or Mergin Maps Input, containing subsets of data of the main project.
   
5. After you do modifications of the data in any of the projects (the main one or the work package projects)
   and run the tool again as in step 3, changes will be propagated among projects.

## How to use

We will assume that you have a Mergin Maps project called `Survey` which contains the following files:
- survey.gpkg - a GeoPackage with a survey table called `farms`
- project.qgz - a QGIS project file using data from the GeoPackage

Data in the `farms` table can look like this:

| fid | name | geometry | notes | survey_team |
|-----|------|----------|-------|------|
|  1  | Old MacDonald's | POINT(...) | Lots of animals | A |
|  2  | MacGyver's farm | POINT(...) | Looking suspicious | B |
| ... | ... | ... | ... | ... |

The `survey_team` column determines which team is responsible for the survey.

To configure the tool, we will create a YAML configuration file named `mergin-work-packages.yml`
and placed in the root folder of Mergin Maps project. Here is how it can look like:
- YAML for `filter-column` filtering method
```yaml
file: survey.gpkg

tables:
  - name: farms
    method: filter-column
    filter-column-name: survey_team

work-packages:
  - name: TeamA
    value: A
    mergin-project: My Company/Survey Team A
  - name: TeamB
    value: B
    mergin-project: My Company/Survey Team B
```
- YAML for `filter-geometry` filtering method
```yaml
file: survey.gpkg

tables:
  - name: farms
    method: filter-geometry

work-packages:
  - name: TeamA
    value: Polygon ((5.35 19.61, 5.44 15.28, 19.40 15.16, 19.14 18.94, 5.35 19.61))
    mergin-project: My Company/Survey Team A
  - name: TeamB
    value: Polygon ((18.50 19.11, 15.28 7.46, 8.07 9.80, 18.50 19.11))
    mergin-project: My Company/Survey Team B
```

Next to the path to the GeoPackage (`file`), there are the following bits of configuration:

- `tables` list - defines which tables will be filtered and based on which column.
  Each item has to define name of the filtered table (`name`) and filtering method
  (`method`). Currently, there are 2 filtering methods available.
  - `filter-column` method, where values from the given column are used to determine whether the row belongs to a
  particular work package or not - this is set with `filter-column-name`.
  - `filter-geometry` method, where polygon geometries written in WKT (Well Known Text) format are used as a work
  packages boundaries. Feature geometries that intersects with those boundaries will be qualified as belonging to the
  work package.

- `work-packages` list - defines dependent "work package" projects:
  what is the internally used name of each work package (`name`), what is the expected
  value of the filter column (`value`) and which Mergin project is assigned to that
  work package (`mergin-project`).

After the configuration file is written (and synced to the Mergin Maps project), we are all
set to run the tool. We only need to specify name of the main Mergin Maps project, the tool
will ask about the login credentials to Mergin Maps and run the processing:

```bash
$ python3 mergin_work_packages.py "My Company/Survey"
```

After the initial run of the tool, the `Survey Team A` and `Survey Team B` Mergin Maps projects will be created,
containing the filtered GeoPackage and any other files from the original (main) project, in our case just the
`project.qgz` file.

The main project and the work package projects can be shared with others and updated, they are independent. But next time
you run the tool, it will do the two-way sync: pull changes from projects for teams A and B, merge
them with changes in the main project, and finally update data in all connected Mergin Maps projects.

## Running with Docker

There is a Docker container available, so it is possible to run the work packaging script like this:

```bash
$ docker run -i -t lutraconsulting/mergin-work-packages john/test-work-packages
```

## Under the hood

The following figure illustrates how the merge/split algorithm works in two steps to first merge changes
in the main (master) project with changes in the work package projects, followed by update of the work
package projects to a new state based on the merged content of the main (master) project.

![Algorithm overview](img/wp-alg.png)

The "base" data are stored in `mergin-work-packages` sub-directory of the main Mergin Maps project
from previous runs of the tool (and should never be edited by users). The "input" data are
the latest versions of the user data in the main (master) project and in work package projects.
The "output" data are then pushed to the main (master) project and work package projects,
and also kept as the "base" data for the next run of the tool.

## Run tests

### MacOS
```
/Applications/QGIS3.16.app/Contents/MacOS/bin/python3 -m pip install pytest
MERGIN_WORKPACKAGES_TMP=~/tmp/mergin_wp /Applications/QGIS3.16.app/Contents/MacOS/bin/python3 -m pytest -v
```

## Developing on Windows
If you're experiencing sqlite3 DLL issues after installing tool dependencies make sure that you have path with `sqlite3.dll` library added to the system PATH
environment variables list. For OSGeo4W users you can use `C:\OSGeo4W64\bin`.


## Releasing new version

1. Run `./scripts/update_version.bash 1.2.3`
2. Tag the new version in git repo and create a release on GitHub
3. Build and upload the new container (both with the new version tag and as the latest tag)
   ```
   docker build --no-cache -t lutraconsulting/mergin-work-packages .
   docker tag lutraconsulting/mergin-work-packages lutraconsulting/mergin-work-packages:1.0.3
   docker push lutraconsulting/mergin-work-packages:1.0.3
   docker push lutraconsulting/mergin-work-packages:latest
   ```
