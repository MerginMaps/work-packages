![Auto Tests](https://github.com/lutraconsulting/mergin-work-packages/workflows/Auto%20Tests/badge.svg)

# mergin-work-packages

Mergin Work Packages - manage field surveys for multiple teams. 

The tool allows users to create Mergin projects that contain only a subset
of data of the main Mergin project and to set up two-way synchronization
between the main project and the dependent projects (called "work package projects"
in this context) as illustrated here:

![High level overview](img/wp-high-level.png) 

The main project ("Survey") contains all data, while the work package projects
("Survey Team A", "Survey Team B") only contain partial data. Teams therefore
only can see and modify data assigned to them by the admin of the main project.

With the two-way synchronization provided by this tool:
- changes in the main project are propagated to the work package projects
- changes in the work package projects are propagated back to the main project

## How to use

We will assume that you have a Mergin project called `Survey` which contains the following files:
- survey.gpkg - a GeoPackage with a survey table called `farms`
- project.qgz - a QGIS project file using data from the GeoPackage

Data in the `farms` table can look like this:

| fid | name | geometry | notes | survey_team |
|-----|------|----------|-------|------|
|  1  | Old MacDonald's | POINT(...) | Lots of animals | A |
|  2  | MacGyver's farm | POINT(...) | Looking suspicious | B | 
| ... | ... | ... | ... | ... |

The `survey_team` column determines which team is responsible for the survey.

To configure the tool, we will create a SQLite database `work-packages/config.db` containing two tables
(see `config.sql` file in this repo for the exact structure of the tables):

- `wp_tables` table - defines which tables will be filtered and based on which column:

  | table_name | filter_column_name |
  |------------|--------------------|
  | farms | survey_team |

- `wp_names` table - defines dependent "work package" projects:
  what is the internally used name of each work package, what is the expected value of the filter
  column and which Mergin project is assigned to that work package: 

  | name | value | mergin_project |
  |------|-------|----------------|
  | Team A | A | My Company / Survey Team A |
  | Team B | B | My Company / Survey Team B |

After this, we are all set to run the tool, with three arguments: 1. name of the main Mergin project, 2. filename
of the GeoPackage used to split data, 3. Mergin username of the admin, who will be creating/updating the projects.  

```bash
$ python3 wp_mergin.py "My Company/Survey" survey.gpkg MyCompanyAdmin
```

After the initial run of the tool, the `Survey Team A` and `Survey Team B` Mergin projects will be created,
containing the filtered GeoPackage and any other files from the original (main) project, in our case just the
`project.qgz` file.

The main project and the work package projects can be shared with others and updated, they are independent. But next time
you run the tool, it will do the two-way sync: pull changes from projects for teams A and B, merge
them with changes in the main project, and finally update data in all connected Mergin projects.

## Under the hood

The following figure illustrates how the merge/split algorithm works in two steps to first merge changes
in the main (master) project with changes in the work package projects, followed by update of the work
package projects to a new state based on the merged content of the main (master) project. 

![Algorithm overview](img/wp-alg.png) 

The "base" data are stored in `mergin-work-packages` sub-directory of the main Mergin project
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

