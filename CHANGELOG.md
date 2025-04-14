# Changelog

## 1.2.1

- fix error when the table does not use `fid` column as primary key (#64)

## 1.2.0

- Improve robustness and speed of sync (#60, #61)
- Fix error when more than 50 projects are used (#53)
- Add a developer option to keep diffs when needed for debugging (#46)
- Bump mergin-py-client to 0.9.0

## 1.1.0

- Download/pull and push projects in parallel (by default max. 8 project in parallel) (#48)
- Retry push if it fails (up to three times)
- Lock projects to lower the chance of the project getting modified while the script is running (#49)
- Added Docker container
- Significantly improved coverage of auto tests


## 1.0.1
### Features
- rebranding and URL update (#40)
- custom user agent header (#41)

### Fixed bugs
- none

### Infrastructure
- uses mergin-client 0.7.3

## 1.0.0
- Initial Release
