# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), 
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Checks for duplicates and completeness

### Changed
- 


### Removed
- Type checking when creating checks for pandera schema (checking column type is unaffected)

### Fixed
- Issue with type when loading schema from file
- Issue where string checks would not be added to pandera schema when loaded from file

## [1.0.0] - 2026-01-09

### Added
- Initial release.
