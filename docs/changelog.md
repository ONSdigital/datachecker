# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), 
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Checks for duplicates and completeness
- Support for polars
- Support for PySpark
- Option to give duplicate checks column subset

### Changed
- Class structure (users are not impacted by change)
- replaced allowed_strings with allowed_values which will now work for all data types (this would have worked previously but new name reflects this better)
- replaced forbidden_strings with forbidden_values as above

### Removed
- Type checking when creating checks for pandera schema (checking column type is unaffected)

### Fixed
- Issue with type when loading schema from file
- Issue where string checks would not be added to pandera schema when loaded from file

## [1.0.1] - 2026-03-13

### Added
- Publishing package to PyPI

## [1.0.0] - 2026-01-09

### Added
- Initial release.
