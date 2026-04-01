# Changelog

## 2026.04.01-8b77aea (2026-04-01)

### Added

- Test(registry-stats): add property-based and edge case tests
- Test(registry-stats): add comprehensive test coverage for filtering and API handlers
- Add newline at end of compose.yaml file
- Add validation for negative download counts and path traversal edge cases
- Add wildcard support for Docker Hub and GHCR repos
- Add repeated param support for multi-select repo filtering
- Add repo filtering to summary endpoint
- Consolidate repo variable to use summary endpoint
- Remove total_pulls field and refine dashboard display
- Refine dashboard UI and standardize repo variable format
- Add registry and repo filtering with dashboard refinements
- Remove base_url variable and use relative API paths
- Add registry filter and refine dashboard UI
- Enhance polling, validation, and grafana dashboard
- Simplify configuration and update dashboard terminology
- Add Grafana dashboard for container image metrics
- Add encrypted environment configuration
- Add Docker Hub and GHCR metadata collection service

### Fixed

- Improve startup health state and graceful shutdown handling
- Update GHCR package URL format
- Handle strconv.Atoi errors explicitly

### Changed

- Remove unnecessary blank line
- Refactor(subflux): enhance code review workflow and strengthen test coverage
- Test(registry-stats): simplify conditional logic with switch statements
- Refactor(registry-stats): improve code documentation, storage safety, and result sorting
- Refactor(registry-stats): optimize HTTP client and improve error handling
- Migrate to structured logging and enhance validation
- Refactor(registry-stats): improve HTML parsing logic clarity
- Docs(registry-stats): update technical documentation and dashboard configuration
- Update documentation and dashboard for polling interval and registry tracking
- Rotate encrypted environment variables
- Refactor Grafana dashboard JSON formatting and layout
- Remove template variables and simplify dashboard queries
- Update Grafana dashboard query configurations
- Update Grafana dashboard datasource configuration
- Perf(registry-stats): optimize slice allocation and improve test coverage
- Test(registry-stats): update dockerHubHeaders calls to use pointer semantics
- Refactor(registry-stats): improve code quality and pointer semantics
- Migrate from structured logging to standard log package

### Dependencies

- Update gcr.io/distroless/static-debian13:nonroot docker digest to e3f9456 (#137)
- Update go to v1.26.1

## 2026.03.21-220833f (2026-03-22)

### Changed

- Remove unnecessary blank line
- Refactor(subflux): enhance code review workflow and strengthen test coverage

## 2026.03.15-ef81144 (2026-03-16)

### Dependencies

- Update gcr.io/distroless/static-debian13:nonroot docker digest to e3f9456 (#137)

## 2026.03.13-7a83b5f (2026-03-14)

### Added

- Test(registry-stats): add property-based and edge case tests
- Test(registry-stats): add comprehensive test coverage for filtering and API handlers

### Changed

- Test(registry-stats): simplify conditional logic with switch statements

## 2026.03.12-0de7110 (2026-03-12)

### Fixed

- Improve startup health state and graceful shutdown handling

## 2026.03.11-116b511 (2026-03-11)

### Added

- Add newline at end of compose.yaml file

### Changed

- Refactor(registry-stats): improve code documentation, storage safety, and result sorting
- Refactor(registry-stats): optimize HTTP client and improve error handling
- Migrate to structured logging and enhance validation

## 2026.03.07-e9dfac0 (2026-03-08)

### Added

- Add validation for negative download counts and path traversal edge cases

### Changed

- Refactor HTML parsing logic clarity

## 2026.03.07-9112d85 (2026-03-07)

### Added

- Initial release
