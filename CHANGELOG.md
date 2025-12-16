# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed
- **Critical: API Data Inconsistency** - Fixed crawler failure due to API backend multi-node data inconsistency
  - API's `totalAnnouncement` field fluctuates between requests (e.g., 1315 vs 1322)
  - Single crawl pass may miss some records due to pagination boundary shifts
  - Solution: Multi-pass crawling with deduplication until unique count equals max(totalAnnouncement)
  - Strict validation: unique count must equal max(totalAnnouncement), throws AssertionError otherwise
  - Max 10 retry attempts before failing

### Added
- **Performance Forecast Support** - Added `category_yjygjxz_szsh` to crawl performance forecasts (业绩预告)
  - Extended `_identify_period_type` to classify forecast types (年报业绩预告/半年报业绩预告/一季报业绩预告/三季报业绩预告)
  - Enables more timely factor signals despite lower data precision

### Fixed
- **Critical: Timezone Handling** - Fixed potential date offset in cloud environments
  - Explicitly set `Asia/Shanghai` timezone for timestamp parsing
  - Prevents Look-ahead bias in backtesting when running in UTC containers

- **Critical: Pagination Data Loss Bug** - Fixed API `totalpages` field bug causing last page data loss
  - Changed pagination logic from `totalpages` to `hasMore` field for accurate page traversal
  - Added strict data integrity validation: actual count must match API declared count
  - Throws `AssertionError` on data mismatch instead of silent failure
  - Throws `RuntimeError` on page fetch failure instead of silent skip
- **PDF Conversion Fallback System** - Resolved Issue #4: PDF conversion failures
  - Added multi-library fallback mechanism for PDF processing
  - Primary: pdfplumber (most accurate)
  - Fallback 1: PyPDF2 (handles CropBox issues)
  - Fallback 2: pdfminer.six (robust alternative)
  - Suppressed CropBox warning messages from pdfplumber
  - Automatic library switching when one method fails
  - Improved error logging with library-specific debug messages
- **Multiprocessing Session Sharing** - Fixed `requests.Session` cross-process sharing issue
  - Each worker process now creates independent `PDFConverter` instance
  - Improved stability in parallel PDF download/conversion

### Added
- `page_delay` config parameter (default 0.3s) for rate limiting between page requests
- Optional dependencies: PyPDF2 and pdfminer.six for better PDF compatibility
- Detailed conversion method logging (shows which library succeeded)

## [2025-11-21] - Major Refactor

### Added
- Incremental save mechanism: auto-save every 100 records during crawling
- Strict year validation: filter out mismatched year records
- Comprehensive GitHub community templates (Issue, PR, Contributing, Code of Conduct)
- Enhanced documentation with bilingual support (English/Chinese)
- Organized image assets in dedicated `imgs/` folder

### Changed
- Refactored all scripts to object-oriented architecture
- Improved error handling and retry logic across all modules
- Enhanced progress tracking and statistics reporting
- Removed all emoji from code and logs for professional appearance
- Updated README with comprehensive disclaimer and donation section

### Fixed
- Progress display overflow issues in crawler
- Missing data due to coarse date segmentation (now crawls daily)
- Type hints and code style improvements throughout

## [2025-03-15]

### Added
- Requirements file for dependency management
- Support for other announcement types in downloader

## [2024-10-13]

### Fixed
- Missing companies in crawler results

## [2024-02-14]

### Added
- Uploaded master sheet covering 2004-2023
- Improved code readability

## [2024-01-04]

### Improved
- Keyword extraction accuracy
- Added universal text analyzer

## [2023-05-25]

### Changed
- Full refactor with parameterized workflow

## [2023-04-20]

### Added
- Initial project release
