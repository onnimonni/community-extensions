//! Rust HTML Parser for DuckDB Crawler Extension
//!
//! Provides FFI interface for extracting structured data from HTML:
//! - JSON-LD (with @graph support)
//! - Microdata (schema.org)
//! - OpenGraph meta tags
//! - CSS selectors
//! - JavaScript variables (via tree-sitter AST parsing)

pub mod extractors;
pub mod ffi;

pub use extractors::*;
pub use ffi::*;
