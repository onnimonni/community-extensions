//! HTML extraction modules
//!
//! Each module provides extraction for a specific data format.

mod css_extractor;
mod js_extractor;
mod jsonld_extractor;
mod microdata_extractor;
mod opengraph_extractor;

pub use css_extractor::*;
pub use js_extractor::*;
pub use jsonld_extractor::*;
pub use microdata_extractor::*;
pub use opengraph_extractor::*;

use serde::{Deserialize, Serialize};

/// Request for extraction from C++
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionRequest {
    pub specs: Vec<ExtractSpec>,
}

/// Single extraction specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractSpec {
    /// Source type: jsonld, microdata, og, js, css, meta
    pub source: String,
    /// Path within source (dot notation or CSS selector)
    pub path: String,
    /// Column alias for output
    pub alias: String,
    /// Optional type for coercion
    #[serde(default)]
    pub target_type: Option<String>,
    /// Optional transform: trim, parse_price, etc.
    #[serde(default)]
    pub transform: Option<String>,
}

/// Result returned to C++
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExtractionResult {
    pub values: Vec<ExtractedValue>,
}

/// Single extracted value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedValue {
    pub alias: String,
    pub value: Option<String>,
}
