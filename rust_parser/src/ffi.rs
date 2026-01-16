//! FFI interface for C++ interop
//!
//! Provides C-compatible functions for extracting data from HTML.
//! All strings are passed as JSON for simplicity and type safety.

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use crate::extractors::{
    extract_css, extract_jsonld, extract_js_variables, extract_microdata, extract_opengraph,
    ExtractionRequest, ExtractionResult,
};

/// Result struct returned to C++
/// Both pointers are owned by Rust and must be freed via free_extraction_result
#[repr(C)]
pub struct ExtractionResultFFI {
    /// JSON-serialized result (null-terminated)
    pub json_ptr: *mut c_char,
    /// Error message if extraction failed (null-terminated), or null on success
    pub error_ptr: *mut c_char,
}

/// Extract data from HTML according to the request specification.
///
/// # Arguments
/// * `html_ptr` - Pointer to HTML content (UTF-8, not necessarily null-terminated)
/// * `html_len` - Length of HTML content in bytes
/// * `request_json` - JSON-serialized ExtractionRequest (null-terminated)
///
/// # Returns
/// ExtractionResultFFI with either json_ptr set (success) or error_ptr set (failure)
///
/// # Safety
/// - `html_ptr` must point to valid memory of at least `html_len` bytes
/// - `request_json` must be a valid null-terminated C string
/// - Caller must free the result via `free_extraction_result`
#[no_mangle]
pub unsafe extern "C" fn extract_from_html(
    html_ptr: *const c_char,
    html_len: usize,
    request_json: *const c_char,
) -> ExtractionResultFFI {
    // Parse HTML
    let html = if html_ptr.is_null() || html_len == 0 {
        String::new()
    } else {
        let slice = std::slice::from_raw_parts(html_ptr as *const u8, html_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => {
                return make_error_result("Invalid UTF-8 in HTML content");
            }
        }
    };

    // Parse request JSON
    let request_str = if request_json.is_null() {
        return make_error_result("Request JSON is null");
    } else {
        match CStr::from_ptr(request_json).to_str() {
            Ok(s) => s,
            Err(_) => {
                return make_error_result("Invalid UTF-8 in request JSON");
            }
        }
    };

    let request: ExtractionRequest = match serde_json::from_str(request_str) {
        Ok(r) => r,
        Err(e) => {
            return make_error_result(&format!("Failed to parse request JSON: {}", e));
        }
    };

    // Perform extraction
    let result = perform_extraction(&html, &request);

    // Serialize result to JSON
    match serde_json::to_string(&result) {
        Ok(json) => match CString::new(json) {
            Ok(cstr) => ExtractionResultFFI {
                json_ptr: cstr.into_raw(),
                error_ptr: ptr::null_mut(),
            },
            Err(_) => make_error_result("Result JSON contains null bytes"),
        },
        Err(e) => make_error_result(&format!("Failed to serialize result: {}", e)),
    }
}

/// Free an ExtractionResultFFI returned by extract_from_html
///
/// # Safety
/// - `result` must have been returned by `extract_from_html`
/// - Must only be called once per result
#[no_mangle]
pub unsafe extern "C" fn free_extraction_result(result: ExtractionResultFFI) {
    if !result.json_ptr.is_null() {
        drop(CString::from_raw(result.json_ptr));
    }
    if !result.error_ptr.is_null() {
        drop(CString::from_raw(result.error_ptr));
    }
}

/// Extract JSON-LD data from HTML (convenience function)
///
/// # Safety
/// Same as extract_from_html
#[no_mangle]
pub unsafe extern "C" fn extract_jsonld_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = if html_ptr.is_null() || html_len == 0 {
        String::new()
    } else {
        let slice = std::slice::from_raw_parts(html_ptr as *const u8, html_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => return make_error_result("Invalid UTF-8 in HTML"),
        }
    };

    let result = extract_jsonld(&html);
    match serde_json::to_string(&result) {
        Ok(json) => match CString::new(json) {
            Ok(cstr) => ExtractionResultFFI {
                json_ptr: cstr.into_raw(),
                error_ptr: ptr::null_mut(),
            },
            Err(_) => make_error_result("Result contains null bytes"),
        },
        Err(e) => make_error_result(&format!("Serialize error: {}", e)),
    }
}

/// Extract microdata from HTML (convenience function)
#[no_mangle]
pub unsafe extern "C" fn extract_microdata_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = if html_ptr.is_null() || html_len == 0 {
        String::new()
    } else {
        let slice = std::slice::from_raw_parts(html_ptr as *const u8, html_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => return make_error_result("Invalid UTF-8 in HTML"),
        }
    };

    let result = extract_microdata(&html);
    match serde_json::to_string(&result) {
        Ok(json) => match CString::new(json) {
            Ok(cstr) => ExtractionResultFFI {
                json_ptr: cstr.into_raw(),
                error_ptr: ptr::null_mut(),
            },
            Err(_) => make_error_result("Result contains null bytes"),
        },
        Err(e) => make_error_result(&format!("Serialize error: {}", e)),
    }
}

/// Extract OpenGraph data from HTML (convenience function)
#[no_mangle]
pub unsafe extern "C" fn extract_opengraph_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = if html_ptr.is_null() || html_len == 0 {
        String::new()
    } else {
        let slice = std::slice::from_raw_parts(html_ptr as *const u8, html_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => return make_error_result("Invalid UTF-8 in HTML"),
        }
    };

    let result = extract_opengraph(&html);
    match serde_json::to_string(&result) {
        Ok(json) => match CString::new(json) {
            Ok(cstr) => ExtractionResultFFI {
                json_ptr: cstr.into_raw(),
                error_ptr: ptr::null_mut(),
            },
            Err(_) => make_error_result("Result contains null bytes"),
        },
        Err(e) => make_error_result(&format!("Serialize error: {}", e)),
    }
}

/// Extract JavaScript variables from HTML (convenience function)
#[no_mangle]
pub unsafe extern "C" fn extract_js_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = if html_ptr.is_null() || html_len == 0 {
        String::new()
    } else {
        let slice = std::slice::from_raw_parts(html_ptr as *const u8, html_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => return make_error_result("Invalid UTF-8 in HTML"),
        }
    };

    let result = extract_js_variables(&html);
    match serde_json::to_string(&result) {
        Ok(json) => match CString::new(json) {
            Ok(cstr) => ExtractionResultFFI {
                json_ptr: cstr.into_raw(),
                error_ptr: ptr::null_mut(),
            },
            Err(_) => make_error_result("Result contains null bytes"),
        },
        Err(e) => make_error_result(&format!("Serialize error: {}", e)),
    }
}

/// CSS selector extraction
#[no_mangle]
pub unsafe extern "C" fn extract_css_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    selector: *const c_char,
) -> ExtractionResultFFI {
    let html = if html_ptr.is_null() || html_len == 0 {
        String::new()
    } else {
        let slice = std::slice::from_raw_parts(html_ptr as *const u8, html_len);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_string(),
            Err(_) => return make_error_result("Invalid UTF-8 in HTML"),
        }
    };

    let selector_str = if selector.is_null() {
        return make_error_result("Selector is null");
    } else {
        match CStr::from_ptr(selector).to_str() {
            Ok(s) => s,
            Err(_) => return make_error_result("Invalid UTF-8 in selector"),
        }
    };

    let result = extract_css(&html, selector_str);
    match serde_json::to_string(&result) {
        Ok(json) => match CString::new(json) {
            Ok(cstr) => ExtractionResultFFI {
                json_ptr: cstr.into_raw(),
                error_ptr: ptr::null_mut(),
            },
            Err(_) => make_error_result("Result contains null bytes"),
        },
        Err(e) => make_error_result(&format!("Serialize error: {}", e)),
    }
}

// Helper to create error result
fn make_error_result(msg: &str) -> ExtractionResultFFI {
    let error_cstr = CString::new(msg).unwrap_or_else(|_| CString::new("Unknown error").unwrap());
    ExtractionResultFFI {
        json_ptr: ptr::null_mut(),
        error_ptr: error_cstr.into_raw(),
    }
}

// Perform the actual extraction based on request
fn perform_extraction(html: &str, request: &ExtractionRequest) -> ExtractionResult {
    let mut result = ExtractionResult::default();

    for spec in &request.specs {
        let value = match spec.source.as_str() {
            "jsonld" => {
                let data = extract_jsonld(html);
                extract_path_from_json(&data, &spec.path)
            }
            "microdata" => {
                let data = extract_microdata(html);
                extract_path_from_json(&data, &spec.path)
            }
            "og" | "opengraph" => {
                let data = extract_opengraph(html);
                extract_path_from_json(&data, &spec.path)
            }
            "js" => {
                let data = extract_js_variables(html);
                extract_path_from_json(&data, &spec.path)
            }
            "css" => {
                // Path is the CSS selector, possibly with ::text or ::attr(x) suffix
                let (selector, extract_mode) = parse_css_selector_with_pseudo(&spec.path);
                let elements = extract_css(html, &selector);
                extract_from_css_elements(&elements, &extract_mode)
            }
            _ => None,
        };

        // Apply transform if present
        let final_value = if let Some(ref transform) = spec.transform {
            apply_transform(value.as_deref(), transform)
        } else {
            value
        };

        result.values.push(crate::extractors::ExtractedValue {
            alias: spec.alias.clone(),
            value: final_value,
        });
    }

    result
}

// Extract a value from JSON using dot notation path
fn extract_path_from_json(json: &serde_json::Value, path: &str) -> Option<String> {
    if path.is_empty() {
        return Some(json.to_string());
    }

    let mut current = json;
    for segment in path.split('.') {
        // Handle array index notation: field[0]
        if let Some(bracket_pos) = segment.find('[') {
            let field = &segment[..bracket_pos];
            if !field.is_empty() {
                current = current.get(field)?;
            }
            // Parse index
            let index_end = segment.find(']')?;
            let index: usize = segment[bracket_pos + 1..index_end].parse().ok()?;
            current = current.get(index)?;
        } else {
            current = current.get(segment)?;
        }
    }

    // Return as string - if it's already a string, unquote it
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        _ => Some(current.to_string()),
    }
}

// Parse CSS selector with pseudo-element (::text, ::attr(x))
fn parse_css_selector_with_pseudo(input: &str) -> (String, CssExtractMode) {
    if let Some(pos) = input.rfind("::text") {
        (input[..pos].to_string(), CssExtractMode::Text)
    } else if let Some(pos) = input.rfind("::attr(") {
        let attr_start = pos + 7;
        if let Some(attr_end) = input[attr_start..].find(')') {
            let attr_name = input[attr_start..attr_start + attr_end].to_string();
            (input[..pos].to_string(), CssExtractMode::Attr(attr_name))
        } else {
            (input.to_string(), CssExtractMode::Html)
        }
    } else {
        (input.to_string(), CssExtractMode::Html)
    }
}

enum CssExtractMode {
    Text,
    Html,
    Attr(String),
}

fn extract_from_css_elements(elements: &[String], mode: &CssExtractMode) -> Option<String> {
    if elements.is_empty() {
        return None;
    }

    // For now, just return the first element
    // TODO: Handle multiple matches
    let html = &elements[0];

    match mode {
        CssExtractMode::Html => Some(html.clone()),
        CssExtractMode::Text => {
            // Simple text extraction - strip HTML tags
            let re = regex::Regex::new(r"<[^>]+>").ok()?;
            let text = re.replace_all(html, "").to_string();
            Some(text.trim().to_string())
        }
        CssExtractMode::Attr(attr_name) => {
            // Extract attribute value
            let pattern = format!(r#"{}="([^"]*)"#, regex::escape(attr_name));
            let re = regex::Regex::new(&pattern).ok()?;
            re.captures(html)
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_string())
        }
    }
}

// Apply transformation to extracted value
fn apply_transform(value: Option<&str>, transform: &str) -> Option<String> {
    let v = value?;

    match transform {
        "trim" => Some(v.trim().to_string()),
        "parse_price" => {
            // Extract numeric value from price string like "€12.99" or "12,99 €"
            let mut result = String::new();
            let mut has_decimal = false;
            for c in v.chars() {
                if c.is_ascii_digit() {
                    result.push(c);
                } else if (c == '.' || c == ',') && !has_decimal {
                    result.push('.');
                    has_decimal = true;
                }
            }
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
        "lowercase" => Some(v.to_lowercase()),
        "uppercase" => Some(v.to_uppercase()),
        _ => Some(v.to_string()),
    }
}
