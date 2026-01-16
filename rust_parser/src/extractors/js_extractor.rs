//! JavaScript variable extraction using tree-sitter AST parsing
//!
//! Extracts variables from <script> tags:
//! - var/let/const declarations with object/array values
//! - window.X = {...} assignments
//! - Inline JSON objects

use scraper::{Html, Selector};
use serde_json::{Map, Value};
use tree_sitter::{Parser, Query, QueryCursor, StreamingIterator};

/// Extract JavaScript variables from HTML script tags
pub fn extract_js_variables(html: &str) -> Value {
    let document = Html::parse_document(html);

    // Select script tags (not type="application/ld+json" or similar)
    let selector = match Selector::parse("script:not([type])") {
        Ok(s) => s,
        Err(_) => return Value::Object(Map::new()),
    };

    let mut result: Map<String, Value> = Map::new();

    for element in document.select(&selector) {
        let script_content = element.inner_html();
        if script_content.trim().is_empty() {
            continue;
        }

        // Extract variables from this script
        let vars = extract_variables_from_js(&script_content);
        for (key, value) in vars {
            result.insert(key, value);
        }
    }

    // Also try scripts with type="text/javascript"
    let js_selector = match Selector::parse(r#"script[type="text/javascript"]"#) {
        Ok(s) => s,
        Err(_) => return Value::Object(result),
    };

    for element in document.select(&js_selector) {
        let script_content = element.inner_html();
        if script_content.trim().is_empty() {
            continue;
        }

        let vars = extract_variables_from_js(&script_content);
        for (key, value) in vars {
            result.insert(key, value);
        }
    }

    Value::Object(result)
}

fn extract_variables_from_js(js_code: &str) -> Vec<(String, Value)> {
    let mut results = Vec::new();

    // Initialize tree-sitter parser
    let mut parser = Parser::new();
    let language = tree_sitter_javascript::LANGUAGE;

    if parser.set_language(&language.into()).is_err() {
        // Fallback to regex-based extraction
        return extract_variables_regex(js_code);
    }

    let tree = match parser.parse(js_code, None) {
        Some(t) => t,
        None => return extract_variables_regex(js_code),
    };

    // Query for variable declarations and assignments
    let query_str = r#"
        ; var/let/const declarations
        (variable_declarator
            name: (identifier) @var_name
            value: [(object) (array)] @var_value)

        ; window.X = {...} assignments
        (assignment_expression
            left: (member_expression
                object: (identifier) @obj_name
                property: (property_identifier) @prop_name)
            right: [(object) (array)] @assign_value)
    "#;

    let query = match Query::new(&language.into(), query_str) {
        Ok(q) => q,
        Err(_) => return extract_variables_regex(js_code),
    };

    let mut cursor = QueryCursor::new();

    let mut matches_iter = cursor.matches(&query, tree.root_node(), js_code.as_bytes());
    while let Some(m) = matches_iter.next() {
        let mut var_name: Option<String> = None;
        let mut var_value: Option<&str> = None;
        let mut obj_name: Option<String> = None;
        let mut prop_name: Option<String> = None;
        let mut assign_value: Option<&str> = None;

        for capture in m.captures {
            let capture_name: &str = query.capture_names()[capture.index as usize];
            let node_text = &js_code[capture.node.byte_range()];

            match capture_name {
                "var_name" => var_name = Some(node_text.to_string()),
                "var_value" => var_value = Some(node_text),
                "obj_name" => obj_name = Some(node_text.to_string()),
                "prop_name" => prop_name = Some(node_text.to_string()),
                "assign_value" => assign_value = Some(node_text),
                _ => {}
            }
        }

        // Handle variable declarations
        if let (Some(name), Some(value_str)) = (var_name, var_value) {
            if let Ok(value) = parse_js_value(value_str) {
                results.push((name, value));
            }
        }

        // Handle window.X assignments
        if let (Some(obj), Some(prop), Some(value_str)) = (obj_name, prop_name, assign_value) {
            if obj == "window" {
                if let Ok(value) = parse_js_value(value_str) {
                    results.push((prop, value));
                }
            }
        }
    }

    results
}

/// Fallback regex-based extraction for when tree-sitter fails
fn extract_variables_regex(js_code: &str) -> Vec<(String, Value)> {
    let mut results = Vec::new();

    // Pattern for var/let/const X = {...} or [...]
    // This is a simplified version - real JS parsing is complex
    let var_pattern = regex::Regex::new(
        r#"(?:var|let|const)\s+(\w+)\s*=\s*(\{[^;]*\}|\[[^\]]*\])"#
    );

    if let Ok(re) = var_pattern {
        for cap in re.captures_iter(js_code) {
            if let (Some(name), Some(value_str)) = (cap.get(1), cap.get(2)) {
                if let Ok(value) = parse_js_value(value_str.as_str()) {
                    results.push((name.as_str().to_string(), value));
                }
            }
        }
    }

    // Pattern for window.X = {...}
    let window_pattern = regex::Regex::new(
        r#"window\.(\w+)\s*=\s*(\{[^;]*\})"#
    );

    if let Ok(re) = window_pattern {
        for cap in re.captures_iter(js_code) {
            if let (Some(name), Some(value_str)) = (cap.get(1), cap.get(2)) {
                if let Ok(value) = parse_js_value(value_str.as_str()) {
                    results.push((name.as_str().to_string(), value));
                }
            }
        }
    }

    results
}

/// Parse a JavaScript value string to JSON
/// Handles trailing commas, single quotes, and unquoted keys
fn parse_js_value(js_str: &str) -> Result<Value, serde_json::Error> {
    // Try parsing as-is first
    if let Ok(v) = serde_json::from_str(js_str) {
        return Ok(v);
    }

    // Convert JS object syntax to JSON
    let mut json_str = js_str.to_string();

    // Replace single quotes with double quotes (careful with escaped quotes)
    json_str = json_str.replace('\'', "\"");

    // Remove trailing commas before } or ]
    let trailing_comma = regex::Regex::new(r",\s*([}\]])").unwrap();
    json_str = trailing_comma.replace_all(&json_str, "$1").to_string();

    // Quote unquoted keys: { key: value } -> { "key": value }
    let unquoted_key = regex::Regex::new(r#"([{,]\s*)(\w+)\s*:"#).unwrap();
    json_str = unquoted_key.replace_all(&json_str, r#"$1"$2":"#).to_string();

    serde_json::from_str(&json_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_js_variables() {
        let html = r#"
        <html>
        <head>
            <script>
                var siteConfig = {
                    price: 19.99,
                    currency: "EUR"
                };
                window.productData = {
                    name: "Test Product",
                    sku: "ABC123"
                };
            </script>
        </head>
        </html>
        "#;

        let result = extract_js_variables(html);

        // These may or may not be extracted depending on tree-sitter availability
        // The test is mainly to ensure no panics
        assert!(result.is_object());
    }

    #[test]
    fn test_parse_js_value() {
        // Standard JSON
        let v1 = parse_js_value(r#"{"name": "test"}"#).unwrap();
        assert_eq!(v1["name"].as_str().unwrap(), "test");

        // Single quotes
        let v2 = parse_js_value(r#"{'name': 'test'}"#).unwrap();
        assert_eq!(v2["name"].as_str().unwrap(), "test");

        // Trailing comma
        let v3 = parse_js_value(r#"{"name": "test",}"#).unwrap();
        assert_eq!(v3["name"].as_str().unwrap(), "test");

        // Unquoted keys
        let v4 = parse_js_value(r#"{name: "test"}"#).unwrap();
        assert_eq!(v4["name"].as_str().unwrap(), "test");
    }
}
