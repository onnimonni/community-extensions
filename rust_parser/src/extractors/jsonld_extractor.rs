//! JSON-LD extraction from HTML
//!
//! Extracts JSON-LD data from <script type="application/ld+json"> tags.
//! Supports @graph arrays and multiple JSON-LD blocks.

use scraper::{Html, Selector};
use serde_json::{Map, Value};

/// Extract JSON-LD data from HTML, keyed by @type
pub fn extract_jsonld(html: &str) -> Value {
    let document = Html::parse_document(html);

    // Select all JSON-LD script tags
    let selector = match Selector::parse(r#"script[type="application/ld+json"]"#) {
        Ok(s) => s,
        Err(_) => return Value::Object(Map::new()),
    };

    let mut result: Map<String, Value> = Map::new();

    for element in document.select(&selector) {
        let content = element.inner_html();
        let trimmed = content.trim();

        if trimmed.is_empty() {
            continue;
        }

        // Parse JSON
        if let Ok(json) = serde_json::from_str::<Value>(trimmed) {
            process_jsonld_value(&json, &mut result);
        }
    }

    Value::Object(result)
}

fn process_jsonld_value(value: &Value, result: &mut Map<String, Value>) {
    match value {
        Value::Array(arr) => {
            // Array of JSON-LD objects
            for item in arr {
                process_jsonld_value(item, result);
            }
        }
        Value::Object(obj) => {
            // Check for @graph
            if let Some(graph) = obj.get("@graph") {
                if let Value::Array(graph_items) = graph {
                    for item in graph_items {
                        process_jsonld_object(item, result);
                    }
                }
            } else {
                process_jsonld_object(value, result);
            }
        }
        _ => {}
    }
}

fn process_jsonld_object(value: &Value, result: &mut Map<String, Value>) {
    if let Value::Object(obj) = value {
        // Get @type
        let type_key = if let Some(type_val) = obj.get("@type") {
            match type_val {
                Value::String(s) => Some(s.clone()),
                Value::Array(arr) => {
                    // Use first type in array
                    arr.first().and_then(|v| v.as_str()).map(String::from)
                }
                _ => None,
            }
        } else {
            None
        };

        if let Some(type_name) = type_key {
            // Store by type - if multiple objects of same type, create array
            if let Some(existing) = result.get_mut(&type_name) {
                match existing {
                    Value::Array(arr) => {
                        arr.push(value.clone());
                    }
                    _ => {
                        let old = existing.clone();
                        *existing = Value::Array(vec![old, value.clone()]);
                    }
                }
            } else {
                result.insert(type_name, value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_jsonld() {
        let html = r#"
        <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "https://schema.org",
                "@type": "Product",
                "name": "Test Product",
                "price": "19.99"
            }
            </script>
        </head>
        </html>
        "#;

        let result = extract_jsonld(html);
        assert!(result.get("Product").is_some());
        assert_eq!(
            result["Product"]["name"].as_str().unwrap(),
            "Test Product"
        );
    }

    #[test]
    fn test_extract_graph_jsonld() {
        let html = r#"
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@graph": [
                {"@type": "Product", "name": "Product 1"},
                {"@type": "Organization", "name": "Org 1"}
            ]
        }
        </script>
        "#;

        let result = extract_jsonld(html);
        assert!(result.get("Product").is_some());
        assert!(result.get("Organization").is_some());
    }
}
