//! OpenGraph meta tags extraction
//!
//! Extracts og: prefixed meta tags and Twitter Card meta tags.

use scraper::{Html, Selector};
use serde_json::{Map, Value};

/// Extract OpenGraph and Twitter Card meta tags
pub fn extract_opengraph(html: &str) -> Value {
    let document = Html::parse_document(html);

    let mut og: Map<String, Value> = Map::new();
    let mut twitter: Map<String, Value> = Map::new();
    let mut meta: Map<String, Value> = Map::new();

    // Select all meta tags
    let selector = match Selector::parse("meta") {
        Ok(s) => s,
        Err(_) => return Value::Object(Map::new()),
    };

    for element in document.select(&selector) {
        let property = element.value().attr("property");
        let name = element.value().attr("name");
        let content = element.value().attr("content").unwrap_or("");

        if content.is_empty() {
            continue;
        }

        // OpenGraph (og:*)
        if let Some(prop) = property {
            if prop.starts_with("og:") {
                let key = prop.strip_prefix("og:").unwrap();
                insert_value(&mut og, key, content);
            } else if prop.starts_with("article:") || prop.starts_with("product:") {
                // Also include article: and product: namespaces
                insert_value(&mut og, prop, content);
            }
        }

        // Twitter Card (twitter:*)
        if let Some(n) = name {
            if n.starts_with("twitter:") {
                let key = n.strip_prefix("twitter:").unwrap();
                insert_value(&mut twitter, key, content);
            }
        }

        // Standard meta tags (description, keywords, author, etc.)
        if let Some(n) = name {
            match n {
                "description" | "keywords" | "author" | "robots" | "viewport" | "generator"
                | "theme-color" | "canonical" => {
                    meta.insert(n.to_string(), Value::String(content.to_string()));
                }
                _ => {}
            }
        }
    }

    let mut result: Map<String, Value> = Map::new();

    if !og.is_empty() {
        result.insert("og".to_string(), Value::Object(og));
    }
    if !twitter.is_empty() {
        result.insert("twitter".to_string(), Value::Object(twitter));
    }
    if !meta.is_empty() {
        result.insert("meta".to_string(), Value::Object(meta));
    }

    Value::Object(result)
}

fn insert_value(map: &mut Map<String, Value>, key: &str, value: &str) {
    // Handle structured keys like "image:width" -> { "image": { "width": "..." } }
    if let Some(colon_pos) = key.find(':') {
        let main_key = &key[..colon_pos];
        let sub_key = &key[colon_pos + 1..];

        // Get or create nested object
        let nested = map
            .entry(main_key.to_string())
            .or_insert_with(|| Value::Object(Map::new()));

        if let Value::Object(nested_map) = nested {
            nested_map.insert(sub_key.to_string(), Value::String(value.to_string()));
        } else if let Value::String(existing) = nested {
            // Convert string to object with "_value" key
            let mut new_obj = Map::new();
            new_obj.insert("_value".to_string(), Value::String(existing.clone()));
            new_obj.insert(sub_key.to_string(), Value::String(value.to_string()));
            *nested = Value::Object(new_obj);
        }
    } else {
        // Simple key
        if map.contains_key(key) {
            // Handle arrays (multiple values for same key, e.g., og:image)
            let existing = map.get_mut(key).unwrap();
            match existing {
                Value::Array(arr) => {
                    arr.push(Value::String(value.to_string()));
                }
                _ => {
                    let old = existing.clone();
                    *existing = Value::Array(vec![old, Value::String(value.to_string())]);
                }
            }
        } else {
            map.insert(key.to_string(), Value::String(value.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_opengraph() {
        let html = r#"
        <html>
        <head>
            <meta property="og:title" content="Test Page">
            <meta property="og:description" content="A test description">
            <meta property="og:image" content="https://example.com/image.jpg">
            <meta property="og:image:width" content="1200">
            <meta name="twitter:card" content="summary_large_image">
            <meta name="description" content="Page description">
        </head>
        </html>
        "#;

        let result = extract_opengraph(html);

        assert!(result.get("og").is_some());
        assert_eq!(result["og"]["title"].as_str().unwrap(), "Test Page");

        assert!(result.get("twitter").is_some());
        assert_eq!(
            result["twitter"]["card"].as_str().unwrap(),
            "summary_large_image"
        );

        assert!(result.get("meta").is_some());
        assert_eq!(
            result["meta"]["description"].as_str().unwrap(),
            "Page description"
        );
    }
}
