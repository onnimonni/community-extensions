//! Microdata (schema.org HTML attributes) extraction
//!
//! Extracts microdata from itemscope/itemprop/itemtype attributes.
//! Reference: https://html.spec.whatwg.org/multipage/microdata.html

use scraper::{Html, Selector};
use serde_json::{Map, Value};

/// Extract microdata from HTML, keyed by itemtype
pub fn extract_microdata(html: &str) -> Value {
    let document = Html::parse_document(html);

    // Find all elements with itemscope (top-level microdata items)
    let selector = match Selector::parse("[itemscope]") {
        Ok(s) => s,
        Err(_) => return Value::Object(Map::new()),
    };

    let mut result: Map<String, Value> = Map::new();

    for element in document.select(&selector) {
        // Skip nested itemscope elements (they'll be processed as part of parent)
        // Check if any ancestor also has itemscope
        let is_nested = element
            .ancestors()
            .filter_map(|n| n.value().as_element())
            .any(|el| el.attr("itemscope").is_some());

        if is_nested {
            continue;
        }

        let item = extract_item(&element);
        if let Some(type_name) = item.get("@type").and_then(|v| v.as_str()) {
            let type_key = type_name.to_string();

            // Store by type
            if let Some(existing) = result.get_mut(&type_key) {
                match existing {
                    Value::Array(arr) => {
                        arr.push(item);
                    }
                    _ => {
                        let old = existing.clone();
                        *existing = Value::Array(vec![old, item]);
                    }
                }
            } else {
                result.insert(type_key, item);
            }
        }
    }

    Value::Object(result)
}

fn extract_item(element: &scraper::ElementRef) -> Value {
    let mut item: Map<String, Value> = Map::new();

    // Get itemtype
    if let Some(itemtype) = element.value().attr("itemtype") {
        // Extract type name from URL like "https://schema.org/Product"
        let type_name = itemtype
            .rsplit('/')
            .next()
            .unwrap_or(itemtype);
        item.insert("@type".to_string(), Value::String(type_name.to_string()));
    }

    // Get itemid if present
    if let Some(itemid) = element.value().attr("itemid") {
        item.insert("@id".to_string(), Value::String(itemid.to_string()));
    }

    // Find all itemprop elements within this scope
    let prop_selector = match Selector::parse("[itemprop]") {
        Ok(s) => s,
        Err(_) => return Value::Object(item),
    };

    for prop_element in element.select(&prop_selector) {
        // Skip if this property belongs to a nested itemscope
        let mut found_itemscope = false;
        let mut current = prop_element.parent();

        while let Some(parent_node) = current {
            if let Some(parent_elem) = parent_node.value().as_element() {
                // If we hit the original element, we're good
                if parent_elem.id() == element.value().id() {
                    break;
                }
                // If we hit another itemscope first, this prop belongs to it
                if parent_elem.attr("itemscope").is_some() {
                    found_itemscope = true;
                    break;
                }
            }
            current = parent_node.parent();
        }

        if found_itemscope {
            continue;
        }

        // Get property name
        let prop_name = match prop_element.value().attr("itemprop") {
            Some(name) => name.to_string(),
            None => continue,
        };

        // Get property value
        let prop_value = if prop_element.value().attr("itemscope").is_some() {
            // Nested item
            extract_item(&prop_element)
        } else {
            // Scalar value - depends on element type
            let tag = prop_element.value().name();
            let value_string: String = match tag {
                "meta" => prop_element.value().attr("content").unwrap_or("").to_string(),
                "link" | "a" | "area" => prop_element.value().attr("href").unwrap_or("").to_string(),
                "img" | "audio" | "video" | "source" => {
                    prop_element.value().attr("src").unwrap_or("").to_string()
                }
                "time" => {
                    prop_element
                        .value()
                        .attr("datetime")
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| prop_element.text().collect::<String>())
                }
                "data" | "meter" => prop_element.value().attr("value").unwrap_or("").to_string(),
                _ => {
                    // Use text content
                    prop_element.text().collect::<String>()
                }
            };
            Value::String(value_string.trim().to_string())
        };

        // Handle multiple values for same property
        if let Some(existing) = item.get_mut(&prop_name) {
            match existing {
                Value::Array(arr) => {
                    arr.push(prop_value);
                }
                _ => {
                    let old = existing.clone();
                    *existing = Value::Array(vec![old, prop_value]);
                }
            }
        } else {
            item.insert(prop_name, prop_value);
        }
    }

    Value::Object(item)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_simple_microdata() {
        let html = r#"
        <div itemscope itemtype="https://schema.org/Product">
            <span itemprop="name">Test Product</span>
            <meta itemprop="gtin13" content="1234567890123">
            <span itemprop="price">19.99</span>
        </div>
        "#;

        let result = extract_microdata(html);
        assert!(result.get("Product").is_some());
        assert_eq!(
            result["Product"]["name"].as_str().unwrap(),
            "Test Product"
        );
        assert_eq!(
            result["Product"]["gtin13"].as_str().unwrap(),
            "1234567890123"
        );
    }

    #[test]
    fn test_nested_microdata() {
        let html = r#"
        <div itemscope itemtype="https://schema.org/Product">
            <span itemprop="name">Product</span>
            <div itemprop="offers" itemscope itemtype="https://schema.org/Offer">
                <span itemprop="price">19.99</span>
            </div>
        </div>
        "#;

        let result = extract_microdata(html);
        assert!(result.get("Product").is_some());
        let product = &result["Product"];
        assert!(product.get("offers").is_some());
        assert_eq!(product["offers"]["price"].as_str().unwrap(), "19.99");
    }
}
