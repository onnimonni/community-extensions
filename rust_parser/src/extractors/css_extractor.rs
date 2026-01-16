//! CSS selector-based extraction
//!
//! Uses the scraper crate to select elements by CSS selectors.

use scraper::{Html, Selector};

/// Extract elements matching a CSS selector
/// Returns outer HTML of matching elements
pub fn extract_css(html: &str, selector_str: &str) -> Vec<String> {
    let document = Html::parse_document(html);

    let selector = match Selector::parse(selector_str) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    document
        .select(&selector)
        .map(|el| el.html())
        .collect()
}

/// Extract text content from elements matching a CSS selector
pub fn extract_css_text(html: &str, selector_str: &str) -> Vec<String> {
    let document = Html::parse_document(html);

    let selector = match Selector::parse(selector_str) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    document
        .select(&selector)
        .map(|el| el.text().collect::<String>().trim().to_string())
        .collect()
}

/// Extract attribute value from elements matching a CSS selector
pub fn extract_css_attr(html: &str, selector_str: &str, attr_name: &str) -> Vec<String> {
    let document = Html::parse_document(html);

    let selector = match Selector::parse(selector_str) {
        Ok(s) => s,
        Err(_) => return vec![],
    };

    document
        .select(&selector)
        .filter_map(|el| el.value().attr(attr_name).map(String::from))
        .collect()
}

/// Extract first matching element's text
pub fn extract_css_first_text(html: &str, selector_str: &str) -> Option<String> {
    let document = Html::parse_document(html);

    let selector = Selector::parse(selector_str).ok()?;

    document
        .select(&selector)
        .next()
        .map(|el| el.text().collect::<String>().trim().to_string())
}

/// Extract first matching element's attribute
pub fn extract_css_first_attr(html: &str, selector_str: &str, attr_name: &str) -> Option<String> {
    let document = Html::parse_document(html);

    let selector = Selector::parse(selector_str).ok()?;

    document
        .select(&selector)
        .next()
        .and_then(|el| el.value().attr(attr_name).map(String::from))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_css_extract() {
        let html = r#"
        <html>
        <body>
            <div class="price">$19.99</div>
            <div class="price">$29.99</div>
            <a href="/product/123" class="link">Product</a>
        </body>
        </html>
        "#;

        let prices = extract_css_text(html, ".price");
        assert_eq!(prices.len(), 2);
        assert_eq!(prices[0], "$19.99");

        let first_price = extract_css_first_text(html, ".price");
        assert_eq!(first_price.unwrap(), "$19.99");

        let href = extract_css_first_attr(html, ".link", "href");
        assert_eq!(href.unwrap(), "/product/123");
    }

    #[test]
    fn test_complex_selectors() {
        let html = r#"
        <div class="product">
            <span class="name">Product A</span>
            <span class="unit-price">€1.50/kg</span>
        </div>
        "#;

        let unit_price = extract_css_first_text(html, "div.product .unit-price");
        assert_eq!(unit_price.unwrap(), "€1.50/kg");
    }
}
