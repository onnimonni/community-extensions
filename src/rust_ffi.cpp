// This file is only compiled when RUST_PARSER_AVAILABLE is defined
#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE

#include "rust_ffi.hpp"
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"
#include <sstream>

namespace duckdb {

using namespace duckdb_yyjson;

// Forward declaration
static void ParseExtractExpression(const std::string &expr, std::string &source,
                                    std::string &path);

// Build JSON request for Rust extraction
static std::string BuildExtractionRequest(const std::vector<ExtractSpec> &specs) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    if (!doc) {
        return "{}";
    }

    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *specs_arr = yyjson_mut_arr(doc);

    for (const auto &spec : specs) {
        yyjson_mut_val *spec_obj = yyjson_mut_obj(doc);

        // Parse expression to determine source and path
        // Format: source.path.to.value or source->'path'->>'value'
        std::string source, path;
        ParseExtractExpression(spec.expression, source, path);

        yyjson_mut_obj_add_strcpy(doc, spec_obj, "source", source.c_str());
        yyjson_mut_obj_add_strcpy(doc, spec_obj, "path", path.c_str());
        yyjson_mut_obj_add_strcpy(doc, spec_obj, "alias", spec.alias.c_str());

        // Add type and transform if present (from enhanced ExtractSpec)
        // For now, these are empty strings

        yyjson_mut_arr_append(specs_arr, spec_obj);
    }

    yyjson_mut_obj_add_val(doc, root, "specs", specs_arr);

    size_t len = 0;
    char *json_str = yyjson_mut_write(doc, 0, &len);
    yyjson_mut_doc_free(doc);

    if (!json_str) {
        return "{}";
    }

    std::string result(json_str, len);
    free(json_str);
    return result;
}

// Parse expression like "jsonld.Product.name" or "jsonld->'Product'->>'name'"
// Returns source (jsonld, microdata, etc.) and path (Product.name)
static void ParseExtractExpression(const std::string &expr, std::string &source,
                                    std::string &path) {
    // Check for dot notation: jsonld.Product.name
    size_t dot_pos = expr.find('.');
    if (dot_pos != std::string::npos && expr.find("->") == std::string::npos) {
        source = expr.substr(0, dot_pos);
        path = expr.substr(dot_pos + 1);
        return;
    }

    // Check for arrow notation: jsonld->'Product'->>'name'
    size_t arrow_pos = expr.find("->");
    if (arrow_pos != std::string::npos) {
        source = expr.substr(0, arrow_pos);

        // Convert arrow notation to dot notation
        std::string remainder = expr.substr(arrow_pos);
        std::ostringstream path_builder;
        bool first = true;

        size_t pos = 0;
        while (pos < remainder.length()) {
            // Find next arrow
            size_t next_arrow = remainder.find("->", pos);
            if (next_arrow == std::string::npos) break;

            // Check if it's ->> or ->
            bool is_text = (next_arrow + 2 < remainder.length() &&
                           remainder[next_arrow + 2] == '>');
            size_t key_start = is_text ? next_arrow + 3 : next_arrow + 2;

            // Skip whitespace
            while (key_start < remainder.length() &&
                   std::isspace(remainder[key_start])) {
                key_start++;
            }

            // Extract key (quoted or unquoted)
            std::string key;
            if (key_start < remainder.length() && remainder[key_start] == '\'') {
                // Quoted key
                size_t key_end = remainder.find('\'', key_start + 1);
                if (key_end != std::string::npos) {
                    key = remainder.substr(key_start + 1, key_end - key_start - 1);
                    pos = key_end + 1;
                } else {
                    break;
                }
            } else {
                // Unquoted key (until next -> or end)
                size_t key_end = remainder.find("->", key_start);
                if (key_end == std::string::npos) {
                    key = remainder.substr(key_start);
                    pos = remainder.length();
                } else {
                    key = remainder.substr(key_start, key_end - key_start);
                    pos = key_end;
                }
                // Trim whitespace
                while (!key.empty() && std::isspace(key.back())) {
                    key.pop_back();
                }
            }

            if (!key.empty()) {
                if (!first) {
                    path_builder << ".";
                }
                path_builder << key;
                first = false;
            }
        }

        path = path_builder.str();
        return;
    }

    // CSS selector: css 'selector' or just the selector if source is "css"
    if (StringUtil::StartsWith(StringUtil::Lower(expr), "css ")) {
        source = "css";
        path = expr.substr(4);
        // Remove quotes if present
        if (path.length() >= 2 && path.front() == '\'' && path.back() == '\'') {
            path = path.substr(1, path.length() - 2);
        }
        return;
    }

    // Default: treat whole expression as path with empty source
    source = expr;
    path = "";
}

std::vector<std::string> ExtractWithRust(const std::string &html,
                                          const std::vector<ExtractSpec> &specs) {
    std::vector<std::string> results;

    if (specs.empty() || html.empty()) {
        return results;
    }

    // Build request JSON
    std::string request_json = BuildExtractionRequest(specs);

    // Call Rust FFI
    auto ffi_result = extract_from_html(html.c_str(), html.length(), request_json.c_str());
    RustExtractionResult result(ffi_result);

    if (result.HasError()) {
        // Return empty strings for each spec on error
        results.resize(specs.size(), "");
        return results;
    }

    // Parse result JSON
    std::string json = result.GetJson();
    if (json.empty()) {
        results.resize(specs.size(), "");
        return results;
    }

    yyjson_doc *doc = yyjson_read(json.c_str(), json.length(), 0);
    if (!doc) {
        results.resize(specs.size(), "");
        return results;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *values_arr = yyjson_obj_get(root, "values");

    if (values_arr && yyjson_is_arr(values_arr)) {
        size_t idx, max;
        yyjson_val *item;
        yyjson_arr_foreach(values_arr, idx, max, item) {
            yyjson_val *value_val = yyjson_obj_get(item, "value");
            if (value_val && !yyjson_is_null(value_val)) {
                if (yyjson_is_str(value_val)) {
                    results.push_back(yyjson_get_str(value_val));
                } else {
                    // Serialize non-string values
                    size_t len = 0;
                    char *str = yyjson_val_write(value_val, 0, &len);
                    if (str) {
                        results.push_back(std::string(str, len));
                        free(str);
                    } else {
                        results.push_back("");
                    }
                }
            } else {
                results.push_back("");
            }
        }
    }

    yyjson_doc_free(doc);

    // Ensure we have the right number of results
    while (results.size() < specs.size()) {
        results.push_back("");
    }

    return results;
}

std::string ExtractJsonLdWithRust(const std::string &html) {
    if (html.empty()) return "{}";

    auto ffi_result = extract_jsonld_ffi(html.c_str(), html.length());
    RustExtractionResult result(ffi_result);

    if (result.HasError()) {
        return "{}";
    }

    return result.GetJson();
}

std::string ExtractMicrodataWithRust(const std::string &html) {
    if (html.empty()) return "{}";

    auto ffi_result = extract_microdata_ffi(html.c_str(), html.length());
    RustExtractionResult result(ffi_result);

    if (result.HasError()) {
        return "{}";
    }

    return result.GetJson();
}

std::string ExtractOpenGraphWithRust(const std::string &html) {
    if (html.empty()) return "{}";

    auto ffi_result = extract_opengraph_ffi(html.c_str(), html.length());
    RustExtractionResult result(ffi_result);

    if (result.HasError()) {
        return "{}";
    }

    return result.GetJson();
}

std::string ExtractJsWithRust(const std::string &html) {
    if (html.empty()) return "{}";

    auto ffi_result = extract_js_ffi(html.c_str(), html.length());
    RustExtractionResult result(ffi_result);

    if (result.HasError()) {
        return "{}";
    }

    return result.GetJson();
}

std::string ExtractCssWithRust(const std::string &html, const std::string &selector) {
    if (html.empty() || selector.empty()) return "[]";

    auto ffi_result = extract_css_ffi(html.c_str(), html.length(), selector.c_str());
    RustExtractionResult result(ffi_result);

    if (result.HasError()) {
        return "[]";
    }

    return result.GetJson();
}

} // namespace duckdb

#endif // RUST_PARSER_AVAILABLE
