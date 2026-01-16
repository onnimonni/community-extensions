#pragma once

#include <string>
#include <unordered_map>

namespace duckdb {

// Result of JavaScript variables extraction
struct JsVariablesResult {
	// Map of variable name -> JSON value
	std::unordered_map<std::string, std::string> variables;

	// Combined JSON: {"varName": {...}, "otherVar": [...], ...}
	std::string as_json;

	// Whether any variables were found
	bool found = false;
};

// Extract top-level JavaScript variable assignments from HTML
// Looks for patterns like:
//   var name = {...};
//   let name = {...};
//   const name = {...};
//   window.name = {...};
// Only extracts variables with JSON object/array values
JsVariablesResult ExtractJsVariables(const std::string &html);

// Extract and return as JSON string
std::string ExtractJsVariablesAsJson(const std::string &html);

} // namespace duckdb
