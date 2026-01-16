#pragma once

#include <string>
#include <unordered_map>

namespace duckdb {

// Result of hydration data extraction
struct HydrationResult {
	// Map of variable name -> JSON content
	// e.g., {"__NEXT_DATA__": {...}, "__NUXT__": {...}}
	std::unordered_map<std::string, std::string> data;

	// Combined JSON: {"__NEXT_DATA__": {...}, "__NUXT__": {...}}
	std::string as_json;

	// Whether any hydration data was found
	bool found = false;
};

// Extract hydration data from HTML
// Looks for common patterns:
// - <script id="__NEXT_DATA__" type="application/json">
// - window.__NUXT__ = {...}
// - window.__INITIAL_STATE__ = {...}
// - window.__PRELOADED_STATE__ = {...}
// - __DATA__ = {...}
HydrationResult ExtractHydration(const std::string &html);

// Extract and return as JSON string
std::string ExtractHydrationAsJson(const std::string &html);

} // namespace duckdb
