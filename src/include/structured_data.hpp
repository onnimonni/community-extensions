#pragma once

#include <string>
#include "jsonld_extractor.hpp"
#include "opengraph_extractor.hpp"
#include "hydration_extractor.hpp"
#include "js_variables_extractor.hpp"

namespace duckdb {

// Combined result of all structured data extraction
struct StructuredDataResult {
	// JSON-LD data (keyed by @type)
	std::string jsonld;

	// OpenGraph data
	std::string opengraph;

	// Meta tags (description, keywords, etc.)
	std::string meta;

	// Hydration data (Next.js, Nuxt, etc.)
	std::string hydration;

	// JavaScript variables (var/let/const/window.X = {...})
	std::string js;

	// Whether any structured data was found
	bool found = false;
};

// Configuration for what to extract
struct ExtractionConfig {
	bool extract_jsonld = true;
	bool extract_opengraph = true;
	bool extract_meta = true;
	bool extract_hydration = true;
	bool extract_js = true;
};

// Extract all structured data from HTML
StructuredDataResult ExtractStructuredData(const std::string &html, const ExtractionConfig &config = {});

// Convenience functions for individual extraction
std::string ExtractJsonLdJson(const std::string &html);
std::string ExtractOpenGraphJson(const std::string &html);
std::string ExtractMetaJson(const std::string &html);
std::string ExtractHydrationJson(const std::string &html);
std::string ExtractJsJson(const std::string &html);

} // namespace duckdb
