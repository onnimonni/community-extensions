#include "structured_data.hpp"

namespace duckdb {

StructuredDataResult ExtractStructuredData(const std::string &html, const ExtractionConfig &config) {
	StructuredDataResult result;

	if (html.empty()) {
		return result;
	}

	// Extract JSON-LD
	if (config.extract_jsonld) {
		auto jsonld_result = ExtractJsonLd(html);
		if (jsonld_result.found) {
			result.jsonld = jsonld_result.as_json;
			result.found = true;
		}
	}

	// Extract OpenGraph
	if (config.extract_opengraph) {
		auto og_result = ExtractOpenGraph(html);
		if (og_result.found) {
			result.opengraph = og_result.as_json;
			result.found = true;
		}
	}

	// Extract Meta tags
	if (config.extract_meta) {
		auto meta_result = ExtractMetaTags(html);
		if (meta_result.found) {
			result.meta = meta_result.as_json;
			result.found = true;
		}
	}

	// Extract Hydration data
	if (config.extract_hydration) {
		auto hydration_result = ExtractHydration(html);
		if (hydration_result.found) {
			result.hydration = hydration_result.as_json;
			result.found = true;
		}
	}

	// Extract JavaScript variables
	if (config.extract_js) {
		auto js_result = ExtractJsVariables(html);
		if (js_result.found) {
			result.js = js_result.as_json;
			result.found = true;
		}
	}

	return result;
}

std::string ExtractJsonLdJson(const std::string &html) {
	return ExtractJsonLdAsJson(html);
}

std::string ExtractOpenGraphJson(const std::string &html) {
	return ExtractOpenGraphAsJson(html);
}

std::string ExtractMetaJson(const std::string &html) {
	auto result = ExtractMetaTags(html);
	return result.as_json;
}

std::string ExtractHydrationJson(const std::string &html) {
	return ExtractHydrationAsJson(html);
}

std::string ExtractJsJson(const std::string &html) {
	return ExtractJsVariablesAsJson(html);
}

} // namespace duckdb
