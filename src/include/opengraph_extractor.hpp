#pragma once

#include <string>
#include <unordered_map>

namespace duckdb {

// Result of OpenGraph extraction
struct OpenGraphResult {
	// All og:* meta tags
	std::unordered_map<std::string, std::string> properties;

	// Common fields for convenience
	std::string title;
	std::string description;
	std::string image;
	std::string url;
	std::string type;
	std::string site_name;

	// Twitter card data (twitter:*)
	std::unordered_map<std::string, std::string> twitter;

	// As JSON string: {"title": "...", "description": "...", ...}
	std::string as_json;

	// Whether any OpenGraph data was found
	bool found = false;
};

// Extract OpenGraph (og:*) and Twitter Card (twitter:*) meta tags
OpenGraphResult ExtractOpenGraph(const std::string &html);

// Extract and return as JSON string
std::string ExtractOpenGraphAsJson(const std::string &html);

// Also extract standard meta tags (description, keywords, author)
struct MetaTagsResult {
	std::string description;
	std::string keywords;
	std::string author;
	std::string canonical;
	std::string robots;
	std::string as_json;
	bool found = false;
};

MetaTagsResult ExtractMetaTags(const std::string &html);

} // namespace duckdb
