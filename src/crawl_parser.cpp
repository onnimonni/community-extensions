#include "crawl_parser.hpp"
#include "crawler_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// CrawlParseData
//===--------------------------------------------------------------------===//
unique_ptr<ParserExtensionParseData> CrawlParseData::Copy() const {
	auto copy = make_uniq<CrawlParseData>();
	copy->statement_type = statement_type;
	copy->source_query = source_query;
	copy->target_table = target_table;
	copy->user_agent = user_agent;
	copy->default_crawl_delay = default_crawl_delay;
	copy->min_crawl_delay = min_crawl_delay;
	copy->max_crawl_delay = max_crawl_delay;
	copy->timeout_seconds = timeout_seconds;
	copy->respect_robots_txt = respect_robots_txt;
	copy->log_skipped = log_skipped;
	copy->url_filter = url_filter;
	copy->sitemap_cache_hours = sitemap_cache_hours;
	copy->update_stale = update_stale;
	copy->max_retry_backoff_seconds = max_retry_backoff_seconds;
	copy->max_parallel_per_domain = max_parallel_per_domain;
	copy->max_total_connections = max_total_connections;
	copy->max_response_bytes = max_response_bytes;
	copy->compress = compress;
	copy->accept_content_types = accept_content_types;
	copy->reject_content_types = reject_content_types;
	copy->follow_links = follow_links;
	copy->allow_subdomains = allow_subdomains;
	copy->max_crawl_pages = max_crawl_pages;
	copy->max_crawl_depth = max_crawl_depth;
	copy->respect_nofollow = respect_nofollow;
	copy->follow_canonical = follow_canonical;
	copy->num_threads = num_threads;
	copy->extract_js = extract_js;
	copy->extract_specs = extract_specs;
	return copy;
}

string CrawlParseData::ToString() const {
	return "CRAWL (" + source_query + ") INTO " + target_table;
}

//===--------------------------------------------------------------------===//
// Parser helpers
//===--------------------------------------------------------------------===//
static string Trim(const string &str) {
	size_t start = 0;
	size_t end = str.length();
	while (start < end && std::isspace(str[start])) {
		start++;
	}
	while (end > start && std::isspace(str[end - 1])) {
		end--;
	}
	return str.substr(start, end - start);
}

// Find keyword as standalone word (not part of identifier)
// Returns position or string::npos if not found
static size_t FindKeyword(const string &str, const string &keyword) {
	size_t pos = 0;
	while ((pos = str.find(keyword, pos)) != string::npos) {
		// Check if preceded by whitespace or start
		bool valid_start = (pos == 0 || !std::isalnum(str[pos - 1]) && str[pos - 1] != '_');
		// Check if followed by whitespace, '(', or end
		size_t after = pos + keyword.length();
		bool valid_end = (after >= str.length() ||
		                  std::isspace(str[after]) ||
		                  str[after] == '(' ||
		                  (!std::isalnum(str[after]) && str[after] != '_'));
		if (valid_start && valid_end) {
			return pos;
		}
		pos++;
	}
	return string::npos;
}

// Find matching closing parenthesis
static size_t FindClosingParen(const string &str, size_t open_pos) {
	int depth = 1;
	bool in_string = false;
	char string_char = 0;

	for (size_t i = open_pos + 1; i < str.length(); i++) {
		char c = str[i];

		// Handle string literals
		if (!in_string && (c == '\'' || c == '"')) {
			in_string = true;
			string_char = c;
		} else if (in_string && c == string_char) {
			// Check for escaped quote
			if (i + 1 < str.length() && str[i + 1] == string_char) {
				i++; // Skip escaped quote
			} else {
				in_string = false;
			}
		} else if (!in_string) {
			if (c == '(') {
				depth++;
			} else if (c == ')') {
				depth--;
				if (depth == 0) {
					return i;
				}
			}
		}
	}
	return string::npos;
}

// Parse WITH options: WITH (key1 value1, key2 value2, ...)
static bool ParseWithOptions(const string &options_str, CrawlParseData &data) {
	// Remove outer parentheses if present
	string opts = Trim(options_str);
	if (!opts.empty() && opts.front() == '(' && opts.back() == ')') {
		opts = opts.substr(1, opts.length() - 2);
	}

	// Simple parsing: split by comma (handles quoted strings)
	size_t pos = 0;
	while (pos < opts.length()) {
		// Skip whitespace
		while (pos < opts.length() && std::isspace(opts[pos])) {
			pos++;
		}
		if (pos >= opts.length()) {
			break;
		}

		// Find key
		size_t key_start = pos;
		while (pos < opts.length() && !std::isspace(opts[pos]) && opts[pos] != ',') {
			pos++;
		}
		string key = StringUtil::Lower(opts.substr(key_start, pos - key_start));

		// Skip whitespace
		while (pos < opts.length() && std::isspace(opts[pos])) {
			pos++;
		}

		// Find value
		string value;
		if (pos < opts.length() && opts[pos] == '\'') {
			// Quoted string
			pos++; // skip opening quote
			size_t value_start = pos;
			while (pos < opts.length() && opts[pos] != '\'') {
				pos++;
			}
			value = opts.substr(value_start, pos - value_start);
			if (pos < opts.length()) {
				pos++; // skip closing quote
			}
		} else {
			// Unquoted value
			size_t value_start = pos;
			while (pos < opts.length() && !std::isspace(opts[pos]) && opts[pos] != ',') {
				pos++;
			}
			value = opts.substr(value_start, pos - value_start);
		}

		// Skip comma
		while (pos < opts.length() && (std::isspace(opts[pos]) || opts[pos] == ',')) {
			pos++;
		}

		// Apply option
		if (key == "user_agent") {
			data.user_agent = value;
		} else if (key == "default_crawl_delay") {
			data.default_crawl_delay = std::stod(value);
		} else if (key == "min_crawl_delay") {
			data.min_crawl_delay = std::stod(value);
		} else if (key == "max_crawl_delay") {
			data.max_crawl_delay = std::stod(value);
		} else if (key == "timeout_seconds") {
			data.timeout_seconds = std::stoi(value);
		} else if (key == "respect_robots_txt") {
			data.respect_robots_txt = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "log_skipped") {
			data.log_skipped = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "sitemap_cache_hours") {
			data.sitemap_cache_hours = std::stod(value);
		} else if (key == "update_stale") {
			data.update_stale = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "max_retry_backoff_seconds") {
			data.max_retry_backoff_seconds = std::stoi(value);
		} else if (key == "max_parallel_per_domain") {
			data.max_parallel_per_domain = std::stoi(value);
		} else if (key == "max_total_connections") {
			data.max_total_connections = std::stoi(value);
		} else if (key == "max_response_bytes") {
			data.max_response_bytes = std::stoll(value);
		} else if (key == "compress") {
			data.compress = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "accept_content_types") {
			data.accept_content_types = value;
		} else if (key == "reject_content_types") {
			data.reject_content_types = value;
		} else if (key == "follow_links") {
			data.follow_links = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "allow_subdomains") {
			data.allow_subdomains = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "max_crawl_pages") {
			data.max_crawl_pages = std::stoi(value);
		} else if (key == "max_crawl_depth") {
			data.max_crawl_depth = std::stoi(value);
		} else if (key == "respect_nofollow") {
			data.respect_nofollow = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "follow_canonical") {
			data.follow_canonical = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "threads" || key == "num_threads") {
			data.num_threads = std::stoi(value);
		} else if (key == "extract_js") {
			data.extract_js = (StringUtil::Lower(value) == "true" || value == "1");
		}
	}

	return true;
}

// Parse EXTRACT clause: EXTRACT (expr AS alias, expr2 AS alias2, ...)
static bool ParseExtractClause(const string &extract_str, CrawlParseData &data) {
	// Remove outer parentheses
	string content = Trim(extract_str);
	if (!content.empty() && content.front() == '(' && content.back() == ')') {
		content = content.substr(1, content.length() - 2);
	}

	// Split by comma (respecting nested parentheses and quotes)
	vector<string> specs;
	size_t pos = 0;
	size_t start = 0;
	int paren_depth = 0;
	bool in_string = false;
	char string_char = 0;

	while (pos < content.length()) {
		char c = content[pos];

		if (!in_string && (c == '\'' || c == '"')) {
			in_string = true;
			string_char = c;
		} else if (in_string && c == string_char) {
			in_string = false;
		} else if (!in_string) {
			if (c == '(') {
				paren_depth++;
			} else if (c == ')') {
				paren_depth--;
			} else if (c == ',' && paren_depth == 0) {
				specs.push_back(Trim(content.substr(start, pos - start)));
				start = pos + 1;
			}
		}
		pos++;
	}
	// Add last spec
	if (start < content.length()) {
		specs.push_back(Trim(content.substr(start)));
	}

	// Parse each spec: "expr AS alias" or "expr as alias"
	for (const auto &spec_str : specs) {
		if (spec_str.empty()) {
			continue;
		}

		ExtractSpec spec;

		// Find " AS " or " as " (case-insensitive, with spaces)
		string spec_lower = StringUtil::Lower(spec_str);
		size_t as_pos = spec_lower.rfind(" as ");
		if (as_pos == string::npos) {
			// No alias - use last part of expression as alias
			// e.g., jsonld->'Product'->>'name' -> alias is 'name'
			size_t arrow_pos = spec_str.rfind("->>");
			if (arrow_pos == string::npos) {
				arrow_pos = spec_str.rfind("->");
			}
			if (arrow_pos != string::npos) {
				string last_part = spec_str.substr(arrow_pos + (spec_str[arrow_pos + 2] == '>' ? 3 : 2));
				last_part = Trim(last_part);
				// Remove quotes from 'name'
				if (last_part.length() >= 2 && last_part.front() == '\'' && last_part.back() == '\'') {
					last_part = last_part.substr(1, last_part.length() - 2);
				}
				spec.alias = last_part;
			} else {
				// No arrow, use whole expression as alias
				spec.alias = spec_str;
			}
			spec.expression = spec_str;
		} else {
			spec.expression = Trim(spec_str.substr(0, as_pos));
			spec.alias = Trim(spec_str.substr(as_pos + 4));
		}

		// Determine if expression ends with ->> (text) or -> (json)
		spec.is_text = (spec.expression.find("->>") != string::npos &&
		                spec.expression.rfind("->>") > spec.expression.rfind("->'" ));

		data.extract_specs.push_back(spec);
	}

	return !data.extract_specs.empty();
}

// Serialize extract_specs to JSON string for parameter passing
static string SerializeExtractSpecs(const vector<ExtractSpec> &specs) {
	if (specs.empty()) {
		return "[]";
	}

	string json = "[";
	for (size_t i = 0; i < specs.size(); i++) {
		if (i > 0) {
			json += ",";
		}
		// Escape quotes in expression and alias
		string expr = specs[i].expression;
		string alias = specs[i].alias;
		// Simple escape: replace " with \"
		size_t pos = 0;
		while ((pos = expr.find('"', pos)) != string::npos) {
			expr.replace(pos, 1, "\\\"");
			pos += 2;
		}
		pos = 0;
		while ((pos = alias.find('"', pos)) != string::npos) {
			alias.replace(pos, 1, "\\\"");
			pos += 2;
		}

		json += "{\"expr\":\"" + expr + "\",\"alias\":\"" + alias + "\",\"is_text\":" +
		        (specs[i].is_text ? "true" : "false") + "}";
	}
	json += "]";
	return json;
}

//===--------------------------------------------------------------------===//
// CrawlParserExtension
//===--------------------------------------------------------------------===//
CrawlParserExtension::CrawlParserExtension() {
	parse_function = ParseCrawl;
	plan_function = PlanCrawl;
}

ParserExtensionParseResult CrawlParserExtension::ParseCrawl(ParserExtensionInfo *info, const string &query) {
	// Check if query starts with CRAWL (case-insensitive)
	string trimmed = Trim(query);
	string lower = StringUtil::Lower(trimmed);

	if (!StringUtil::StartsWith(lower, "crawl")) {
		// Not a CRAWL statement, let default parser handle it
		return ParserExtensionParseResult();
	}

	auto data = make_uniq<CrawlParseData>();
	data->statement_type = CrawlStatementType::CRAWL;

	// Parse: CRAWL (SELECT ...) INTO table_name [WHERE ...] WITH (options)
	size_t keyword_end = 5; // "crawl" length

	// Find the opening parenthesis after CRAWL
	size_t paren_start = trimmed.find('(', keyword_end);
	if (paren_start == string::npos) {
		return ParserExtensionParseResult("CRAWL syntax error: expected '(' after CRAWL");
	}

	// Find matching closing parenthesis
	size_t paren_end = FindClosingParen(trimmed, paren_start);
	if (paren_end == string::npos) {
		return ParserExtensionParseResult("CRAWL syntax error: unmatched parenthesis");
	}

	// Extract source query
	data->source_query = Trim(trimmed.substr(paren_start + 1, paren_end - paren_start - 1));

	// Find INTO keyword
	string remainder = trimmed.substr(paren_end + 1);
	string remainder_lower = StringUtil::Lower(remainder);
	size_t into_pos = remainder_lower.find("into");
	if (into_pos == string::npos) {
		return ParserExtensionParseResult("CRAWL syntax error: expected INTO after source query");
	}

	// Extract table name (everything after INTO until WHERE, EXTRACT, WITH, or LIMIT)
	string after_into = Trim(remainder.substr(into_pos + 4));
	string after_into_lower = StringUtil::Lower(after_into);

	// Use FindKeyword to avoid matching keywords inside identifiers (e.g., test_extract)
	size_t where_pos = FindKeyword(after_into_lower, "where");
	size_t extract_pos = FindKeyword(after_into_lower, "extract");
	size_t with_pos = FindKeyword(after_into_lower, "with");
	size_t limit_pos = FindKeyword(after_into_lower, "limit");

	// Determine where table name ends (first keyword found)
	size_t table_end = string::npos;
	vector<size_t> positions = {where_pos, extract_pos, with_pos, limit_pos};
	for (size_t pos : positions) {
		if (pos != string::npos && (table_end == string::npos || pos < table_end)) {
			table_end = pos;
		}
	}

	if (table_end != string::npos) {
		data->target_table = Trim(after_into.substr(0, table_end));
	} else {
		data->target_table = Trim(after_into);
	}

	// Parse WHERE clause if present (must come before WITH)
	if (where_pos != string::npos && (with_pos == string::npos || where_pos < with_pos)) {
		string after_where = after_into.substr(where_pos + 5); // skip "where"
		string after_where_lower = StringUtil::Lower(after_where);

		// Find WITH position in remaining string
		size_t with_in_where = after_where_lower.find("with");
		string where_clause;
		if (with_in_where != string::npos) {
			where_clause = Trim(after_where.substr(0, with_in_where));
			// Update with_pos to be relative to after_into for later processing
			with_pos = where_pos + 5 + with_in_where;
		} else {
			where_clause = Trim(after_where);
		}

		// Parse WHERE clause: expect "url LIKE 'pattern'"
		string where_lower = StringUtil::Lower(where_clause);
		if (!StringUtil::StartsWith(where_lower, "url")) {
			return ParserExtensionParseResult("CRAWL syntax error: WHERE clause must start with 'url'");
		}

		// Find LIKE keyword
		size_t like_pos = where_lower.find("like");
		if (like_pos == string::npos) {
			return ParserExtensionParseResult("CRAWL syntax error: WHERE clause must use 'url LIKE pattern'");
		}

		// Extract pattern after LIKE
		string pattern_part = Trim(where_clause.substr(like_pos + 4));

		// Remove quotes if present
		if (pattern_part.length() >= 2 && pattern_part.front() == '\'' && pattern_part.back() == '\'') {
			data->url_filter = pattern_part.substr(1, pattern_part.length() - 2);
		} else {
			data->url_filter = pattern_part;
		}
	}

	// Parse EXTRACT clause if present
	if (extract_pos != string::npos) {
		string after_extract = after_into.substr(extract_pos + 7); // skip "extract"
		string after_extract_trimmed = Trim(after_extract);

		if (!after_extract_trimmed.empty() && after_extract_trimmed.front() == '(') {
			size_t extract_paren_end = FindClosingParen(after_extract_trimmed, 0);
			if (extract_paren_end != string::npos) {
				string extract_content = after_extract_trimmed.substr(0, extract_paren_end + 1);
				if (!ParseExtractClause(extract_content, *data)) {
					return ParserExtensionParseResult("CRAWL syntax error: invalid EXTRACT clause");
				}

				// Recalculate with_pos and limit_pos by searching in the remaining text
				string remaining_after_extract = after_extract_trimmed.substr(extract_paren_end + 1);
				string remaining_lower = StringUtil::Lower(remaining_after_extract);

				// Calculate proper offset: extract_pos + "extract" + trimmed whitespace + paren content + 1
				size_t trim_offset = after_extract.length() - after_extract_trimmed.length();
				size_t base_offset = extract_pos + 7 + trim_offset + extract_paren_end + 1;

				size_t with_in_remaining = FindKeyword(remaining_lower, "with");
				if (with_in_remaining != string::npos) {
					with_pos = base_offset + with_in_remaining;
				} else {
					with_pos = string::npos; // WITH not after EXTRACT
				}

				size_t limit_in_remaining = FindKeyword(remaining_lower, "limit");
				if (limit_in_remaining != string::npos) {
					limit_pos = base_offset + limit_in_remaining;
				} else {
					limit_pos = string::npos; // LIMIT not after EXTRACT
				}
			} else {
				return ParserExtensionParseResult("CRAWL syntax error: unmatched parenthesis in EXTRACT clause");
			}
		} else {
			return ParserExtensionParseResult("CRAWL syntax error: EXTRACT must be followed by parentheses");
		}
	}

	// Parse WITH clause if present
	if (with_pos != string::npos) {
		string after_with = after_into.substr(with_pos + 4);
		string after_with_trimmed = Trim(after_with);

		// Find the end of WITH (...) - need to match parentheses
		if (!after_with_trimmed.empty() && after_with_trimmed.front() == '(') {
			size_t with_paren_end = FindClosingParen(after_with_trimmed, 0);
			if (with_paren_end != string::npos) {
				string with_content = after_with_trimmed.substr(0, with_paren_end + 1);
				if (!ParseWithOptions(with_content, *data)) {
					return ParserExtensionParseResult("CRAWL syntax error: invalid WITH clause");
				}
			} else {
				return ParserExtensionParseResult("CRAWL syntax error: unmatched parenthesis in WITH clause");
			}
		} else {
			// WITH without parentheses - parse until LIMIT or end
			string with_content;
			if (limit_pos != string::npos && limit_pos > with_pos) {
				with_content = Trim(after_into.substr(with_pos + 4, limit_pos - with_pos - 4));
			} else {
				with_content = after_with_trimmed;
			}
			if (!ParseWithOptions(with_content, *data)) {
				return ParserExtensionParseResult("CRAWL syntax error: invalid WITH clause");
			}
		}
	}

	// Parse LIMIT clause if present - sets max_crawl_pages
	if (limit_pos != string::npos) {
		string after_limit = Trim(after_into.substr(limit_pos + 5)); // skip "limit"
		// Remove trailing semicolon
		if (!after_limit.empty() && after_limit.back() == ';') {
			after_limit.pop_back();
			after_limit = Trim(after_limit);
		}
		// Parse the number
		try {
			int limit_value = std::stoi(after_limit);
			if (limit_value > 0) {
				data->max_crawl_pages = limit_value;
			}
		} catch (...) {
			return ParserExtensionParseResult("CRAWL syntax error: LIMIT must be followed by a positive integer");
		}
	}

	// Remove trailing semicolon from table name if present
	if (!data->target_table.empty() && data->target_table.back() == ';') {
		data->target_table.pop_back();
		data->target_table = Trim(data->target_table);
	}

	// Validate required parameters
	if (data->user_agent.empty()) {
		return ParserExtensionParseResult("CRAWL syntax error: user_agent is required in WITH clause");
	}

	if (data->target_table.empty()) {
		return ParserExtensionParseResult("CRAWL syntax error: target table name is required");
	}

	return ParserExtensionParseResult(std::move(data));
}

ParserExtensionPlanResult CrawlParserExtension::PlanCrawl(ParserExtensionInfo *info, ClientContext &context,
                                                          unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = (CrawlParseData &)*parse_data;
	auto &catalog = Catalog::GetSystemCatalog(context);
	ParserExtensionPlanResult result;

	// Look up the registered crawl_into_internal function from the catalog
	auto catalog_entry = catalog.GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, DEFAULT_SCHEMA,
	                                       "crawl_into_internal", OnEntryNotFound::THROW_EXCEPTION);
	auto &table_function_catalog_entry = catalog_entry->Cast<TableFunctionCatalogEntry>();

	if (table_function_catalog_entry.functions.functions.empty()) {
		throw BinderException("CRAWL: crawl_into_internal function not found");
	}

	result.function = table_function_catalog_entry.functions.functions[0];

	// Pass all data as parameters
	// statement_type: 0=CRAWL_CONCURRENTLY
	result.parameters.push_back(Value(static_cast<int32_t>(data.statement_type)));
	result.parameters.push_back(Value(data.source_query));
	result.parameters.push_back(Value(data.target_table));
	result.parameters.push_back(Value(data.user_agent));
	result.parameters.push_back(Value(data.default_crawl_delay));
	result.parameters.push_back(Value(data.min_crawl_delay));
	result.parameters.push_back(Value(data.max_crawl_delay));
	result.parameters.push_back(Value(data.timeout_seconds));
	result.parameters.push_back(Value(data.respect_robots_txt));
	result.parameters.push_back(Value(data.log_skipped));
	result.parameters.push_back(Value(data.url_filter));
	result.parameters.push_back(Value(data.sitemap_cache_hours));
	result.parameters.push_back(Value(data.update_stale));
	result.parameters.push_back(Value(data.max_retry_backoff_seconds));
	result.parameters.push_back(Value(data.max_parallel_per_domain));
	result.parameters.push_back(Value(data.max_total_connections));
	result.parameters.push_back(Value(data.max_response_bytes));
	result.parameters.push_back(Value(data.compress));
	result.parameters.push_back(Value(data.accept_content_types));
	result.parameters.push_back(Value(data.reject_content_types));
	result.parameters.push_back(Value(data.follow_links));
	result.parameters.push_back(Value(data.allow_subdomains));
	result.parameters.push_back(Value(data.max_crawl_pages));
	result.parameters.push_back(Value(data.max_crawl_depth));
	result.parameters.push_back(Value(data.respect_nofollow));
	result.parameters.push_back(Value(data.follow_canonical));
	result.parameters.push_back(Value(data.num_threads));
	result.parameters.push_back(Value(data.extract_js));
	result.parameters.push_back(Value(SerializeExtractSpecs(data.extract_specs)));

	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::CHANGED_ROWS;

	return result;
}

} // namespace duckdb
