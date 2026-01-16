#pragma once

#include <string>
#include <vector>
#include "crawl_parser.hpp"

namespace duckdb {

#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE

// C-compatible struct matching Rust's ExtractionResultFFI
extern "C" {
struct ExtractionResultFFI {
    char *json_ptr;   // JSON-serialized result (owned by Rust)
    char *error_ptr;  // Error message or null (owned by Rust)
};

// FFI functions exported from Rust
ExtractionResultFFI extract_from_html(const char *html_ptr, size_t html_len,
                                       const char *request_json);
void free_extraction_result(ExtractionResultFFI result);

ExtractionResultFFI extract_jsonld_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_microdata_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_opengraph_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_js_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_css_ffi(const char *html_ptr, size_t html_len,
                                     const char *selector);
}

#endif // RUST_PARSER_AVAILABLE

// C++ wrapper functions

#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE

// RAII wrapper for ExtractionResultFFI
class RustExtractionResult {
public:
    explicit RustExtractionResult(ExtractionResultFFI result) : result_(result) {}

    ~RustExtractionResult() {
        free_extraction_result(result_);
    }

    RustExtractionResult(const RustExtractionResult &) = delete;
    RustExtractionResult &operator=(const RustExtractionResult &) = delete;

    RustExtractionResult(RustExtractionResult &&other) noexcept : result_(other.result_) {
        other.result_ = {nullptr, nullptr};
    }

    bool HasError() const { return result_.error_ptr != nullptr; }
    std::string GetError() const {
        return result_.error_ptr ? std::string(result_.error_ptr) : "";
    }
    std::string GetJson() const {
        return result_.json_ptr ? std::string(result_.json_ptr) : "";
    }

private:
    ExtractionResultFFI result_;
};

// High-level extraction function
// Takes HTML and ExtractSpec list, returns extracted values as JSON strings
std::vector<std::string> ExtractWithRust(const std::string &html,
                                          const std::vector<ExtractSpec> &specs);

// Convenience extraction functions
std::string ExtractJsonLdWithRust(const std::string &html);
std::string ExtractMicrodataWithRust(const std::string &html);
std::string ExtractOpenGraphWithRust(const std::string &html);
std::string ExtractJsWithRust(const std::string &html);
std::string ExtractCssWithRust(const std::string &html, const std::string &selector);

#endif // RUST_PARSER_AVAILABLE

// Check if Rust parser is available at compile time
inline bool IsRustParserAvailable() {
#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE
    return true;
#else
    return false;
#endif
}

} // namespace duckdb
