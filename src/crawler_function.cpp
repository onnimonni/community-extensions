#include "crawler_internal.hpp"
#include "crawler_utils.hpp"
#include "thread_utils.hpp"
#include "robots_parser.hpp"
#include "sitemap_parser.hpp"
#include "link_parser.hpp"
#include "json_path_evaluator.hpp"
#include "crawl_parser.hpp"
#include "rust_ffi.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/appender.hpp"
#include <set>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <sstream>
#include <unordered_map>
#include <queue>
#include <deque>
#include <vector>
#include <ctime>
#include <cmath>
#include <future>
#include <zlib.h>

namespace duckdb {

// Global connection counter for rate limiting across all domains
std::atomic<int> g_active_connections(0);

// Adaptive rate limiting: adjust delay based on response times
// Uses exponential moving average (EMA) with alpha=0.2
void UpdateAdaptiveDelay(DomainState &state, double response_ms, double max_delay) {
	// Update EMA (alpha=0.2 for smoothing)
	constexpr double alpha = 0.2;
	if (state.response_count == 0) {
		state.average_response_ms = response_ms;
	} else {
		state.average_response_ms = alpha * response_ms + (1.0 - alpha) * state.average_response_ms;
	}
	state.response_count++;

	// Need at least 3 samples before adapting
	if (state.response_count < 3) {
		return;
	}

	// If response is significantly slower than average, increase delay
	if (response_ms > 2.0 * state.average_response_ms) {
		state.crawl_delay_seconds = std::min(state.crawl_delay_seconds * 1.5, max_delay);
	}
	// If response is significantly faster than average, decrease delay (but respect floor)
	else if (response_ms < 0.5 * state.average_response_ms) {
		state.crawl_delay_seconds = std::max(state.crawl_delay_seconds * 0.9, state.min_crawl_delay_seconds);
	}
}

} // namespace duckdb
