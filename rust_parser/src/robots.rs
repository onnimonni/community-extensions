//! robots.txt parsing and checking

use std::collections::HashMap;
use std::sync::RwLock;
use texting_robots::Robot;

/// Cached robots.txt data per domain
#[derive(Debug)]
pub struct RobotsCache {
    cache: RwLock<HashMap<String, CachedRobots>>,
}

#[derive(Debug)]
struct CachedRobots {
    /// Raw robots.txt content (Robot doesn't impl Clone, so we store raw)
    robots_txt: String,
    crawl_delay: Option<f64>,
    sitemaps: Vec<String>,
    fetched_at: std::time::Instant,
}

/// Result of robots.txt check
#[derive(Debug, Clone, serde::Serialize)]
pub struct RobotsCheckResult {
    pub allowed: bool,
    pub crawl_delay: Option<f64>,
    pub sitemaps: Vec<String>,
}

impl RobotsCache {
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Check if URL is allowed by robots.txt (using ureq)
    pub fn check_blocking(
        &self,
        agent: &ureq::Agent,
        url: &str,
        user_agent: &str,
    ) -> RobotsCheckResult {
        let parsed = match url::Url::parse(url) {
            Ok(u) => u,
            Err(_) => {
                return RobotsCheckResult {
                    allowed: true,
                    crawl_delay: None,
                    sitemaps: vec![],
                };
            }
        };

        let domain = match parsed.host_str() {
            Some(h) => h.to_lowercase(),
            None => {
                return RobotsCheckResult {
                    allowed: true,
                    crawl_delay: None,
                    sitemaps: vec![],
                };
            }
        };

        // Check cache first
        {
            if let Ok(cache) = self.cache.read() {
                if let Some(cached) = cache.get(&domain) {
                    // Cache valid for 1 hour
                    if cached.fetched_at.elapsed().as_secs() < 3600 {
                        return Self::check_cached(cached, url, user_agent);
                    }
                }
            }
        }

        // Fetch robots.txt
        let robots_url = format!("{}://{}/robots.txt", parsed.scheme(), domain);
        let robots_txt = match agent.get(&robots_url).call() {
            Ok(resp) if resp.status().is_success() => {
                resp.into_body().read_to_string().unwrap_or_default()
            }
            _ => String::new(), // No robots.txt = allow all
        };

        // Parse robots.txt
        let crawl_delay = Self::extract_crawl_delay(&robots_txt, user_agent);
        let sitemaps = Self::extract_sitemaps(&robots_txt);

        // Check if allowed using Robot
        let allowed = Robot::new(user_agent, robots_txt.as_bytes())
            .map(|r| r.allowed(url))
            .unwrap_or(true);

        let cached = CachedRobots {
            robots_txt: robots_txt.clone(),
            crawl_delay,
            sitemaps: sitemaps.clone(),
            fetched_at: std::time::Instant::now(),
        };

        // Store in cache
        {
            if let Ok(mut cache) = self.cache.write() {
                cache.insert(domain, cached);
            }
        }

        RobotsCheckResult {
            allowed,
            crawl_delay,
            sitemaps,
        }
    }

    fn check_cached(cached: &CachedRobots, url: &str, user_agent: &str) -> RobotsCheckResult {
        // Re-parse robots.txt to check URL (Robot doesn't impl Clone)
        let allowed = Robot::new(user_agent, cached.robots_txt.as_bytes())
            .map(|r| r.allowed(url))
            .unwrap_or(true);

        RobotsCheckResult {
            allowed,
            crawl_delay: cached.crawl_delay,
            sitemaps: cached.sitemaps.clone(),
        }
    }

    /// Extract Crawl-delay from robots.txt
    fn extract_crawl_delay(robots_txt: &str, user_agent: &str) -> Option<f64> {
        let ua_lower = user_agent.to_lowercase();
        let mut in_matching_section = false;
        let mut default_delay: Option<f64> = None;

        for line in robots_txt.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let lower = line.to_lowercase();
            if lower.starts_with("user-agent:") {
                let agent = lower.strip_prefix("user-agent:").unwrap().trim();
                in_matching_section = agent == "*" || ua_lower.contains(agent);
                if agent == "*" && default_delay.is_none() {
                    // Will capture default section delay
                }
            } else if lower.starts_with("crawl-delay:") {
                if let Some(delay_str) = lower.strip_prefix("crawl-delay:") {
                    if let Ok(delay) = delay_str.trim().parse::<f64>() {
                        if in_matching_section {
                            return Some(delay);
                        }
                        if default_delay.is_none() {
                            default_delay = Some(delay);
                        }
                    }
                }
            }
        }

        default_delay
    }

    /// Extract Sitemap URLs from robots.txt
    fn extract_sitemaps(robots_txt: &str) -> Vec<String> {
        robots_txt
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                let lower = line.to_lowercase();
                if lower.starts_with("sitemap:") {
                    Some(line[8..].trim().to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get sitemaps for a domain (fetches robots.txt if needed)
    pub fn get_sitemaps_blocking(
        &self,
        agent: &ureq::Agent,
        base_url: &str,
        user_agent: &str,
    ) -> Vec<String> {
        let result = self.check_blocking(agent, base_url, user_agent);
        result.sitemaps
    }
}

impl Default for RobotsCache {
    fn default() -> Self {
        Self::new()
    }
}
