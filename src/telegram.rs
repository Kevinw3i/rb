use crate::alerting::{Alert, AlertSender};
use serde::Serialize;
use std::env;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;

const DEFAULT_API_BASE_URL: &str = "https://api.telegram.org";
const DEFAULT_QUEUE_SIZE: usize = 100;
const DEFAULT_TIMEOUT_SECS: u64 = 3;
const DEFAULT_RATE_LIMIT_PER_SEC: u32 = 1;
const MAX_TEXT_CHARS: usize = 4096;
const TRUNCATION_SUFFIX: &str = "...(truncated)";

#[derive(Clone, Debug)]
pub struct TelegramConfig {
    pub api_base_url: String,
    pub bot_token: String,
    pub chat_id: String,
    pub thread_id: Option<i64>,
    pub queue_size: usize,
    pub timeout: Duration,
    pub rate_limit_per_sec: u32,
}

impl TelegramConfig {
    pub fn from_env() -> Result<Option<Self>, String> {
        let bot_token = env::var("TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let chat_id = env::var("TELEGRAM_CHAT_ID")
            .ok()
            .filter(|v| !v.trim().is_empty());
        if bot_token.is_none() || chat_id.is_none() {
            return Ok(None);
        }

        let api_base_url = env::var("TG_API_BASE_URL").unwrap_or_else(|_| DEFAULT_API_BASE_URL.to_string());

        let thread_id = match env::var("TELEGRAM_MESSAGE_THREAD_ID") {
            Ok(value) if value.trim().is_empty() => None,
            Ok(value) => Some(
                value
                    .parse::<i64>()
                    .map_err(|_| "invalid TELEGRAM_MESSAGE_THREAD_ID")?,
            ),
            Err(_) => None,
        };

        let queue_size = match env::var("TG_ALERT_QUEUE_SIZE") {
            Ok(value) => value
                .parse::<usize>()
                .map_err(|_| "invalid TG_ALERT_QUEUE_SIZE")?,
            Err(_) => DEFAULT_QUEUE_SIZE,
        };
        if queue_size == 0 {
            return Err("TG_ALERT_QUEUE_SIZE must be > 0".to_string());
        }

        let timeout_secs = match env::var("TG_ALERT_TIMEOUT_SECS") {
            Ok(value) => value
                .parse::<u64>()
                .map_err(|_| "invalid TG_ALERT_TIMEOUT_SECS")?,
            Err(_) => DEFAULT_TIMEOUT_SECS,
        };
        if timeout_secs == 0 {
            return Err("TG_ALERT_TIMEOUT_SECS must be > 0".to_string());
        }

        let rate_limit_per_sec = match env::var("TG_ALERT_RATE_LIMIT_PER_SEC") {
            Ok(value) => value
                .parse::<u32>()
                .map_err(|_| "invalid TG_ALERT_RATE_LIMIT_PER_SEC")?,
            Err(_) => DEFAULT_RATE_LIMIT_PER_SEC,
        };
        if rate_limit_per_sec == 0 {
            return Err("TG_ALERT_RATE_LIMIT_PER_SEC must be > 0".to_string());
        }

        Ok(Some(Self {
            api_base_url,
            bot_token: bot_token.unwrap(),
            chat_id: chat_id.unwrap(),
            thread_id,
            queue_size,
            timeout: Duration::from_secs(timeout_secs),
            rate_limit_per_sec,
        }))
    }
}

pub struct TelegramWorker {
    pub sender: AlertSender,
    pub handle: JoinHandle<()>,
}

pub fn spawn_telegram_worker(cfg: TelegramConfig) -> TelegramWorker {
    let (tx, mut rx) = mpsc::channel::<Alert>(cfg.queue_size);
    let sender = AlertSender::new(tx);

    let handle = tokio::spawn(async move {
        let client = match reqwest::Client::builder().timeout(cfg.timeout).build() {
            Ok(client) => client,
            Err(err) => {
                eprintln!(
                    "[{}] ERROR event=tg_client_build_error err={err}",
                    chrono::Utc::now().to_rfc3339()
                );
                return;
            }
        };

        let api_base_url = cfg.api_base_url.trim_end_matches('/').to_string();
        let url = format!("{api_base_url}/bot{}/sendMessage", cfg.bot_token);
        let min_interval =
            Duration::from_secs_f64(1.0 / (cfg.rate_limit_per_sec.max(1) as f64));
        let mut backoff = Duration::from_secs(0);

        while let Some(alert) = rx.recv().await {
            let text = format_alert_text(&alert);
            let body = SendMessageRequest {
                chat_id: &cfg.chat_id,
                text: &text,
                parse_mode: Some("HTML"),
                message_thread_id: cfg.thread_id,
            };

            let mut failed = false;
            match client.post(&url).json(&body).send().await {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        failed = true;
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        eprintln!(
                            "[{}] ERROR event=tg_send_failed status={status} body={}",
                            chrono::Utc::now().to_rfc3339(),
                            truncate_for_log(&body, 512)
                        );
                    }
                }
                Err(err) => {
                    failed = true;
                    eprintln!(
                        "[{}] ERROR event=tg_send_error err={err}",
                        chrono::Utc::now().to_rfc3339()
                    );
                }
            }

            if failed {
                backoff = if backoff.is_zero() {
                    Duration::from_secs(1)
                } else {
                    backoff.saturating_mul(2).min(Duration::from_secs(30))
                };
            } else {
                backoff = Duration::from_secs(0);
            }

            let sleep_for = if backoff > min_interval {
                backoff
            } else {
                min_interval
            };
            if !sleep_for.is_zero() {
                sleep(sleep_for).await;
            }
        }
    });

    TelegramWorker { sender, handle }
}

#[derive(Serialize)]
struct SendMessageRequest<'a> {
    chat_id: &'a str,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_thread_id: Option<i64>,
}

fn format_alert_text(alert: &Alert) -> String {
    let mut header = "rb".to_string();
    if let Some(market) = &alert.context.market {
        header.push(' ');
        header.push_str(market);
    }
    if let Some(symbol) = &alert.context.symbol {
        header.push(' ');
        header.push_str(symbol);
    }

    let mut lines = Vec::with_capacity(8);
    lines.push(html_escape(&header));
    lines.push("".to_string());
    lines.push(format!("<b>{}</b>", html_escape(&alert.title)));
    lines.push(format!("category={}", html_escape(alert.kind.as_str())));
    for (key, value) in &alert.fields {
        lines.push(format!(
            "{}={}",
            html_escape(key),
            html_escape(value)
        ));
    }
    if let Some(price) = &alert.current_price {
        lines.push(format!("current_price={}", html_escape(price)));
    }
    lines.push(format!("ts={}", html_escape(&alert.ts.to_rfc3339())));
    if let Some(body) = &alert.body {
        lines.push("".to_string());
        lines.push(html_escape(body));
    }

    truncate_for_telegram(&lines.join("\n"))
}

fn html_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

fn truncate_for_telegram(text: &str) -> String {
    if text.chars().count() <= MAX_TEXT_CHARS {
        return text.to_string();
    }

    let suffix_len = TRUNCATION_SUFFIX.chars().count();
    let keep = MAX_TEXT_CHARS.saturating_sub(suffix_len);
    let mut out: String = text.chars().take(keep).collect();
    out.push_str(TRUNCATION_SUFFIX);
    out
}

fn truncate_for_log(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max_chars).collect();
    out.push_str("...(truncated)");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerting::AlertContext;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn format_alert_text_truncates_to_telegram_limit() {
        let ctx = AlertContext::for_symbol_market("futures", "BTCUSDC");
        let alert = Alert::error("x".repeat(10_000), ctx);
        let text = format_alert_text(&alert);
        assert!(text.chars().count() <= MAX_TEXT_CHARS);
    }

    #[tokio::test]
    async fn telegram_worker_posts_to_api() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/botTEST_TOKEN/sendMessage"))
            .and(body_string_contains("\"parse_mode\":\"HTML\""))
            .and(body_string_contains("<b>ERROR</b>"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let cfg = TelegramConfig {
            api_base_url: server.uri(),
            bot_token: "TEST_TOKEN".to_string(),
            chat_id: "123".to_string(),
            thread_id: None,
            queue_size: 10,
            timeout: Duration::from_secs(2),
            rate_limit_per_sec: 1000,
        };

        let TelegramWorker { sender, handle } = spawn_telegram_worker(cfg);
        let ctx = AlertContext::for_symbol_market("futures", "BTCUSDC");
        sender.try_send(Alert::error("event=test", ctx));

        // Dropping the sender closes the channel and lets the worker finish cleanly.
        drop(sender);
        match tokio::time::timeout(Duration::from_secs(2), handle).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("worker join error: {err}"),
            Err(_) => panic!("worker did not stop in time"),
        }
    }
}
