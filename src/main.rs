mod alerting;
mod telegram;

use std::env;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, Utc};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{interval, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const FUTURES_WS_BASE_URL: &str = "wss://fstream.binance.com/ws";
const SPOT_WS_BASE_URL: &str = "wss://stream.binance.com:9443/ws";
const FUTURES_USER_STREAM_WS_BASE_URL: &str = "wss://fstream.binance.com/pm/ws";
const SPOT_USER_STREAM_WS_BASE_URL: &str = "wss://stream.binance.com:9443/ws";
const FUTURES_API_BASE_URL: &str = "https://papi.binance.com";
const FUTURES_EXCHANGE_BASE_URL: &str = "https://fapi.binance.com";
const SPOT_API_BASE_URL: &str = "https://api.binance.com";
const SPOT_EXCHANGE_BASE_URL: &str = "https://api.binance.com";
const FUTURES_POSITION_RISK_PATH: &str = "/papi/v1/um/positionRisk";
const FUTURES_OPEN_ORDERS_PATH: &str = "/papi/v1/um/openOrders";
const FUTURES_ORDER_PATH: &str = "/papi/v1/um/order";
const FUTURES_CONDITIONAL_ORDER_PATH: &str = "/papi/v1/um/conditional/order";
const FUTURES_OPEN_CONDITIONAL_ORDERS_PATH: &str = "/papi/v1/um/conditional/openOrders";
const FUTURES_CONDITIONAL_ORDER_HISTORY_PATH: &str = "/papi/v1/um/conditional/orderHistory";
const FUTURES_USER_STREAM_LISTEN_KEY_PATH: &str = "/papi/v1/listenKey";
const FUTURES_EXCHANGE_INFO_PATH: &str = "/fapi/v1/exchangeInfo";
const SPOT_ACCOUNT_PATH: &str = "/api/v3/account";
const SPOT_OPEN_ORDERS_PATH: &str = "/api/v3/openOrders";
const SPOT_ORDER_PATH: &str = "/api/v3/order";
const SPOT_USER_STREAM_LISTEN_KEY_PATH: &str = "/api/v3/userDataStream";
const SPOT_EXCHANGE_INFO_PATH: &str = "/api/v3/exchangeInfo";
const RECV_WINDOW_MS: u64 = 5000;
const INITIAL_BACKOFF_SECS: u64 = 1;
const MAX_BACKOFF_SECS: u64 = 32;
const PING_INTERVAL_SECS: u64 = 30;
const POSITION_REFRESH_SECS: u64 = 1;
const ORDER_MANAGE_INTERVAL_SECS: u64 = 1;
const ENTRY_MANAGE_INTERVAL_SECS: u64 = 1;
const OPEN_ORDERS_CACHE_SECS: u64 = 1;
const USER_STREAM_KEEPALIVE_SECS: u64 = 30 * 60;
const SYMBOL_FILTERS_REFRESH_SECS: u64 = 60;
const CLIENT_ID_PREFIX: &str = "rb-tp-";
const ENTRY_CLIENT_ID_PREFIX: &str = "rb-entry-";
const STOP_CLIENT_ID_PREFIX: &str = "rb-stop-";
const RATE_LIMIT_LOG_SECS: u64 = 30;
const RATE_LIMIT_BACKOFF_INITIAL_SECS: u64 = 1;
const RATE_LIMIT_MAX_BACKOFF_SECS: u64 = 120;
const RATE_LIMIT_MAX_RETRIES: u32 = 3;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

type HmacSha256 = Hmac<Sha256>;

enum TriggerMode {
    Below,
    Above,
}

impl TriggerMode {
    fn as_str(&self) -> &'static str {
        match self {
            TriggerMode::Below => "below",
            TriggerMode::Above => "above",
        }
    }
}

#[derive(Clone, Copy)]
enum MarketType {
    Futures,
    Spot,
}

impl MarketType {
    fn as_str(&self) -> &'static str {
        match self {
            MarketType::Futures => "futures",
            MarketType::Spot => "spot",
        }
    }

    fn ws_base_url(&self) -> &'static str {
        match self {
            MarketType::Futures => FUTURES_WS_BASE_URL,
            MarketType::Spot => SPOT_WS_BASE_URL,
        }
    }

    fn user_stream_ws_base_url(&self) -> &'static str {
        match self {
            MarketType::Futures => FUTURES_USER_STREAM_WS_BASE_URL,
            MarketType::Spot => SPOT_USER_STREAM_WS_BASE_URL,
        }
    }

    fn default_api_base_url(&self) -> &'static str {
        match self {
            MarketType::Futures => FUTURES_API_BASE_URL,
            MarketType::Spot => SPOT_API_BASE_URL,
        }
    }

    fn default_exchange_base_url(&self) -> &'static str {
        match self {
            MarketType::Futures => FUTURES_EXCHANGE_BASE_URL,
            MarketType::Spot => SPOT_EXCHANGE_BASE_URL,
        }
    }

    fn exchange_info_path(&self) -> &'static str {
        match self {
            MarketType::Futures => FUTURES_EXCHANGE_INFO_PATH,
            MarketType::Spot => SPOT_EXCHANGE_INFO_PATH,
        }
    }
}

#[derive(Clone, Copy)]
enum EntrySide {
    Long,
    Short,
}

impl EntrySide {
    fn as_str(&self) -> &'static str {
        match self {
            EntrySide::Long => "long",
            EntrySide::Short => "short",
        }
    }

    fn entry_side(&self) -> &'static str {
        match self {
            EntrySide::Long => "BUY",
            EntrySide::Short => "SELL",
        }
    }

    fn stop_side(&self) -> &'static str {
        match self {
            EntrySide::Long => "SELL",
            EntrySide::Short => "BUY",
        }
    }
}

#[derive(Clone, Copy)]
enum EntryDetect {
    Prefix,
    Any,
}

impl EntryDetect {
    fn as_str(&self) -> &'static str {
        match self {
            EntryDetect::Prefix => "prefix",
            EntryDetect::Any => "any",
        }
    }
}

#[derive(Clone)]
struct EntryConfig {
    entry_price: f64,
    entry_price_str: String,
    stop_price: f64,
    stop_price_str: String,
    side: EntrySide,
    entry_usdc: Option<f64>,
    entry_usdc_str: Option<String>,
    leverage: u32,
    entry_usdc_provided: bool,
    leverage_provided: bool,
}

struct Config {
    symbol: String,
    market: MarketType,
    trigger_price: f64,
    order_price: f64,
    order_price_str: String,
    mode: TriggerMode,
    log_enabled: bool,
    entry: Option<EntryConfig>,
    entry_detect: EntryDetect,
    entry_abort_price: Option<f64>,
    entry_abort_price_str: Option<String>,
    base_asset: Option<String>,
    quote_asset: Option<String>,
}

impl EntryConfig {
    fn entry_qty(&self) -> Option<(f64, String)> {
        let entry_usdc = self.entry_usdc?;
        if entry_usdc <= 0.0 || self.entry_price <= 0.0 {
            return None;
        }
        let qty = (entry_usdc * self.leverage as f64) / self.entry_price;
        if qty <= 0.0 {
            return None;
        }
        Some((qty, format_qty(qty)))
    }
}

#[derive(Clone)]
struct Logger {
    inner: Arc<LoggerInner>,
}

struct LoggerInner {
    path: PathBuf,
    file: Option<StdMutex<std::fs::File>>,
    enabled: bool,
    alert_sender: StdMutex<Option<alerting::AlertSender>>,
    alert_context: StdMutex<alerting::AlertContext>,
}

enum LogLevel {
    Event,
    Tick,
    Error,
}

impl LogLevel {
    fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Event => "EVENT",
            LogLevel::Tick => "TICK",
            LogLevel::Error => "ERROR",
        }
    }
}

impl Logger {
    fn new(path: PathBuf, enabled: bool) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let file = if enabled {
            Some(StdMutex::new(
                OpenOptions::new().create(true).append(true).open(&path)?,
            ))
        } else {
            None
        };
        Ok(Self {
            inner: Arc::new(LoggerInner {
                path,
                file,
                enabled,
                alert_sender: StdMutex::new(None),
                alert_context: StdMutex::new(alerting::AlertContext::default()),
            }),
        })
    }

    fn path(&self) -> &Path {
        &self.inner.path
    }

    fn set_context(&self, market: &str, symbol: &str) {
        let mut guard = self
            .inner
            .alert_context
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *guard = alerting::AlertContext::for_symbol_market(market, symbol);
    }

    fn context(&self) -> alerting::AlertContext {
        self.inner
            .alert_context
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn set_alert_sender(&self, sender: alerting::AlertSender) {
        let mut guard = self
            .inner
            .alert_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *guard = Some(sender);
    }

    fn shutdown_alerting(&self) {
        let mut guard = self
            .inner
            .alert_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        *guard = None;
    }

    fn event(&self, message: &str) {
        self.log(LogLevel::Event, message, true);
    }

    fn tick(&self, message: &str) {
        self.log(LogLevel::Tick, message, false);
    }

    fn error(&self, message: &str) {
        self.log(LogLevel::Error, message, true);
        self.send_alert(alerting::Alert::error(message, self.context()));
    }

    fn send_alert(&self, alert: alerting::Alert) {
        let sender = self
            .inner
            .alert_sender
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        if let Some(sender) = sender {
            sender.try_send(alert);
        }
    }

    fn log(&self, level: LogLevel, message: &str, print_stderr: bool) {
        let line = format!("[{}] {} {}", log_timestamp(), level.as_str(), message);
        if print_stderr {
            eprintln!("{line}");
        }
        if self.inner.enabled && matches!(level, LogLevel::Event) {
            if let Err(err) = self.write_line(&line) {
                eprintln!("log write error: {err}");
            }
        }
    }

    fn write_line(&self, line: &str) -> std::io::Result<()> {
        let file = match &self.inner.file {
            Some(file) => file,
            None => return Ok(()),
        };
        let mut file = file.lock().unwrap_or_else(|poison| poison.into_inner());
        writeln!(file, "{line}")?;
        file.flush()
    }
}

struct RateLimitState {
    last_logged_at: Option<Instant>,
    backoff_secs: u64,
    backoff_until: Option<Instant>,
}

impl RateLimitState {
    fn new() -> Self {
        Self {
            last_logged_at: None,
            backoff_secs: 0,
            backoff_until: None,
        }
    }
}

#[derive(Clone)]
struct BinanceClient {
    http: reqwest::Client,
    api_key: String,
    api_secret: String,
    base_url: String,
    exchange_base_url: String,
    market: MarketType,
    logger: Logger,
    rate_limit_state: Arc<StdMutex<RateLimitState>>,
}

#[derive(Clone, Debug)]
struct PositionSnapshot {
    amt: f64,
    amt_str: String,
    position_side: String,
}

#[derive(Deserialize, Debug)]
struct PositionRisk {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "positionSide")]
    position_side: String,
}

#[derive(Deserialize, Debug)]
struct AccountInfo {
    balances: Vec<AccountBalance>,
}

#[derive(Deserialize, Debug)]
struct AccountBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Deserialize, Debug, Clone)]
struct OpenOrder {
    #[serde(rename = "orderId")]
    order_id: u64,
    price: String,
    #[serde(rename = "origQty")]
    orig_qty: String,
    #[serde(rename = "reduceOnly")]
    #[serde(default)]
    reduce_only: bool,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(rename = "positionSide")]
    #[serde(default)]
    position_side: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    #[serde(rename = "timeInForce")]
    #[serde(default)]
    time_in_force: String,
}

#[derive(Deserialize, Debug, Clone)]
struct ConditionalOrder {
    #[serde(rename = "strategyId")]
    strategy_id: u64,
    #[serde(rename = "strategyType", default)]
    strategy_type: String,
    #[serde(rename = "stopPrice", default)]
    stop_price: String,
    #[serde(rename = "origQty", default)]
    orig_qty: String,
    #[serde(rename = "reduceOnly", default)]
    reduce_only: bool,
    side: String,
    #[serde(rename = "newClientStrategyId", default)]
    client_strategy_id: String,
}

#[derive(Deserialize, Debug)]
struct OrderAck {
    #[serde(rename = "orderId", alias = "strategyId")]
    order_id: u64,
    #[serde(rename = "clientOrderId", alias = "newClientStrategyId")]
    client_order_id: String,
}

#[derive(Deserialize, Debug)]
struct ExchangeInfo {
    symbols: Vec<ExchangeSymbol>,
}

#[derive(Deserialize, Debug)]
struct ExchangeSymbol {
    symbol: String,
    #[serde(rename = "baseAsset", default)]
    base_asset: String,
    #[serde(rename = "quoteAsset", default)]
    quote_asset: String,
    filters: Vec<ExchangeFilter>,
}

#[derive(Deserialize, Debug)]
struct ExchangeFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "stepSize", default)]
    step_size: String,
}

#[derive(Clone)]
struct SymbolFilters {
    lot_step_str: String,
    lot_precision: usize,
    lot_step_units: u64,
}

impl SymbolFilters {
    fn round_qty(&self, qty: f64) -> (f64, String) {
        let rounded = floor_to_step(qty, self.lot_precision, self.lot_step_units);
        let rounded_str = format_with_precision(rounded, self.lot_precision);
        (rounded, rounded_str)
    }
}

#[derive(Deserialize, Debug)]
struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Deserialize, Debug)]
struct OrderStatus {
    status: String,
}

#[derive(Deserialize, Debug)]
struct ConditionalOrderHistory {
    #[serde(rename = "strategyStatus")]
    strategy_status: String,
    #[serde(rename = "status", default)]
    status: String,
}

#[derive(Clone, Debug, Default)]
struct FillInfo {
    side: Option<String>,
    qty: Option<String>,
    price: Option<String>,
    client_id: Option<String>,
}

struct OrderManager {
    client: BinanceClient,
    config: Config,
    logger: Logger,
    last_position: Option<PositionSnapshot>,
    last_open_position: Option<PositionSnapshot>,
    last_position_at: Option<Instant>,
    last_manage_at: Option<Instant>,
    last_active: Option<bool>,
    last_price_str: Option<String>,
    last_entry_manage_at: Option<Instant>,
    entry_missing_logged: bool,
    open_orders_cache: Option<Vec<OpenOrder>>,
    open_orders_cached_at: Option<Instant>,
    tp_client_id: Option<String>,
    stop_client_id: Option<String>,
    entry_client_id: Option<String>,
    last_fill: Option<FillInfo>,
    exit_requested: bool,
    exit_reason: Option<String>,
    position_was_open: bool,
    entry_completed: bool,
    symbol_filters: Option<SymbolFilters>,
    last_filters_attempt_at: Option<Instant>,
    entry_round_logged: bool,
    entry_startup_cleanup_done: bool,
    entry_abort_checked: bool,
}

type SharedManager = Arc<AsyncMutex<OrderManager>>;

#[tokio::main]
async fn main() {
    let mut config = match parse_args() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return;
        }
    };

    let log_path = log_path_from_env();
    let logger = match Logger::new(log_path, config.log_enabled) {
        Ok(logger) => logger,
        Err(err) => {
            eprintln!("failed to open log file: {err}");
            return;
        }
    };
    logger.set_context(config.market.as_str(), &config.symbol);

    let mut tg_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut tg_shutdown_timeout = Duration::from_secs(1);
    match telegram::TelegramConfig::from_env() {
        Ok(Some(cfg)) => {
            let thread_display = cfg
                .thread_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "unset".to_string());
            let enabled_line = format!(
                "event=tg_alerting enabled=true chat_id={} thread_id={} api_base_url={} queue_size={} timeout_secs={} rate_limit_per_sec={}",
                cfg.chat_id,
                thread_display,
                cfg.api_base_url,
                cfg.queue_size,
                cfg.timeout.as_secs(),
                cfg.rate_limit_per_sec
            );
            // Let shutdown wait at least one full Telegram request timeout,
            // plus a small buffer for task scheduling/cleanup.
            tg_shutdown_timeout = cfg.timeout + Duration::from_secs(1);
            let telegram::TelegramWorker { sender, handle } = telegram::spawn_telegram_worker(cfg);
            logger.set_alert_sender(sender);
            tg_handle = Some(handle);
            logger.event(&enabled_line);
        }
        Ok(None) => {
            logger.event("event=tg_alerting enabled=false reason=missing_env");
        }
        Err(err) => {
            logger.error(&format!("event=tg_alerting_config_error err={err}"));
        }
    }

    let log_path_display = if config.log_enabled {
        logger.path().display().to_string()
    } else {
        "disabled".to_string()
    };
    logger.event(&format!(
        "event=start symbol={} market={} trigger={} order={} mode={} log_enabled={} log_path={}",
        config.symbol,
        config.market.as_str(),
        config.trigger_price,
        config.order_price,
        config.mode.as_str(),
        config.log_enabled,
        log_path_display
    ));
    log_price_gap_warning(&logger, config.trigger_price, config.order_price);
    if let Some(entry) = &config.entry {
        let entry_usdc = entry.entry_usdc_str.as_deref().unwrap_or("unset");
        let entry_abort = config
            .entry_abort_price_str
            .as_deref()
            .unwrap_or("unset");
        let entry_qty = entry
            .entry_qty()
            .map(|(_, qty)| qty)
            .unwrap_or_else(|| "unset".to_string());
        logger.event(&format!(
            "event=entry_config entry={} stop={} side={} entry_usdc={} leverage={} entry_qty={} entry_detect={} entry_abort={}",
            entry.entry_price_str,
            entry.stop_price_str,
            entry.side.as_str(),
            entry_usdc,
            entry.leverage,
            entry_qty,
            config.entry_detect.as_str(),
            entry_abort
        ));
    }

    let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
    let api_secret = env::var("BINANCE_API_SECRET").unwrap_or_default();
    if api_key.is_empty() || api_secret.is_empty() {
        logger.error("event=missing_keys message=missing BINANCE_API_KEY or BINANCE_API_SECRET");
        return;
    }

    let base_url = env::var("BINANCE_BASE_URL")
        .unwrap_or_else(|_| config.market.default_api_base_url().to_string());
    let exchange_base_url = env::var("BINANCE_EXCHANGE_BASE_URL")
        .unwrap_or_else(|_| config.market.default_exchange_base_url().to_string());
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            logger.error(&format!("event=http_client_error err={err}"));
            return;
        }
    };

    let client = BinanceClient {
        http,
        api_key,
        api_secret,
        base_url,
        exchange_base_url,
        market: config.market,
        logger: logger.clone(),
        rate_limit_state: Arc::new(StdMutex::new(RateLimitState::new())),
    };

    let symbol_info = match client.fetch_exchange_symbol(&config.symbol).await {
        Ok(info) => info,
        Err(err) => {
            logger.error(&format!(
                "event=symbol_invalid symbol={} market={} err={err}",
                config.symbol,
                config.market.as_str()
            ));
            return;
        }
    };
    if !symbol_info.base_asset.trim().is_empty() {
        config.base_asset = Some(symbol_info.base_asset.clone());
    }
    if !symbol_info.quote_asset.trim().is_empty() {
        config.quote_asset = Some(symbol_info.quote_asset.clone());
    }
    if matches!(config.market, MarketType::Spot) && config.base_asset.is_none() {
        logger.error(&format!(
            "event=symbol_invalid symbol={} market={} reason=missing_base_asset",
            config.symbol,
            config.market.as_str()
        ));
        return;
    }

    let market = config.market;
    let symbol = config.symbol.clone();
    let ws_url = format!("{}/{}@aggTrade", market.ws_base_url(), symbol.to_ascii_lowercase());

    let manager = Arc::new(AsyncMutex::new(OrderManager::new(
        config,
        client.clone(),
        logger.clone(),
    )));
    let user_stream_handle = tokio::spawn(run_user_stream(
        client.clone(),
        manager.clone(),
        logger.clone(),
        market,
        symbol.clone(),
    ));
    let mut backoff = Duration::from_secs(INITIAL_BACKOFF_SECS);
    let max_backoff = Duration::from_secs(MAX_BACKOFF_SECS);

    loop {
        if manager.lock().await.exit_requested() {
            break;
        }
        logger.event(&format!("event=connect_attempt url={ws_url}"));
        match connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                logger.event("event=connected");
                backoff = Duration::from_secs(INITIAL_BACKOFF_SECS);
                if let Err(err) = stream_last_price(ws_stream, manager.clone(), symbol.clone()).await {
                    logger.error(&format!("event=connection_error err={err}"));
                }
                if manager.lock().await.exit_requested() {
                    if let Some(reason) = manager.lock().await.exit_reason() {
                        logger.event(&format!("event=exit_done reason={reason}"));
                    }
                    break;
                }
            }
            Err(err) => {
                logger.error(&format!("event=connect_error err={err}"));
            }
        }

        logger.event(&format!(
            "event=reconnect_sleep seconds={}",
            backoff.as_secs()
        ));
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }

    if let Err(err) = user_stream_handle.await {
        logger.error(&format!("event=user_stream_task_error err={err}"));
    }

    logger.shutdown_alerting();
    if let Some(handle) = tg_handle {
        match timeout(tg_shutdown_timeout, handle).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                logger.event(&format!("event=tg_worker_join_error err={err}"));
            }
            Err(_) => {
                logger.event(&format!(
                    "event=tg_worker_join_timeout seconds={}",
                    tg_shutdown_timeout.as_secs()
                ));
            }
        }
    }
}

impl OrderManager {
    fn new(config: Config, client: BinanceClient, logger: Logger) -> Self {
        Self {
            client,
            config,
            logger,
            last_position: None,
            last_open_position: None,
            last_position_at: None,
            last_manage_at: None,
            last_active: None,
            last_price_str: None,
            last_entry_manage_at: None,
            entry_missing_logged: false,
            open_orders_cache: None,
            open_orders_cached_at: None,
            tp_client_id: None,
            stop_client_id: None,
            entry_client_id: None,
            last_fill: None,
            exit_requested: false,
            exit_reason: None,
            position_was_open: false,
            entry_completed: false,
            symbol_filters: None,
            last_filters_attempt_at: None,
            entry_round_logged: false,
            entry_startup_cleanup_done: false,
            entry_abort_checked: false,
        }
    }

    fn event_with_price(&self, message: &str) {
        if let Some(price) = &self.last_price_str {
            self.logger
                .event(&format!("{message} current_price={price}"));
        } else {
            self.logger.event(message);
        }
    }

    fn exit_requested(&self) -> bool {
        self.exit_requested
    }

    fn exit_reason(&self) -> Option<&str> {
        self.exit_reason.as_deref()
    }

    async fn handle_tick(&mut self, symbol: &str, price_str: &str, price: f64, event_time_ms: u64) {
        if self.exit_requested {
            return;
        }

        self.last_price_str = Some(price_str.to_string());
        if let Err(err) = self.refresh_position_if_needed(symbol, false).await {
            self.logger
                .error(&format!("event=position_refresh_error err={err}"));
        }
        if let Some(position) = &self.last_position {
            if position.amt.abs() > f64::EPSILON {
                self.position_was_open = true;
                self.last_open_position = Some(position.clone());
            }
        }

        let time_str = format_event_time(event_time_ms).unwrap_or_else(|| "-".to_string());
        let position_display = self.position_display();
        let tick_line = format!("{symbol} / {price_str} / {time_str} / {position_display}");
        println!("{tick_line}");
        self.logger.tick(&tick_line);

        let active = match self.config.mode {
            TriggerMode::Below => price < self.config.trigger_price,
            TriggerMode::Above => price > self.config.trigger_price,
        };
        let active_changed = self.last_active.map_or(true, |prev| prev != active);
        if active_changed {
            let decision = if active { "activate" } else { "deactivate" };
            self.logger.event(&format!(
                "event=trigger_state symbol={symbol} mode={} decision={decision} active={active} trigger={} price={}",
                self.config.mode.as_str(),
                self.config.trigger_price,
                price_str
            ));
        }

        if let Err(err) = self.check_exit_on_filled(symbol).await {
            self.logger
                .error(&format!("event=exit_check_error err={err}"));
        }
        if self.exit_requested {
            return;
        }

        if self.should_manage(active) {
            if let Err(err) = self.manage_orders(symbol, active).await {
                self.logger
                    .error(&format!("event=order_manage_error err={err}"));
            }
            self.last_manage_at = Some(Instant::now());
        }

        if self.config.entry.is_some() && self.should_manage_entry() {
            if let Err(err) = self.manage_entry_orders(symbol, price).await {
                self.logger
                    .error(&format!("event=entry_manage_error err={err}"));
            }
            self.last_entry_manage_at = Some(Instant::now());
        }

        self.last_active = Some(active);
    }

    fn should_manage(&self, active: bool) -> bool {
        if self.last_active.map_or(true, |prev| prev != active) {
            return true;
        }

        if active {
            if let Some(last) = self.last_manage_at {
                return last.elapsed() >= Duration::from_secs(ORDER_MANAGE_INTERVAL_SECS);
            }
            return true;
        }

        false
    }

    fn should_manage_entry(&self) -> bool {
        match self.last_entry_manage_at {
            None => true,
            Some(last) => last.elapsed() >= Duration::from_secs(ENTRY_MANAGE_INTERVAL_SECS),
        }
    }

    async fn refresh_position_if_needed(
        &mut self,
        symbol: &str,
        force: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let should_refresh = match self.last_position_at {
            None => true,
            Some(last) => force || last.elapsed() >= Duration::from_secs(POSITION_REFRESH_SECS),
        };

        if should_refresh {
            let snapshot = self
                .client
                .get_position(symbol, self.config.base_asset.as_deref())
                .await?;
            self.last_position = Some(snapshot.clone());
            self.last_position_at = Some(Instant::now());
            self.event_with_price(&format!(
                "event=position_refresh symbol={symbol} side={} amt={}",
                snapshot.position_side, snapshot.amt_str
            ));
        }

        Ok(())
    }

    async fn get_open_orders_cached(
        &mut self,
        symbol: &str,
    ) -> Result<Vec<OpenOrder>, Box<dyn Error + Send + Sync>> {
        if let (Some(last), Some(cache)) = (self.open_orders_cached_at, self.open_orders_cache.as_ref())
        {
            if last.elapsed() < Duration::from_secs(OPEN_ORDERS_CACHE_SECS) {
                return Ok(cache.clone());
            }
        }

        let orders = self.client.get_open_orders(symbol).await?;
        self.open_orders_cache = Some(orders.clone());
        self.open_orders_cached_at = Some(Instant::now());
        Ok(orders)
    }

    fn clear_open_orders_cache(&mut self) {
        self.open_orders_cache = None;
        self.open_orders_cached_at = None;
    }

    async fn cancel_managed_orders(
        &mut self,
        symbol: &str,
    ) -> Result<(usize, usize), Box<dyn Error + Send + Sync>> {
        let orders = self.client.get_open_orders(symbol).await?;
        let conditional_orders = if matches!(self.config.market, MarketType::Futures) {
            self.client.get_open_conditional_orders(symbol).await?
        } else {
            Vec::new()
        };

        let managed_orders: Vec<OpenOrder> = orders
            .into_iter()
            .filter(|o| {
                o.client_order_id.starts_with(CLIENT_ID_PREFIX)
                    || o.client_order_id.starts_with(ENTRY_CLIENT_ID_PREFIX)
            })
            .collect();
        let managed_conditional: Vec<ConditionalOrder> = conditional_orders
            .into_iter()
            .filter(|o| o.client_strategy_id.starts_with(STOP_CLIENT_ID_PREFIX))
            .collect();

        if !managed_orders.is_empty() {
            self.cancel_orders(symbol, &managed_orders).await?;
        }
        if !managed_conditional.is_empty() {
            self.cancel_conditional_orders(symbol, &managed_conditional)
                .await?;
        }

        self.clear_open_orders_cache();

        Ok((managed_orders.len(), managed_conditional.len()))
    }

    async fn exit_with_reason(
        &mut self,
        symbol: &str,
        reason: &str,
        source: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.exit_requested {
            return Ok(());
        }

        let (orders, conditional) = self.cancel_managed_orders(symbol).await?;
        self.exit_requested = true;
        self.exit_reason = Some(format!("{reason}_{source}"));
        let log_line = format!(
            "event=exit reason={reason} source={source} canceled_orders={orders} canceled_conditional={conditional}"
        );
        self.logger.event(&log_line);

        let title = match reason {
            "tp_filled" | "order_filled" => "TAKE PROFIT FILLED",
            "stop_filled" => "STOP FILLED",
            "entry_abort_touched" => "ENTRY ABORT TRIGGERED",
            _ => "EXIT",
        };
        let mut alert = alerting::Alert::exit(
            title,
            alerting::AlertContext::for_symbol_market(self.config.market.as_str(), symbol),
        )
        .with_current_price(self.last_price_str.clone())
        .with_field("kind", reason)
        .with_field("source", source)
        .with_field("canceled_orders", orders.to_string())
        .with_field("canceled_conditional", conditional.to_string());

        if reason == "entry_abort_touched" {
            if let Some(entry) = &self.config.entry {
                alert = alert
                    .with_field("entry_price", entry.entry_price_str.clone())
                    .with_field("stop_price", entry.stop_price_str.clone());
            }
            let abort = self
                .config
                .entry_abort_price_str
                .as_deref()
                .unwrap_or("unset");
            alert = alert.with_field("abort_price", abort);
        } else if reason == "tp_filled" || reason == "order_filled" || reason == "stop_filled" {
            let fill = self.last_fill.take();
            let mut side: Option<String> = None;
            let mut qty: Option<String> = None;
            let mut price: Option<String> = None;
            let mut client_id: Option<String> = None;

            if let Some(fill) = fill {
                side = fill.side;
                qty = fill.qty;
                price = fill.price;
                client_id = fill.client_id;
            }

            if side.is_none() {
                if let Some(pos) = &self.last_open_position {
                    if pos.amt.abs() > f64::EPSILON {
                        let close_side = if pos.amt > 0.0 { "SELL" } else { "BUY" };
                        side = Some(close_side.to_string());
                    }
                }
            }
            if qty.is_none() {
                if let Some(pos) = &self.last_open_position {
                    if pos.amt.abs() > f64::EPSILON {
                        qty = Some(abs_str(&pos.amt_str));
                    }
                }
            }

            if price.is_none() {
                if reason == "stop_filled" {
                    if let Some(entry) = &self.config.entry {
                        price = Some(entry.stop_price_str.clone());
                    }
                } else {
                    price = Some(self.config.order_price_str.clone());
                }
            }
            if client_id.is_none() {
                if reason == "stop_filled" {
                    client_id = self.stop_client_id.clone();
                } else {
                    client_id = self.tp_client_id.clone();
                }
            }

            if let Some(value) = side {
                alert = alert.with_field("side", value);
            }
            if let Some(value) = qty {
                alert = alert.with_field("qty", value);
            }
            if let Some(value) = price {
                alert = alert.with_field("price", value);
            }
            if let Some(value) = client_id {
                alert = alert.with_field("client_id", value);
            }
        }

        self.logger.send_alert(alert);
        Ok(())
    }

    async fn check_exit_on_filled(
        &mut self,
        symbol: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.exit_requested {
            return Ok(());
        }

        let position = match &self.last_position {
            Some(position) => position,
            None => return Ok(()),
        };

        if !self.position_was_open {
            return Ok(());
        }

        if position.amt.abs() > f64::EPSILON {
            return Ok(());
        }

        let mut exit_reason: Option<String> = None;

        if matches!(self.config.market, MarketType::Futures) {
            if let Some(stop_id) = self.stop_client_id.clone() {
                let history = self
                    .client
                    .get_conditional_order_history(symbol, &stop_id)
                    .await?;
                if is_conditional_filled(&history) {
                    exit_reason = Some("stop_filled".to_string());
                }
            }
        }

        if exit_reason.is_none() {
            if let Some(tp_id) = self.tp_client_id.clone() {
                let status = self.client.get_order_status(symbol, &tp_id).await?;
                if is_filled_status(&status.status) {
                    exit_reason = Some("tp_filled".to_string());
                }
            }
        }

        if let Some(reason) = exit_reason {
            self.exit_with_reason(symbol, &reason, "rest").await?;
        }

        Ok(())
    }

    async fn ensure_symbol_filters(
        &mut self,
        symbol: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.symbol_filters.is_some() {
            return Ok(());
        }

        if let Some(last) = self.last_filters_attempt_at {
            if last.elapsed() < Duration::from_secs(SYMBOL_FILTERS_REFRESH_SECS) {
                return Ok(());
            }
        }

        self.last_filters_attempt_at = Some(Instant::now());
        match self.client.get_symbol_filters(symbol).await {
            Ok(filters) => {
                self.logger.event(&format!(
                    "event=symbol_filters symbol={symbol} lot_step={} lot_precision={} exchange_base_url={}",
                    filters.lot_step_str,
                    filters.lot_precision,
                    self.client.exchange_base_url
                ));
                self.symbol_filters = Some(filters);
            }
            Err(err) => {
                self.logger
                    .error(&format!("event=symbol_filters_error symbol={symbol} err={err}"));
            }
        }

        Ok(())
    }

    async fn manage_orders(
        &mut self,
        symbol: &str,
        active: bool,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.refresh_position_if_needed(symbol, true).await?;
        self.event_with_price(&format!(
            "event=manage_orders symbol={symbol} active={active} position={}",
            self.position_display()
        ));

        let position = match &self.last_position {
            Some(position) => position.clone(),
            None => {
                self.event_with_price(&format!(
                    "event=manage_skip symbol={symbol} reason=no_position"
                ));
                return Ok(());
            }
        };

        if position.amt.abs() < f64::EPSILON {
            self.event_with_price(&format!(
                "event=manage_skip symbol={symbol} reason=flat"
            ));
            return Ok(());
        }

        let market = self.config.market;
        let mut expected_qty_str = abs_str(&position.amt_str);
        let mut expected_qty = expected_qty_str.parse::<f64>().unwrap_or(0.0);
        if matches!(market, MarketType::Spot) {
            self.ensure_symbol_filters(symbol).await?;
            if let Some(filters) = &self.symbol_filters {
                let (rounded_qty, rounded_str) = filters.round_qty(expected_qty);
                if rounded_qty <= f64::EPSILON {
                    self.event_with_price(&format!(
                        "event=manage_skip symbol={symbol} reason=qty_too_small"
                    ));
                    return Ok(());
                }
                expected_qty = rounded_qty;
                expected_qty_str = rounded_str;
            }
        }
        let expected_side = if position.amt > 0.0 { "SELL" } else { "BUY" };
        let expected_position_side = position.position_side.as_str();

        if active {
            let orders = self.get_open_orders_cached(symbol).await?;
            let managed_orders: Vec<OpenOrder> = orders
                .into_iter()
                .filter(|o| {
                    o.client_order_id.starts_with(CLIENT_ID_PREFIX)
                        && (matches!(market, MarketType::Spot) || o.reduce_only)
                })
                .collect();
            self.tp_client_id = managed_orders.first().map(|o| o.client_order_id.clone());
            let mut has_expected = false;

            for order in &managed_orders {
                if is_expected_order(
                    order,
                    market,
                    expected_side,
                    expected_position_side,
                    expected_qty,
                    self.config.order_price,
                ) {
                    has_expected = true;
                    continue;
                }
                has_expected = false;
                break;
            }

            if has_expected && managed_orders.len() == 1 {
                self.event_with_price(&format!(
                    "event=orders_ok symbol={symbol} price={} qty={}",
                    self.config.order_price_str, expected_qty_str
                ));
                return Ok(());
            }

            if !managed_orders.is_empty() {
                self.event_with_price(&format!(
                    "event=cancel_orders symbol={symbol} count={}",
                    managed_orders.len()
                ));
                self.cancel_orders(symbol, &managed_orders).await?;
                self.tp_client_id = None;
            }

            let client_order_id = format!("{CLIENT_ID_PREFIX}{}", now_millis());
            let order = self
                .client
                .place_reduce_only_limit(
                    symbol,
                    expected_side,
                    &expected_qty_str,
                    &self.config.order_price_str,
                    expected_position_side,
                    &client_order_id,
                )
                .await?;
            self.event_with_price(&format!(
                "event=place_order symbol={symbol} order_id={} client_id={} side={} price={} qty={}",
                order.order_id,
                order.client_order_id,
                expected_side,
                self.config.order_price_str,
                expected_qty_str
            ));
            self.clear_open_orders_cache();
            self.tp_client_id = Some(order.client_order_id);
        } else {
            let orders = self.get_open_orders_cached(symbol).await?;
            let managed_orders: Vec<OpenOrder> = orders
                .into_iter()
                .filter(|o| {
                    o.client_order_id.starts_with(CLIENT_ID_PREFIX)
                        && (matches!(market, MarketType::Spot) || o.reduce_only)
                })
                .collect();

            if !managed_orders.is_empty() {
                self.event_with_price(&format!(
                    "event=cancel_orders symbol={symbol} count={}",
                    managed_orders.len()
                ));
                self.cancel_orders(symbol, &managed_orders).await?;
                self.tp_client_id = None;
            } else {
                self.event_with_price(&format!(
                    "event=cancel_skip symbol={symbol} reason=none"
                ));
            }
        }

        Ok(())
    }

    async fn manage_entry_orders(
        &mut self,
        symbol: &str,
        price: f64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if matches!(self.config.market, MarketType::Spot) {
            return Ok(());
        }
        let entry_detect = self.config.entry_detect;
        let entry = match self.config.entry.clone() {
            Some(entry) => entry,
            None => return Ok(()),
        };
        let expected_entry_side = entry.side.entry_side();
        let expected_stop_side = entry.side.stop_side();

        self.refresh_position_if_needed(symbol, true).await?;
        let orders = self.client.get_open_orders(symbol).await?;
        let conditional_orders = self.client.get_open_conditional_orders(symbol).await?;
        let mut entry_orders: Vec<OpenOrder> = orders
            .iter()
            .filter(|o| match entry_detect {
                EntryDetect::Prefix => o.client_order_id.starts_with(ENTRY_CLIENT_ID_PREFIX),
                EntryDetect::Any => is_entry_candidate(o, expected_entry_side, entry.entry_price),
            })
            .cloned()
            .collect();
        let entry_orders_start_count = entry_orders.len();
        let had_entry_orders_at_start = entry_orders_start_count > 0;
        if let Some(order) = entry_orders.first() {
            self.entry_client_id = Some(order.client_order_id.clone());
        }
        let mut stop_orders: Vec<ConditionalOrder> = conditional_orders
            .into_iter()
            .filter(|o| o.client_strategy_id.starts_with(STOP_CLIENT_ID_PREFIX))
            .collect();
        let should_startup_cleanup = !self.entry_startup_cleanup_done
            && entry.entry_usdc_provided
            && entry.leverage_provided;
        if should_startup_cleanup {
            let same_price_entry_orders: Vec<OpenOrder> = entry_orders
                .iter()
                .filter(|o| is_same_price_entry_order(o, expected_entry_side, entry.entry_price))
                .cloned()
                .collect();
            let same_price_stop_orders: Vec<ConditionalOrder> = stop_orders
                .iter()
                .filter(|o| is_same_price_stop_order(o, expected_stop_side, entry.stop_price))
                .cloned()
                .collect();

            if !same_price_entry_orders.is_empty() || !same_price_stop_orders.is_empty() {
                self.event_with_price(&format!(
                    "event=entry_startup_cleanup symbol={symbol} entry_cancel={} stop_cancel={}",
                    same_price_entry_orders.len(),
                    same_price_stop_orders.len()
                ));
                if !same_price_entry_orders.is_empty() {
                    self.cancel_orders(symbol, &same_price_entry_orders).await?;
                    entry_orders.retain(|o| {
                        !is_same_price_entry_order(o, expected_entry_side, entry.entry_price)
                    });
                }
                if !same_price_stop_orders.is_empty() {
                    self.cancel_conditional_orders(symbol, &same_price_stop_orders)
                        .await?;
                    stop_orders.retain(|o| {
                        !is_same_price_stop_order(o, expected_stop_side, entry.stop_price)
                    });
                }
                self.clear_open_orders_cache();
            }
            self.entry_startup_cleanup_done = true;
        }
        self.stop_client_id = stop_orders.first().map(|o| o.client_strategy_id.clone());

        let has_entry_orders = !entry_orders.is_empty();
        if !self.entry_abort_checked {
            self.entry_abort_checked = true;
            if self.config.entry_abort_price.is_some() && had_entry_orders_at_start {
                self.event_with_price(&format!(
                    "event=entry_abort_disabled symbol={symbol} reason=existing_entry_orders count={}",
                    entry_orders_start_count
                ));
                self.config.entry_abort_price = None;
                self.config.entry_abort_price_str = None;
            }
        }
        let position = self.last_position.clone().unwrap_or(PositionSnapshot {
            amt: 0.0,
            amt_str: "0".to_string(),
            position_side: "BOTH".to_string(),
        });
        let position_qty_str = abs_str(&position.amt_str);
        let position_qty = position_qty_str.parse::<f64>().unwrap_or(0.0);
        let has_position = position.amt.abs() > f64::EPSILON;
        if has_position && !self.entry_completed {
            self.entry_completed = true;
            self.event_with_price(&format!("event=entry_completed symbol={symbol}"));

            let mut alert = alerting::Alert::fill(
                "ENTRY FILLED",
                alerting::AlertContext::for_symbol_market(self.config.market.as_str(), symbol),
            )
            .with_field("kind", "entry_filled");
            let side = if position.amt > 0.0 { "LONG" } else { "SHORT" };
            alert = alert.with_field("side", side);
            alert = alert.with_field("qty", position_qty_str.clone());
            if let Some(price_str) = &self.last_price_str {
                alert = alert.with_field("price", price_str.clone());
            }
            if let Some(client_id) = &self.entry_client_id {
                alert = alert.with_field("client_id", client_id.clone());
            }
            alert = alert
                .with_field("entry_price", entry.entry_price_str.clone())
                .with_field("stop_price", entry.stop_price_str.clone());
            self.logger.send_alert(alert);
        }
        let mut entry_qty = entry.entry_qty();
        let mut entry_qty_reason: Option<&str> = None;
        if entry_qty.is_none() {
            if entry.entry_usdc.is_none() {
                entry_qty_reason = Some("missing_entry_usdc");
            } else {
                entry_qty_reason = Some("invalid_entry_qty");
            }
        } else {
            self.ensure_symbol_filters(symbol).await?;
            if let (Some(filters), Some((raw_qty, _))) =
                (self.symbol_filters.as_ref(), entry_qty.clone())
            {
                let (rounded_qty, rounded_str) = filters.round_qty(raw_qty);
                if rounded_qty <= f64::EPSILON {
                    if !self.entry_round_logged {
                        self.event_with_price(&format!(
                            "event=entry_qty_round_skip symbol={symbol} raw_qty={} step={}",
                            format_qty(raw_qty),
                            filters.lot_step_str
                        ));
                        self.entry_round_logged = true;
                    }
                    entry_qty = None;
                    entry_qty_reason = Some("entry_qty_too_small");
                } else {
                    if !approx_eq(raw_qty, rounded_qty) && !self.entry_round_logged {
                        self.event_with_price(&format!(
                            "event=entry_qty_round symbol={symbol} raw_qty={} rounded_qty={} step={}",
                            format_qty(raw_qty),
                            rounded_str,
                            filters.lot_step_str
                        ));
                        self.entry_round_logged = true;
                    }
                    entry_qty = Some((rounded_qty, rounded_str));
                }
            }
        }
        let entry_order_qty_str = entry_orders.first().map(|order| order.orig_qty.clone());
        let entry_order_qty = entry_order_qty_str
            .as_deref()
            .and_then(|qty| qty.parse::<f64>().ok());
        if entry_qty.is_some() || has_entry_orders {
            self.entry_missing_logged = false;
        }

        let entry_ready = has_entry_orders || entry_qty.is_some();
        if let Some(entry_abort) = self.config.entry_abort_price {
            if entry_ready
                && !self.entry_completed
                && !has_position
                && entry_abort_touched(entry.entry_price, entry_abort, price)
            {
                let abort_str = self
                    .config
                    .entry_abort_price_str
                    .as_deref()
                    .unwrap_or("unset");
                self.event_with_price(&format!(
                    "event=entry_abort_touched symbol={symbol} abort={abort_str}"
                ));
                let extra_entry_orders: Vec<OpenOrder> = entry_orders
                    .iter()
                    .filter(|order| !order.client_order_id.starts_with(ENTRY_CLIENT_ID_PREFIX))
                    .cloned()
                    .collect();
                if !extra_entry_orders.is_empty() {
                    self.cancel_orders(symbol, &extra_entry_orders).await?;
                }
                self.exit_with_reason(symbol, "entry_abort_touched", "tick")
                    .await?;
                return Ok(());
            }
        }

        if self.entry_completed {
            // Entry already filled once; do not place new entry orders.
        } else if let Some((entry_qty, entry_qty_str)) = entry_qty.clone() {
            let mut has_expected = false;

            for order in &entry_orders {
                if is_expected_entry_order(order, expected_entry_side, entry.entry_price, entry_qty)
                {
                    has_expected = true;
                    continue;
                }
                has_expected = false;
                break;
            }

            if has_expected && entry_orders.len() == 1 {
                self.event_with_price(&format!(
                    "event=entry_orders_ok symbol={symbol} price={} qty={}",
                    entry.entry_price_str, entry_qty_str
                ));
            } else {
                if !entry_orders.is_empty() {
                    self.event_with_price(&format!(
                        "event=entry_cancel_orders symbol={symbol} count={}",
                        entry_orders.len()
                    ));
                    self.cancel_orders(symbol, &entry_orders).await?;
                }

                let client_order_id = format!("{ENTRY_CLIENT_ID_PREFIX}{}", now_millis());
                let order = self
                    .client
                    .place_entry_limit(
                        symbol,
                        expected_entry_side,
                        &entry_qty_str,
                        &entry.entry_price_str,
                        &client_order_id,
                    )
                    .await?;
                self.event_with_price(&format!(
                    "event=entry_place_order symbol={symbol} order_id={} client_id={} side={} price={} qty={}",
                    order.order_id,
                    order.client_order_id,
                    expected_entry_side,
                    entry.entry_price_str,
                    entry_qty_str
                ));
                self.entry_client_id = Some(order.client_order_id.clone());
                self.clear_open_orders_cache();
            }
        } else if entry_orders.is_empty() {
            if !self.entry_missing_logged {
                let reason = entry_qty_reason.unwrap_or("missing_entry_usdc");
                self.event_with_price(&format!(
                    "event=entry_missing_amount symbol={symbol} reason={reason}"
                ));
                self.entry_missing_logged = true;
            }
        }

        let should_have_stop = has_entry_orders || has_position;
        if should_have_stop {
            let (stop_qty, stop_qty_str) = if position_qty > f64::EPSILON {
                (position_qty, position_qty_str.clone())
            } else if let Some((entry_qty, entry_qty_str)) = entry_qty.clone() {
                (entry_qty, entry_qty_str)
            } else if let (Some(entry_order_qty), Some(entry_order_qty_str)) =
                (entry_order_qty, entry_order_qty_str.as_deref())
            {
                (entry_order_qty, entry_order_qty_str.to_string())
            } else {
                (0.0, "0".to_string())
            };

            if stop_qty <= f64::EPSILON {
                self.event_with_price(&format!(
                    "event=stop_skip symbol={symbol} reason=no_qty"
                ));
                return Ok(());
            }

            let expected_side = entry.side.stop_side();
            let mut has_expected = false;

            for order in &stop_orders {
                if is_expected_stop_order(order, expected_side, entry.stop_price, stop_qty) {
                    has_expected = true;
                    continue;
                }
                has_expected = false;
                break;
            }

            if has_expected && stop_orders.len() == 1 {
                self.event_with_price(&format!(
                    "event=stop_orders_ok symbol={symbol} stop={} qty={}",
                    entry.stop_price_str, stop_qty_str
                ));
                return Ok(());
            }

            if !stop_orders.is_empty() {
                self.event_with_price(&format!(
                    "event=stop_cancel_orders symbol={symbol} count={}",
                    stop_orders.len()
                ));
                self.cancel_conditional_orders(symbol, &stop_orders).await?;
                self.stop_client_id = None;
            }

            let client_order_id = format!("{STOP_CLIENT_ID_PREFIX}{}", now_millis());
            let position_side = if position.position_side != "BOTH" {
                Some(position.position_side.as_str())
            } else {
                None
            };
            let order = self
                .client
                .place_stop_market(
                    symbol,
                    expected_side,
                    &stop_qty_str,
                    &entry.stop_price_str,
                    position_side,
                    &client_order_id,
                )
                .await?;
            self.event_with_price(&format!(
                "event=stop_place_order symbol={symbol} order_id={} client_id={} side={} stop={} qty={}",
                order.order_id,
                order.client_order_id,
                expected_side,
                entry.stop_price_str,
                stop_qty_str
            ));
            self.stop_client_id = Some(order.client_order_id);
        } else if !stop_orders.is_empty() {
            self.event_with_price(&format!(
                "event=stop_cancel_orders symbol={symbol} count={}",
                stop_orders.len()
            ));
            self.cancel_conditional_orders(symbol, &stop_orders).await?;
            self.stop_client_id = None;
        }

        Ok(())
    }

    async fn cancel_orders(
        &mut self,
        symbol: &str,
        orders: &[OpenOrder],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for order in orders {
            self.client.cancel_order(symbol, order.order_id).await?;
            self.event_with_price(&format!(
                "event=cancel_order symbol={symbol} order_id={} client_id={}",
                order.order_id, order.client_order_id
            ));
        }
        if !orders.is_empty() {
            self.clear_open_orders_cache();
        }
        Ok(())
    }

    async fn cancel_conditional_orders(
        &self,
        symbol: &str,
        orders: &[ConditionalOrder],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for order in orders {
            self.client
                .cancel_conditional_order(symbol, order.strategy_id)
                .await?;
            self.event_with_price(&format!(
                "event=cancel_order symbol={symbol} order_id={} client_id={}",
                order.strategy_id, order.client_strategy_id
            ));
        }
        Ok(())
    }

    fn position_display(&self) -> String {
        match &self.last_position {
            Some(position) if position.amt > 0.0 => {
                format!("LONG {}", abs_str(&position.amt_str))
            }
            Some(position) if position.amt < 0.0 => {
                format!("SHORT {}", abs_str(&position.amt_str))
            }
            Some(_) => "FLAT".to_string(),
            None => "UNKNOWN".to_string(),
        }
    }
}

impl BinanceClient {
    async fn get_position(
        &self,
        symbol: &str,
        base_asset: Option<&str>,
    ) -> Result<PositionSnapshot, Box<dyn Error + Send + Sync>> {
        match self.market {
            MarketType::Futures => {
                let params = vec![("symbol".to_string(), symbol.to_string())];
                let positions: Vec<PositionRisk> = self
                    .signed_request(Method::GET, FUTURES_POSITION_RISK_PATH, params)
                    .await?;

                let position = positions
                    .into_iter()
                    .find(|pos| pos.symbol == symbol)
                    .unwrap_or(PositionRisk {
                        symbol: symbol.to_string(),
                        position_amt: "0".to_string(),
                        position_side: "BOTH".to_string(),
                    });

                let amt = position.position_amt.parse::<f64>().unwrap_or(0.0);
                Ok(PositionSnapshot {
                    amt,
                    amt_str: position.position_amt,
                    position_side: position.position_side,
                })
            }
            MarketType::Spot => {
                let base_asset = base_asset.ok_or("spot base asset missing")?;
                let account: AccountInfo =
                    self.signed_request(Method::GET, SPOT_ACCOUNT_PATH, Vec::new())
                        .await?;
                let balance = account
                    .balances
                    .into_iter()
                    .find(|item| item.asset == base_asset)
                    .unwrap_or(AccountBalance {
                        asset: base_asset.to_string(),
                        free: "0".to_string(),
                        locked: "0".to_string(),
                    });
                let free = balance.free.parse::<f64>().unwrap_or(0.0);
                let locked = balance.locked.parse::<f64>().unwrap_or(0.0);
                let total = free + locked;
                Ok(PositionSnapshot {
                    amt: total,
                    amt_str: format_qty(total),
                    position_side: "BOTH".to_string(),
                })
            }
        }
    }

    async fn get_open_orders(
        &self,
        symbol: &str,
    ) -> Result<Vec<OpenOrder>, Box<dyn Error + Send + Sync>> {
        let params = vec![("symbol".to_string(), symbol.to_string())];
        let path = match self.market {
            MarketType::Futures => FUTURES_OPEN_ORDERS_PATH,
            MarketType::Spot => SPOT_OPEN_ORDERS_PATH,
        };
        self.signed_request(Method::GET, path, params).await
    }

    async fn get_open_conditional_orders(
        &self,
        symbol: &str,
    ) -> Result<Vec<ConditionalOrder>, Box<dyn Error + Send + Sync>> {
        if matches!(self.market, MarketType::Spot) {
            return Ok(Vec::new());
        }
        let params = vec![("symbol".to_string(), symbol.to_string())];
        self.signed_request(Method::GET, FUTURES_OPEN_CONDITIONAL_ORDERS_PATH, params)
            .await
    }

    async fn start_user_stream(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let path = match self.market {
            MarketType::Futures => FUTURES_USER_STREAM_LISTEN_KEY_PATH,
            MarketType::Spot => SPOT_USER_STREAM_LISTEN_KEY_PATH,
        };
        let response: ListenKeyResponse = self
            .api_key_request(Method::POST, path, Vec::new())
            .await?;
        Ok(response.listen_key)
    }

    async fn keepalive_user_stream(&self, listen_key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = match self.market {
            MarketType::Futures => FUTURES_USER_STREAM_LISTEN_KEY_PATH,
            MarketType::Spot => SPOT_USER_STREAM_LISTEN_KEY_PATH,
        };
        let params = vec![("listenKey".to_string(), listen_key.to_string())];
        let _: Value = self.api_key_request(Method::PUT, path, params).await?;
        Ok(())
    }

    async fn close_user_stream(&self, listen_key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = match self.market {
            MarketType::Futures => FUTURES_USER_STREAM_LISTEN_KEY_PATH,
            MarketType::Spot => SPOT_USER_STREAM_LISTEN_KEY_PATH,
        };
        let params = vec![("listenKey".to_string(), listen_key.to_string())];
        let _: Value = self
            .api_key_request(Method::DELETE, path, params)
            .await?;
        Ok(())
    }

    async fn get_order_status(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<OrderStatus, Box<dyn Error + Send + Sync>> {
        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            (
                "origClientOrderId".to_string(),
                client_order_id.to_string(),
            ),
        ];
        let path = match self.market {
            MarketType::Futures => FUTURES_ORDER_PATH,
            MarketType::Spot => SPOT_ORDER_PATH,
        };
        self.signed_request(Method::GET, path, params).await
    }

    async fn get_conditional_order_history(
        &self,
        symbol: &str,
        client_strategy_id: &str,
    ) -> Result<ConditionalOrderHistory, Box<dyn Error + Send + Sync>> {
        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            (
                "newClientStrategyId".to_string(),
                client_strategy_id.to_string(),
            ),
        ];
        if matches!(self.market, MarketType::Spot) {
            return Err("spot conditional order history not supported".into());
        }
        self.signed_request(Method::GET, FUTURES_CONDITIONAL_ORDER_HISTORY_PATH, params)
            .await
    }

    async fn place_reduce_only_limit(
        &self,
        symbol: &str,
        side: &str,
        quantity: &str,
        price: &str,
        position_side: &str,
        client_order_id: &str,
    ) -> Result<OrderAck, Box<dyn Error + Send + Sync>> {
        let (path, params) = match self.market {
            MarketType::Futures => {
                let mut params = vec![
                    ("symbol".to_string(), symbol.to_string()),
                    ("side".to_string(), side.to_string()),
                    ("type".to_string(), "LIMIT".to_string()),
                    ("timeInForce".to_string(), "GTX".to_string()),
                    ("quantity".to_string(), quantity.to_string()),
                    ("price".to_string(), price.to_string()),
                    ("reduceOnly".to_string(), "true".to_string()),
                    (
                        "newClientOrderId".to_string(),
                        client_order_id.to_string(),
                    ),
                ];
                if position_side != "BOTH" {
                    params.push(("positionSide".to_string(), position_side.to_string()));
                }
                (FUTURES_ORDER_PATH, params)
            }
            MarketType::Spot => {
                let params = vec![
                    ("symbol".to_string(), symbol.to_string()),
                    ("side".to_string(), side.to_string()),
                    ("type".to_string(), "LIMIT_MAKER".to_string()),
                    ("quantity".to_string(), quantity.to_string()),
                    ("price".to_string(), price.to_string()),
                    (
                        "newClientOrderId".to_string(),
                        client_order_id.to_string(),
                    ),
                ];
                (SPOT_ORDER_PATH, params)
            }
        };

        self.signed_request(Method::POST, path, params).await
    }

    async fn place_entry_limit(
        &self,
        symbol: &str,
        side: &str,
        quantity: &str,
        price: &str,
        client_order_id: &str,
    ) -> Result<OrderAck, Box<dyn Error + Send + Sync>> {
        if matches!(self.market, MarketType::Spot) {
            return Err("spot entry orders not supported".into());
        }
        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_string()),
            ("type".to_string(), "LIMIT".to_string()),
            ("timeInForce".to_string(), "GTX".to_string()),
            ("quantity".to_string(), quantity.to_string()),
            ("price".to_string(), price.to_string()),
            (
                "newClientOrderId".to_string(),
                client_order_id.to_string(),
            ),
        ];

        self.signed_request(Method::POST, FUTURES_ORDER_PATH, params)
            .await
    }

    async fn place_stop_market(
        &self,
        symbol: &str,
        side: &str,
        quantity: &str,
        stop_price: &str,
        position_side: Option<&str>,
        client_order_id: &str,
    ) -> Result<OrderAck, Box<dyn Error + Send + Sync>> {
        if matches!(self.market, MarketType::Spot) {
            return Err("spot stop orders not supported".into());
        }
        let mut params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_string()),
            ("strategyType".to_string(), "STOP_MARKET".to_string()),
            ("stopPrice".to_string(), stop_price.to_string()),
            ("quantity".to_string(), quantity.to_string()),
            ("reduceOnly".to_string(), "true".to_string()),
            ("workingType".to_string(), "CONTRACT_PRICE".to_string()),
            (
                "newClientStrategyId".to_string(),
                client_order_id.to_string(),
            ),
        ];

        if let Some(position_side) = position_side {
            params.push(("positionSide".to_string(), position_side.to_string()));
        }

        self.signed_request(Method::POST, FUTURES_CONDITIONAL_ORDER_PATH, params)
            .await
    }

    async fn cancel_order(
        &self,
        symbol: &str,
        order_id: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("orderId".to_string(), order_id.to_string()),
        ];
        let path = match self.market {
            MarketType::Futures => FUTURES_ORDER_PATH,
            MarketType::Spot => SPOT_ORDER_PATH,
        };
        let _: Value = self
            .signed_request(Method::DELETE, path, params)
            .await?;
        Ok(())
    }

    async fn cancel_conditional_order(
        &self,
        symbol: &str,
        strategy_id: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if matches!(self.market, MarketType::Spot) {
            return Err("spot conditional order cancel not supported".into());
        }
        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("strategyId".to_string(), strategy_id.to_string()),
        ];
        let _: Value = self
            .signed_request(Method::DELETE, FUTURES_CONDITIONAL_ORDER_PATH, params)
            .await?;
        Ok(())
    }

    async fn get_symbol_filters(
        &self,
        symbol: &str,
    ) -> Result<SymbolFilters, Box<dyn Error + Send + Sync>> {
        let symbol_info = self.fetch_exchange_symbol(symbol).await?;
        let lot_filter = symbol_info
            .filters
            .into_iter()
            .find(|filter| filter.filter_type == "LOT_SIZE")
            .ok_or_else(|| format!("exchange info missing LOT_SIZE for {symbol}"))?;
        let (_step, precision, step_units) = parse_step_size(&lot_filter.step_size)
            .ok_or_else(|| format!("invalid LOT_SIZE stepSize={}", lot_filter.step_size))?;
        Ok(SymbolFilters {
            lot_step_str: lot_filter.step_size,
            lot_precision: precision,
            lot_step_units: step_units,
        })
    }

    async fn fetch_exchange_symbol(
        &self,
        symbol: &str,
    ) -> Result<ExchangeSymbol, Box<dyn Error + Send + Sync>> {
        let params = vec![("symbol".to_string(), symbol.to_string())];
        let info: ExchangeInfo = self
            .request_with_backoff_base(
                &self.exchange_base_url,
                Method::GET,
                self.market.exchange_info_path(),
                params,
                false,
            )
            .await?;
        info.symbols
            .into_iter()
            .find(|item| item.symbol == symbol)
            .ok_or_else(|| format!("exchange info missing symbol {symbol}").into())
    }

    fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string())
    }

    fn rate_limit_parts(headers: &HeaderMap) -> Vec<String> {
        let mut parts = Vec::new();
        if let Some(value) = Self::header_value(headers, "X-MBX-USED-WEIGHT") {
            parts.push(format!("used_weight={value}"));
        }
        if let Some(value) = Self::header_value(headers, "X-MBX-USED-WEIGHT-1S") {
            parts.push(format!("used_weight_1s={value}"));
        }
        if let Some(value) = Self::header_value(headers, "X-MBX-USED-WEIGHT-1M") {
            parts.push(format!("used_weight_1m={value}"));
        }
        if let Some(value) = Self::header_value(headers, "X-MBX-ORDER-COUNT-1S") {
            parts.push(format!("order_count_1s={value}"));
        }
        if let Some(value) = Self::header_value(headers, "X-MBX-ORDER-COUNT-1M") {
            parts.push(format!("order_count_1m={value}"));
        }
        parts
    }

    fn log_rate_limit_status(&self, path: &str, headers: &HeaderMap) {
        let parts = Self::rate_limit_parts(headers);
        if parts.is_empty() {
            return;
        }

        let now = Instant::now();
        let should_log = {
            let mut state = self.rate_limit_state.lock().unwrap();
            match state.last_logged_at {
                Some(last) if now.duration_since(last) < Duration::from_secs(RATE_LIMIT_LOG_SECS) => false,
                _ => {
                    state.last_logged_at = Some(now);
                    true
                }
            }
        };

        if should_log {
            self.logger
                .event(&format!("event=rate_limit_status path={path} {}", parts.join(" ")));
        }
    }

    fn retry_after_secs(headers: &HeaderMap) -> Option<u64> {
        headers
            .get("Retry-After")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
    }

    fn next_backoff_secs(&self, retry_after: Option<u64>) -> u64 {
        let mut state = self.rate_limit_state.lock().unwrap();
        let mut next = if state.backoff_secs == 0 {
            RATE_LIMIT_BACKOFF_INITIAL_SECS
        } else {
            (state.backoff_secs * 2).min(RATE_LIMIT_MAX_BACKOFF_SECS)
        };

        if let Some(retry_after) = retry_after {
            next = next.max(retry_after).min(RATE_LIMIT_MAX_BACKOFF_SECS);
        }

        state.backoff_secs = next;
        state.backoff_until = Some(Instant::now() + Duration::from_secs(next));
        next
    }

    fn reset_backoff(&self) {
        let mut state = self.rate_limit_state.lock().unwrap();
        state.backoff_secs = 0;
        state.backoff_until = None;
    }

    async fn wait_for_backoff(&self) {
        let wait = {
            let mut state = self.rate_limit_state.lock().unwrap();
            match state.backoff_until {
                Some(until) => {
                    let now = Instant::now();
                    if now < until {
                        Some(until - now)
                    } else {
                        state.backoff_until = None;
                        None
                    }
                }
                None => None,
            }
        };

        if let Some(duration) = wait {
            sleep(duration).await;
        }
    }

    async fn api_key_request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.request_with_backoff(method, path, params, false)
            .await
    }

    async fn signed_request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.request_with_backoff(method, path, params, true)
            .await
    }

    async fn request_with_backoff<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: Vec<(String, String)>,
        signed: bool,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        self.request_with_backoff_base(&self.base_url, method, path, params, signed)
            .await
    }

    async fn request_with_backoff_base<T: DeserializeOwned>(
        &self,
        base_url: &str,
        method: Method,
        path: &str,
        params: Vec<(String, String)>,
        signed: bool,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            self.wait_for_backoff().await;

            let url = if signed {
                let mut signed_params = params.clone();
                signed_params.push(("timestamp".to_string(), now_millis().to_string()));
                signed_params.push((
                    "recvWindow".to_string(),
                    RECV_WINDOW_MS.to_string(),
                ));
                let query = signed_params
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join("&");
                let signature = self.sign(&query)?;
                format!("{base_url}{path}?{query}&signature={signature}")
            } else if params.is_empty() {
                format!("{base_url}{path}")
            } else {
                let query = params
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join("&");
                format!("{base_url}{path}?{query}")
            };

            let response = self
                .http
                .request(method.clone(), &url)
                .header("X-MBX-APIKEY", &self.api_key)
                .send()
                .await?;

            let status = response.status();
            let headers = response.headers().clone();
            let body = response.text().await?;
            self.log_rate_limit_status(path, &headers);

            if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::IM_A_TEAPOT {
                let retry_after = Self::retry_after_secs(&headers);
                let backoff_secs = self.next_backoff_secs(retry_after);
                let retry_after_display = retry_after
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unset".to_string());
                self.logger.event(&format!(
                    "event=rate_limit_backoff status={} path={} attempt={} retry_after={} backoff_secs={}",
                    status.as_u16(),
                    path,
                    attempt,
                    retry_after_display,
                    backoff_secs
                ));

                if attempt < RATE_LIMIT_MAX_RETRIES {
                    continue;
                }
            } else {
                self.reset_backoff();
            }

            if !status.is_success() {
                return Err(format!("binance api error {}: {}", status, body).into());
            }

            let parsed = serde_json::from_str::<T>(&body)?;
            return Ok(parsed);
        }
    }

    fn sign(&self, payload: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|_| "invalid api secret")?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

async fn run_user_stream(
    client: BinanceClient,
    manager: SharedManager,
    logger: Logger,
    market: MarketType,
    symbol: String,
) {
    let mut backoff = Duration::from_secs(INITIAL_BACKOFF_SECS);
    let max_backoff = Duration::from_secs(MAX_BACKOFF_SECS);

    loop {
        if manager.lock().await.exit_requested() {
            break;
        }

        let listen_key = match client.start_user_stream().await {
            Ok(key) => {
                logger.event("event=user_stream_start");
                key
            }
            Err(err) => {
                logger.error(&format!("event=user_stream_start_error err={err}"));
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        let ws_url = format!("{}/{}", market.user_stream_ws_base_url(), listen_key);
        logger.event("event=user_stream_connect");
        match connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                logger.event("event=user_stream_connected");
                backoff = Duration::from_secs(INITIAL_BACKOFF_SECS);
                if let Err(err) =
                    stream_user_data(
                        ws_stream,
                        &client,
                        &manager,
                        &logger,
                        market,
                        &symbol,
                        &listen_key,
                    )
                    .await
                {
                    logger.error(&format!("event=user_stream_error err={err}"));
                }
            }
            Err(err) => {
                logger.error(&format!("event=user_stream_connect_error err={err}"));
            }
        }

        if let Err(err) = client.close_user_stream(&listen_key).await {
            logger.error(&format!("event=user_stream_close_error err={err}"));
        }

        if manager.lock().await.exit_requested() {
            break;
        }

        logger.event(&format!(
            "event=user_stream_reconnect_sleep seconds={}",
            backoff.as_secs()
        ));
        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn stream_user_data(
    ws_stream: WsStream,
    client: &BinanceClient,
    manager: &SharedManager,
    logger: &Logger,
    market: MarketType,
    symbol: &str,
    listen_key: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut write, mut read) = ws_stream.split();
    let mut ping_interval = interval(Duration::from_secs(PING_INTERVAL_SECS));
    let mut keepalive_interval = interval(Duration::from_secs(USER_STREAM_KEEPALIVE_SECS));
    let mut exit_interval = interval(Duration::from_secs(1));
    keepalive_interval.tick().await;

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(value) = serde_json::from_str::<Value>(&text) {
                            if let Some(event_type) = value.get("e").and_then(|value| value.as_str()) {
                                if event_type == "listenKeyExpired" {
                                    logger.event("event=user_stream_expired");
                                    return Ok(());
                                }
                            }
                            if let Some((event_symbol, reason, fill)) = parse_user_stream_exit(&value, market) {
                                if event_symbol == symbol {
                                    let mut guard = manager.lock().await;
                                    guard.last_fill = Some(fill);
                                    if let Err(err) = guard.exit_with_reason(&event_symbol, &reason, "ws").await {
                                        logger.error(&format!("event=exit_ws_error err={err}"));
                                    }
                                    if guard.exit_requested() {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        logger.event(&format!("event=user_stream_close frame={frame:?}"));
                        return Ok(());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        logger.event("event=user_stream_end");
                        return Ok(());
                    }
                }
            }
            _ = ping_interval.tick() => {
                write.send(Message::Ping(Vec::new())).await?;
            }
            _ = keepalive_interval.tick() => {
                if let Err(err) = client.keepalive_user_stream(listen_key).await {
                    logger.error(&format!("event=user_stream_keepalive_error err={err}"));
                    return Ok(());
                }
                logger.event("event=user_stream_keepalive");
            }
            _ = exit_interval.tick() => {
                if manager.lock().await.exit_requested() {
                    return Ok(());
                }
            }
        }
    }
}

async fn stream_last_price(
    ws_stream: WsStream,
    manager: SharedManager,
    symbol: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut write, mut read) = ws_stream.split();
    let mut ping_interval = interval(Duration::from_secs(PING_INTERVAL_SECS));

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some((event_symbol, price, event_time)) = parse_trade_event(&text) {
                            if !event_symbol.eq_ignore_ascii_case(&symbol) {
                                continue;
                            }
                            if let Ok(price_value) = price.parse::<f64>() {
                                let mut guard = manager.lock().await;
                                guard.handle_tick(&symbol, &price, price_value, event_time).await;
                                if guard.exit_requested() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        let guard = manager.lock().await;
                        guard
                            .logger
                            .event(&format!("event=ws_close frame={frame:?}"));
                        return Ok(());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        let guard = manager.lock().await;
                        guard.logger.event("event=ws_stream_end");
                        return Ok(());
                    }
                }
            }
            _ = ping_interval.tick() => {
                write.send(Message::Ping(Vec::new())).await?;
            }
        }
    }
}

fn parse_trade_event(text: &str) -> Option<(String, String, u64)> {
    let value: Value = serde_json::from_str(text).ok()?;
    let symbol = value.get("s")?.as_str()?;
    let price = value.get("p")?.as_str()?;
    let event_time = value.get("E")?.as_u64()?;
    Some((symbol.to_string(), price.to_string(), event_time))
}

fn format_event_time(event_time_ms: u64) -> Option<String> {
    let utc_time = DateTime::<Utc>::from_timestamp_millis(event_time_ms as i64)?;
    let offset = FixedOffset::east_opt(8 * 3600)?;
    Some(utc_time.with_timezone(&offset).format("%Y-%m-%d %H:%M:%S").to_string())
}

fn log_timestamp() -> String {
    let offset = FixedOffset::east_opt(8 * 3600).unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
    Utc::now()
        .with_timezone(&offset)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn abs_str(value: &str) -> String {
    value.strip_prefix('-').unwrap_or(value).to_string()
}

fn is_expected_order(
    order: &OpenOrder,
    market: MarketType,
    side: &str,
    position_side: &str,
    qty: f64,
    price: f64,
) -> bool {
    if order.side != side {
        return false;
    }

    match market {
        MarketType::Futures => {
            if order.time_in_force != "GTX" {
                return false;
            }
            if position_side != "BOTH" && order.position_side != position_side {
                return false;
            }
        }
        MarketType::Spot => {
            if order.order_type != "LIMIT_MAKER" {
                return false;
            }
        }
    }

    let order_price = order.price.parse::<f64>().unwrap_or(0.0);
    let order_qty = order.orig_qty.parse::<f64>().unwrap_or(0.0);

    approx_eq(order_price, price) && approx_eq(order_qty, qty)
}

fn is_expected_entry_order(order: &OpenOrder, side: &str, price: f64, qty: f64) -> bool {
    if order.side != side {
        return false;
    }

    if order.order_type != "LIMIT" {
        return false;
    }

    if order.time_in_force != "GTX" {
        return false;
    }

    if order.reduce_only {
        return false;
    }

    let order_price = order.price.parse::<f64>().unwrap_or(0.0);
    let order_qty = order.orig_qty.parse::<f64>().unwrap_or(0.0);

    approx_eq(order_price, price) && approx_eq(order_qty, qty)
}

fn is_same_price_entry_order(order: &OpenOrder, side: &str, price: f64) -> bool {
    is_entry_candidate(order, side, price)
}

fn is_entry_candidate(order: &OpenOrder, side: &str, price: f64) -> bool {
    if order.side != side {
        return false;
    }

    if order.order_type != "LIMIT" {
        return false;
    }

    if order.reduce_only {
        return false;
    }

    let order_price = order.price.parse::<f64>().unwrap_or(0.0);
    approx_eq(order_price, price)
}

fn is_same_price_stop_order(order: &ConditionalOrder, side: &str, stop_price: f64) -> bool {
    if order.side != side {
        return false;
    }

    if order.strategy_type != "STOP_MARKET" {
        return false;
    }

    if !order.reduce_only {
        return false;
    }

    let order_stop = order.stop_price.parse::<f64>().unwrap_or(0.0);
    approx_eq(order_stop, stop_price)
}

fn is_expected_stop_order(order: &ConditionalOrder, side: &str, stop_price: f64, qty: f64) -> bool {
    if order.side != side {
        return false;
    }

    if order.strategy_type != "STOP_MARKET" {
        return false;
    }

    if !order.reduce_only {
        return false;
    }

    let order_stop = order.stop_price.parse::<f64>().unwrap_or(0.0);
    let order_qty = order.orig_qty.parse::<f64>().unwrap_or(0.0);

    approx_eq(order_stop, stop_price) && approx_eq(order_qty, qty)
}

fn is_filled_status(status: &str) -> bool {
    status == "FILLED"
}

fn is_conditional_filled(order: &ConditionalOrderHistory) -> bool {
    if order.strategy_status != "TRIGGERED" {
        return false;
    }
    if order.status.is_empty() {
        return true;
    }
    is_filled_status(&order.status)
}

fn is_conditional_ws_filled_status(status: &str) -> bool {
    matches!(status, "TRIGGERED" | "FILLED")
}

fn parse_user_stream_exit(value: &Value, market: MarketType) -> Option<(String, String, FillInfo)> {
    let event_type = value.get("e")?.as_str()?;
    match market {
        MarketType::Futures => match event_type {
            "ORDER_TRADE_UPDATE" => {
                let order = value.get("o")?;
                let client_id = order.get("c")?.as_str()?;
                if !client_id.starts_with(CLIENT_ID_PREFIX) {
                    return None;
                }
                let status = order.get("X")?.as_str()?;
                if status != "FILLED" {
                    return None;
                }
                let symbol = order.get("s")?.as_str()?.to_string();
                let side = order.get("S").and_then(|value| value.as_str()).map(|v| v.to_string());
                let qty = order
                    .get("z")
                    .or_else(|| order.get("q"))
                    .and_then(|value| value.as_str())
                    .map(|v| v.to_string());
                let price = order
                    .get("ap")
                    .or_else(|| order.get("L"))
                    .and_then(|value| value.as_str())
                    .map(|v| v.to_string());
                let fill = FillInfo {
                    side,
                    qty,
                    price,
                    client_id: Some(client_id.to_string()),
                };
                Some((symbol, "tp_filled".to_string(), fill))
            }
            "CONDITIONAL_ORDER_TRADE_UPDATE" => {
                let order = value.get("so")?;
                let client_id = order.get("c")?.as_str()?;
                if !client_id.starts_with(STOP_CLIENT_ID_PREFIX) {
                    return None;
                }
                let status = order.get("os")?.as_str()?;
                if !is_conditional_ws_filled_status(status) {
                    return None;
                }
                let symbol = order.get("s")?.as_str()?.to_string();
                let side = order.get("S").and_then(|value| value.as_str()).map(|v| v.to_string());
                let qty = order
                    .get("q")
                    .and_then(|value| value.as_str())
                    .map(|v| v.to_string());
                let price = order
                    .get("ap")
                    .and_then(|value| value.as_str())
                    .map(|v| v.to_string());
                let fill = FillInfo {
                    side,
                    qty,
                    price,
                    client_id: Some(client_id.to_string()),
                };
                Some((symbol, "stop_filled".to_string(), fill))
            }
            _ => None,
        },
        MarketType::Spot => {
            if event_type != "executionReport" {
                return None;
            }
            let client_id = value.get("c")?.as_str()?;
            if !client_id.starts_with(CLIENT_ID_PREFIX) {
                return None;
            }
            let status = value.get("X")?.as_str()?;
            if status != "FILLED" {
                return None;
            }
            let symbol = value.get("s")?.as_str()?.to_string();
            let side = value.get("S").and_then(|value| value.as_str()).map(|v| v.to_string());
            let qty = value
                .get("z")
                .and_then(|value| value.as_str())
                .map(|v| v.to_string());
            let price = value
                .get("L")
                .and_then(|value| value.as_str())
                .map(|v| v.to_string());
            let fill = FillInfo {
                side,
                qty,
                price,
                client_id: Some(client_id.to_string()),
            };
            Some((symbol, "order_filled".to_string(), fill))
        }
    }
}

#[cfg(test)]
mod user_stream_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_user_stream_exit_futures_tp_filled_extracts_fields() {
        let value = json!({
            "e": "ORDER_TRADE_UPDATE",
            "o": {
                "c": "rb-tp-123",
                "X": "FILLED",
                "s": "BTCUSDC",
                "S": "SELL",
                "z": "0.1",
                "ap": "70000"
            }
        });
        let (symbol, reason, fill) =
            parse_user_stream_exit(&value, MarketType::Futures).expect("should parse");
        assert_eq!(symbol, "BTCUSDC");
        assert_eq!(reason, "tp_filled");
        assert_eq!(fill.side.as_deref(), Some("SELL"));
        assert_eq!(fill.qty.as_deref(), Some("0.1"));
        assert_eq!(fill.price.as_deref(), Some("70000"));
        assert_eq!(fill.client_id.as_deref(), Some("rb-tp-123"));
    }

    #[test]
    fn parse_user_stream_exit_futures_stop_filled_extracts_fields() {
        let value = json!({
            "e": "CONDITIONAL_ORDER_TRADE_UPDATE",
            "so": {
                "c": "rb-stop-999",
                "os": "TRIGGERED",
                "s": "BTCUSDC",
                "S": "SELL",
                "q": "0.2",
                "ap": "69950"
            }
        });
        let (symbol, reason, fill) =
            parse_user_stream_exit(&value, MarketType::Futures).expect("should parse");
        assert_eq!(symbol, "BTCUSDC");
        assert_eq!(reason, "stop_filled");
        assert_eq!(fill.side.as_deref(), Some("SELL"));
        assert_eq!(fill.qty.as_deref(), Some("0.2"));
        assert_eq!(fill.price.as_deref(), Some("69950"));
        assert_eq!(fill.client_id.as_deref(), Some("rb-stop-999"));
    }

    #[test]
    fn parse_user_stream_exit_spot_order_filled_extracts_fields() {
        let value = json!({
            "e": "executionReport",
            "c": "rb-tp-1",
            "X": "FILLED",
            "s": "BTCUSDC",
            "S": "SELL",
            "z": "0.05",
            "L": "70010"
        });
        let (symbol, reason, fill) =
            parse_user_stream_exit(&value, MarketType::Spot).expect("should parse");
        assert_eq!(symbol, "BTCUSDC");
        assert_eq!(reason, "order_filled");
        assert_eq!(fill.side.as_deref(), Some("SELL"));
        assert_eq!(fill.qty.as_deref(), Some("0.05"));
        assert_eq!(fill.price.as_deref(), Some("70010"));
        assert_eq!(fill.client_id.as_deref(), Some("rb-tp-1"));
    }

    #[test]
    fn parse_user_stream_exit_ignores_non_bot_orders() {
        let value = json!({
            "e": "executionReport",
            "c": "not-ours",
            "X": "FILLED",
            "s": "BTCUSDC",
            "S": "SELL",
            "z": "0.05",
            "L": "70010"
        });
        assert!(parse_user_stream_exit(&value, MarketType::Spot).is_none());
    }
}

fn approx_eq(a: f64, b: f64) -> bool {
    let diff = (a - b).abs();
    let scale = a.abs().max(b.abs()).max(1.0);
    diff <= 1e-8 * scale
}

fn entry_abort_touched(entry_price: f64, abort_price: f64, price: f64) -> bool {
    if approx_eq(price, abort_price) {
        return true;
    }
    if abort_price >= entry_price {
        price > abort_price
    } else {
        price < abort_price
    }
}

fn now_millis() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    now.as_millis() as u64
}

fn log_price_gap_warning(logger: &Logger, trigger: f64, order: f64) {
    if trigger.abs() < f64::EPSILON {
        return;
    }
    let diff = (order - trigger).abs();
    let ratio = diff / trigger.abs();
    if ratio >= 0.10 {
        logger.event(&format!(
            "event=price_gap_warning trigger={} order={} diff={} diff_pct={}",
            trigger,
            order,
            diff,
            format_percent(ratio)
        ));
    }
}

fn format_percent(value: f64) -> String {
    format!("{:.2}%", value * 100.0)
}

fn format_qty(value: f64) -> String {
    let mut text = format!("{:.8}", value);
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text.is_empty() {
        "0".to_string()
    } else {
        text
    }
}

fn step_precision(step: &str) -> usize {
    let trimmed = step.trim();
    match trimmed.find('.') {
        Some(dot) => trimmed[dot + 1..].trim_end_matches('0').len(),
        None => 0,
    }
}

fn parse_step_size(step: &str) -> Option<(f64, usize, u64)> {
    let value = step.parse::<f64>().ok()?;
    if value <= 0.0 {
        return None;
    }
    let precision = step_precision(step);
    let scale = 10u64.checked_pow(precision as u32)?;
    let step_units = (value * scale as f64).round() as u64;
    if step_units == 0 {
        return None;
    }
    Some((value, precision, step_units))
}

fn floor_to_step(value: f64, precision: usize, step_units: u64) -> f64 {
    if step_units == 0 {
        return value;
    }
    if value <= 0.0 {
        return 0.0;
    }
    let scale = 10u64.checked_pow(precision as u32).unwrap_or(1);
    let units = (value * scale as f64).floor() as u64;
    let steps = units / step_units;
    let rounded_units = steps * step_units;
    rounded_units as f64 / scale as f64
}

fn format_with_precision(value: f64, precision: usize) -> String {
    let mut text = if precision == 0 {
        format!("{:.0}", value)
    } else {
        format!("{:.*}", precision, value)
    };
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text.is_empty() {
        "0".to_string()
    } else {
        text
    }
}

fn log_path_from_env() -> PathBuf {
    env::var("RB_LOG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("rb.log"))
}

fn entry_usdc_from_env() -> Option<String> {
    env::var("RB_ENTRY_USDC").ok()
}

fn entry_leverage_from_env() -> Option<String> {
    env::var("RB_ENTRY_LEVERAGE").ok()
}

fn entry_detect_from_env() -> Option<String> {
    env::var("RB_ENTRY_DETECT").ok()
}

fn normalize_symbol(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut output = String::new();
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_uppercase());
        }
    }
    if output.is_empty() {
        None
    } else {
        Some(output)
    }
}

fn parse_market(value: &str) -> Option<MarketType> {
    match value.to_ascii_lowercase().as_str() {
        "futures" | "future" => Some(MarketType::Futures),
        "spot" => Some(MarketType::Spot),
        _ => None,
    }
}

fn parse_entry_side(value: &str) -> Option<EntrySide> {
    match value.to_ascii_lowercase().as_str() {
        "long" => Some(EntrySide::Long),
        "short" => Some(EntrySide::Short),
        _ => None,
    }
}

fn parse_entry_detect(value: &str) -> Option<EntryDetect> {
    match value.to_ascii_lowercase().as_str() {
        "prefix" => Some(EntryDetect::Prefix),
        "any" => Some(EntryDetect::Any),
        _ => None,
    }
}

fn parse_args() -> Result<Config, String> {
    let mut symbol_input: Option<String> = None;
    let mut market_input: Option<String> = None;
    let mut trigger_price: Option<String> = None;
    let mut order_price: Option<String> = None;
    let mut entry_price: Option<String> = None;
    let mut stop_price: Option<String> = None;
    let mut entry_side: Option<String> = None;
    let mut entry_usdc: Option<String> = None;
    let mut entry_leverage: Option<String> = None;
    let mut entry_detect: Option<String> = None;
    let mut entry_abort: Option<String> = None;
    let mut log_enabled = true;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--symbol" => symbol_input = args.next(),
            "--market" => market_input = args.next(),
            "--trigger" => trigger_price = args.next(),
            "--order" => order_price = args.next(),
            "--entry" => entry_price = args.next(),
            "--stop" => stop_price = args.next(),
            "--side" => entry_side = args.next(),
            "--entry-usdc" => entry_usdc = args.next(),
            "--leverage" => entry_leverage = args.next(),
            "--entry-detect" => entry_detect = args.next(),
            "--entry-abort" => entry_abort = args.next(),
            "--no-log" => log_enabled = false,
            "--help" | "-h" => return Err(usage()),
            _ => return Err(usage()),
        }
    }

    let symbol_raw = symbol_input
        .ok_or_else(|| "symbol is required\n".to_string() + &usage())?;
    let symbol = normalize_symbol(&symbol_raw)
        .ok_or_else(|| "invalid symbol format\n".to_string() + &usage())?;
    let market_raw = market_input
        .ok_or_else(|| "market is required\n".to_string() + &usage())?;
    let market = parse_market(&market_raw)
        .ok_or_else(|| "market must be futures or spot\n".to_string() + &usage())?;

    let trigger_str = trigger_price.ok_or_else(usage)?;
    let order_str = order_price.ok_or_else(usage)?;

    let trigger = trigger_str
        .parse::<f64>()
        .map_err(|_| usage())?;
    let order = order_str.parse::<f64>().map_err(|_| usage())?;

    if (order - trigger).abs() < f64::EPSILON {
        return Err("order price must not equal trigger price\n".to_string() + &usage());
    }

    let mode = if order < trigger {
        TriggerMode::Below
    } else {
        TriggerMode::Above
    };

    let entry_detect = match entry_detect.or_else(entry_detect_from_env) {
        Some(value) => parse_entry_detect(&value).ok_or_else(usage)?,
        None => EntryDetect::Prefix,
    };

    let entry = match (entry_price, stop_price, entry_side) {
        (None, None, None) => None,
        (Some(entry_str), Some(stop_str), Some(side_str)) => {
            let entry_price = entry_str.parse::<f64>().map_err(|_| usage())?;
            let stop_price = stop_str.parse::<f64>().map_err(|_| usage())?;
            let side = parse_entry_side(&side_str).ok_or_else(usage)?;

            let entry_usdc_str = entry_usdc.or_else(entry_usdc_from_env);
            let entry_usdc_provided = entry_usdc_str.is_some();
            let entry_usdc_value = match entry_usdc_str.as_deref() {
                Some(usdc_str) => {
                    let usdc = usdc_str.parse::<f64>().map_err(|_| usage())?;
                    if usdc <= 0.0 {
                        return Err("entry usdc must be > 0\n".to_string() + &usage());
                    }
                    Some(usdc)
                }
                None => None,
            };
            let leverage_str = entry_leverage.or_else(entry_leverage_from_env);
            let leverage_provided = leverage_str.is_some();
            let leverage = match leverage_str.as_deref() {
                Some(leverage_str) => {
                    let value = leverage_str.parse::<u32>().map_err(|_| usage())?;
                    if value == 0 {
                        return Err("entry leverage must be >= 1\n".to_string() + &usage());
                    }
                    value
                }
                None => 100,
            };

            Some(EntryConfig {
                entry_price,
                entry_price_str: entry_str,
                stop_price,
                stop_price_str: stop_str,
                side,
                entry_usdc: entry_usdc_value,
                entry_usdc_str,
                leverage,
                entry_usdc_provided,
                leverage_provided,
            })
        }
        _ => {
            return Err(
                "entry options require --entry --stop --side\n".to_string() + &usage(),
            )
        }
    };

    let entry_abort_parsed = match entry_abort {
        Some(value) => {
            if entry.is_none() {
                return Err(
                    "--entry-abort requires --entry --stop --side\n".to_string() + &usage(),
                );
            }
            let price = value.parse::<f64>().map_err(|_| usage())?;
            if price <= 0.0 {
                return Err("entry abort price must be > 0\n".to_string() + &usage());
            }
            Some((price, value))
        }
        None => None,
    };

    if matches!(market, MarketType::Spot) && entry.is_some() {
        return Err("spot does not support entry/stop options\n".to_string() + &usage());
    }

    Ok(Config {
        symbol,
        market,
        trigger_price: trigger,
        order_price: order,
        order_price_str: order_str,
        mode,
        log_enabled,
        entry,
        entry_detect,
        entry_abort_price: entry_abort_parsed.as_ref().map(|(price, _)| *price),
        entry_abort_price_str: entry_abort_parsed.map(|(_, value)| value),
        base_asset: None,
        quote_asset: None,
    })
}

fn usage() -> String {
    [
        "usage:",
        "  rb --symbol <pair> --market <futures|spot> --trigger <price> --order <price> [--entry <price> --stop <price> --side <long|short> --entry-usdc <amount> [--leverage <n>] [--entry-detect <prefix|any>] [--entry-abort <price>]] [--no-log]",
        "  note: entry/stop options are supported for futures only",
        "env:",
        "  BINANCE_API_KEY=... BINANCE_API_SECRET=... [BINANCE_BASE_URL=<market default>] [BINANCE_EXCHANGE_BASE_URL=<market default>] [RB_LOG_PATH=rb.log] [RB_ENTRY_USDC=<amount>] [RB_ENTRY_LEVERAGE=100] [RB_ENTRY_DETECT=prefix|any]",
        "example:",
        "  BINANCE_API_KEY=... BINANCE_API_SECRET=... RB_LOG_PATH=rb.log rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500",
    ]
    .join("\n")
}
