use std::env;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, Utc};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const WS_URL: &str = "wss://fstream.binance.com/ws/btcusdc@aggTrade";
const API_BASE_URL: &str = "https://papi.binance.com";
const POSITION_RISK_PATH: &str = "/papi/v1/um/positionRisk";
const OPEN_ORDERS_PATH: &str = "/papi/v1/um/openOrders";
const ORDER_PATH: &str = "/papi/v1/um/order";
const RECV_WINDOW_MS: u64 = 5000;
const INITIAL_BACKOFF_SECS: u64 = 1;
const MAX_BACKOFF_SECS: u64 = 32;
const PING_INTERVAL_SECS: u64 = 30;
const POSITION_REFRESH_SECS: u64 = 1;
const ORDER_MANAGE_INTERVAL_SECS: u64 = 1;
const CLIENT_ID_PREFIX: &str = "rb-tp-";

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

struct Config {
    trigger_price: f64,
    order_price: f64,
    order_price_str: String,
    mode: TriggerMode,
    log_enabled: bool,
}

#[derive(Clone)]
struct Logger {
    inner: Arc<LoggerInner>,
}

struct LoggerInner {
    path: PathBuf,
    file: Option<Mutex<std::fs::File>>,
    enabled: bool,
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
            Some(Mutex::new(
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
            }),
        })
    }

    fn path(&self) -> &Path {
        &self.inner.path
    }

    fn event(&self, message: &str) {
        self.log(LogLevel::Event, message, true);
    }

    fn tick(&self, message: &str) {
        self.log(LogLevel::Tick, message, false);
    }

    fn error(&self, message: &str) {
        self.log(LogLevel::Error, message, true);
    }

    fn log(&self, level: LogLevel, message: &str, print_stderr: bool) {
        let line = format!("[{}] {} {}", log_timestamp(), level.as_str(), message);
        if print_stderr {
            eprintln!("{line}");
        }
        if self.inner.enabled {
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

#[derive(Clone)]
struct BinanceClient {
    http: reqwest::Client,
    api_key: String,
    api_secret: String,
    base_url: String,
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
struct OpenOrder {
    #[serde(rename = "orderId")]
    order_id: u64,
    price: String,
    #[serde(rename = "origQty")]
    orig_qty: String,
    #[serde(rename = "reduceOnly")]
    reduce_only: bool,
    side: String,
    #[serde(rename = "positionSide")]
    position_side: String,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    #[serde(rename = "timeInForce")]
    time_in_force: String,
}

#[derive(Deserialize, Debug)]
struct OrderAck {
    #[serde(rename = "orderId")]
    order_id: u64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
}

struct OrderManager {
    client: BinanceClient,
    config: Config,
    logger: Logger,
    last_position: Option<PositionSnapshot>,
    last_position_at: Option<Instant>,
    last_manage_at: Option<Instant>,
    last_active: Option<bool>,
    last_price_str: Option<String>,
}

#[tokio::main]
async fn main() {
    let config = match parse_args() {
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
    let log_path_display = if config.log_enabled {
        logger.path().display().to_string()
    } else {
        "disabled".to_string()
    };
    logger.event(&format!(
        "event=start trigger={} order={} mode={} log_enabled={} log_path={}",
        config.trigger_price,
        config.order_price,
        config.mode.as_str(),
        config.log_enabled,
        log_path_display
    ));
    log_price_gap_warning(&logger, config.trigger_price, config.order_price);

    let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
    let api_secret = env::var("BINANCE_API_SECRET").unwrap_or_default();
    if api_key.is_empty() || api_secret.is_empty() {
        logger.error("event=missing_keys message=missing BINANCE_API_KEY or BINANCE_API_SECRET");
        return;
    }

    let base_url = env::var("BINANCE_BASE_URL").unwrap_or_else(|_| API_BASE_URL.to_string());
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
    };

    let mut manager = OrderManager::new(config, client, logger.clone());
    let mut backoff = Duration::from_secs(INITIAL_BACKOFF_SECS);
    let max_backoff = Duration::from_secs(MAX_BACKOFF_SECS);

    loop {
        logger.event(&format!("event=connect_attempt url={WS_URL}"));
        match connect_async(WS_URL).await {
            Ok((ws_stream, _)) => {
                logger.event("event=connected");
                backoff = Duration::from_secs(INITIAL_BACKOFF_SECS);
                if let Err(err) = stream_last_price(ws_stream, &mut manager).await {
                    logger.error(&format!("event=connection_error err={err}"));
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
}

impl OrderManager {
    fn new(config: Config, client: BinanceClient, logger: Logger) -> Self {
        Self {
            client,
            config,
            logger,
            last_position: None,
            last_position_at: None,
            last_manage_at: None,
            last_active: None,
            last_price_str: None,
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

    async fn handle_tick(&mut self, symbol: &str, price_str: &str, price: f64, event_time_ms: u64) {
        self.last_price_str = Some(price_str.to_string());
        if let Err(err) = self.refresh_position_if_needed(symbol, false).await {
            self.logger
                .error(&format!("event=position_refresh_error err={err}"));
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

        if self.should_manage(active) {
            if let Err(err) = self.manage_orders(symbol, active).await {
                self.logger
                    .error(&format!("event=order_manage_error err={err}"));
            }
            self.last_manage_at = Some(Instant::now());
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
            let snapshot = self.client.get_position(symbol).await?;
            self.last_position = Some(snapshot.clone());
            self.last_position_at = Some(Instant::now());
            self.event_with_price(&format!(
                "event=position_refresh symbol={symbol} side={} amt={}",
                snapshot.position_side, snapshot.amt_str
            ));
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

        let expected_qty_str = abs_str(&position.amt_str);
        let expected_qty = expected_qty_str.parse::<f64>().unwrap_or(0.0);
        let expected_side = if position.amt > 0.0 { "SELL" } else { "BUY" };
        let expected_position_side = position.position_side.as_str();

        if active {
            let orders = self.client.get_open_orders(symbol).await?;
            let reduce_orders: Vec<OpenOrder> = orders.into_iter().filter(|o| o.reduce_only).collect();
            let mut has_expected = false;

            for order in &reduce_orders {
                if is_expected_order(
                    order,
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

            if has_expected && reduce_orders.len() == 1 {
                self.event_with_price(&format!(
                    "event=orders_ok symbol={symbol} price={} qty={}",
                    self.config.order_price_str, expected_qty_str
                ));
                return Ok(());
            }

            if !reduce_orders.is_empty() {
                self.event_with_price(&format!(
                    "event=cancel_orders symbol={symbol} count={}",
                    reduce_orders.len()
                ));
                self.cancel_orders(symbol, &reduce_orders).await?;
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
        } else {
            let orders = self.client.get_open_orders(symbol).await?;
            let reduce_orders: Vec<OpenOrder> = orders.into_iter().filter(|o| o.reduce_only).collect();

            if !reduce_orders.is_empty() {
                self.event_with_price(&format!(
                    "event=cancel_orders symbol={symbol} count={}",
                    reduce_orders.len()
                ));
                self.cancel_orders(symbol, &reduce_orders).await?;
            } else {
                self.event_with_price(&format!(
                    "event=cancel_skip symbol={symbol} reason=none"
                ));
            }
        }

        Ok(())
    }

    async fn cancel_orders(
        &self,
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
    async fn get_position(&self, symbol: &str) -> Result<PositionSnapshot, Box<dyn Error + Send + Sync>> {
        let params = vec![("symbol".to_string(), symbol.to_string())];
        let positions: Vec<PositionRisk> =
            self.signed_request(Method::GET, POSITION_RISK_PATH, params)
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

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<OpenOrder>, Box<dyn Error + Send + Sync>> {
        let params = vec![("symbol".to_string(), symbol.to_string())];
        self.signed_request(Method::GET, OPEN_ORDERS_PATH, params)
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

        self.signed_request(Method::POST, ORDER_PATH, params)
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
        let _: Value = self
            .signed_request(Method::DELETE, ORDER_PATH, params)
            .await?;
        Ok(())
    }

    async fn signed_request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        mut params: Vec<(String, String)>,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        params.push(("timestamp".to_string(), now_millis().to_string()));
        params.push((
            "recvWindow".to_string(),
            RECV_WINDOW_MS.to_string(),
        ));
        let query = params
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let signature = self.sign(&query)?;
        let url = format!("{}{}?{}&signature={}", self.base_url, path, query, signature);

        let response = self
            .http
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;

        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(format!("binance api error {}: {}", status, body).into());
        }

        let parsed = serde_json::from_str::<T>(&body)?;
        Ok(parsed)
    }

    fn sign(&self, payload: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .map_err(|_| "invalid api secret")?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

async fn stream_last_price(
    ws_stream: WsStream,
    manager: &mut OrderManager,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (mut write, mut read) = ws_stream.split();
    let mut ping_interval = interval(Duration::from_secs(PING_INTERVAL_SECS));

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Some((symbol, price, event_time)) = parse_trade_event(&text) {
                            if let Ok(price_value) = price.parse::<f64>() {
                                manager.handle_tick(&symbol, &price, price_value, event_time).await;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        manager
                            .logger
                            .event(&format!("event=ws_close frame={frame:?}"));
                        return Ok(());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => {
                        manager.logger.event("event=ws_stream_end");
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
    side: &str,
    position_side: &str,
    qty: f64,
    price: f64,
) -> bool {
    if order.side != side {
        return false;
    }

    if order.time_in_force != "GTX" {
        return false;
    }

    if position_side != "BOTH" && order.position_side != position_side {
        return false;
    }

    let order_price = order.price.parse::<f64>().unwrap_or(0.0);
    let order_qty = order.orig_qty.parse::<f64>().unwrap_or(0.0);

    approx_eq(order_price, price) && approx_eq(order_qty, qty)
}

fn approx_eq(a: f64, b: f64) -> bool {
    let diff = (a - b).abs();
    let scale = a.abs().max(b.abs()).max(1.0);
    diff <= 1e-8 * scale
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

fn log_path_from_env() -> PathBuf {
    env::var("RB_LOG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("rb.log"))
}

fn parse_args() -> Result<Config, String> {
    let mut trigger_price: Option<String> = None;
    let mut order_price: Option<String> = None;
    let mut log_enabled = true;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--trigger" => trigger_price = args.next(),
            "--order" => order_price = args.next(),
            "--no-log" => log_enabled = false,
            "--help" | "-h" => return Err(usage()),
            _ => return Err(usage()),
        }
    }

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

    Ok(Config {
        trigger_price: trigger,
        order_price: order,
        order_price_str: order_str,
        mode,
        log_enabled,
    })
}

fn usage() -> String {
    [
        "usage:",
        "  rb --trigger <price> --order <price> [--no-log]",
        "env:",
        "  BINANCE_API_KEY=... BINANCE_API_SECRET=... [BINANCE_BASE_URL=https://papi.binance.com] [RB_LOG_PATH=rb.log]",
        "example:",
        "  BINANCE_API_KEY=... BINANCE_API_SECRET=... RB_LOG_PATH=rb.log rb --trigger 70000 --order 70500",
    ]
    .join("\n")
}
