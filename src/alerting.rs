use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

const DROP_LOG_INTERVAL_SECS: u64 = 30;

#[derive(Clone, Debug, Default)]
pub struct AlertContext {
    pub market: Option<String>,
    pub symbol: Option<String>,
}

impl AlertContext {
    pub fn for_symbol_market(market: &str, symbol: &str) -> Self {
        Self {
            market: Some(market.to_string()),
            symbol: Some(symbol.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum AlertKind {
    Error,
    Fill,
    Exit,
}

impl AlertKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertKind::Error => "ERROR",
            AlertKind::Fill => "FILL",
            AlertKind::Exit => "EXIT",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Alert {
    pub kind: AlertKind,
    pub title: String,
    pub fields: Vec<(String, String)>,
    pub body: Option<String>,
    pub ts: DateTime<Utc>,
    pub context: AlertContext,
    pub current_price: Option<String>,
}

impl Alert {
    pub fn new(kind: AlertKind, title: impl Into<String>, context: AlertContext) -> Self {
        Self {
            kind,
            title: title.into(),
            fields: Vec::new(),
            body: None,
            ts: Utc::now(),
            context,
            current_price: None,
        }
    }

    pub fn error(message: impl Into<String>, context: AlertContext) -> Self {
        Self::new(AlertKind::Error, "ERROR", context).with_body(message)
    }

    pub fn fill(title: impl Into<String>, context: AlertContext) -> Self {
        Self::new(AlertKind::Fill, title, context)
    }

    pub fn exit(title: impl Into<String>, context: AlertContext) -> Self {
        Self::new(AlertKind::Exit, title, context)
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }

    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    pub fn with_current_price(mut self, current_price: Option<String>) -> Self {
        self.current_price = current_price;
        self
    }
}

#[derive(Clone)]
pub struct AlertSender {
    tx: mpsc::Sender<Alert>,
    drop_state: Arc<Mutex<DropState>>,
}

struct DropState {
    dropped_since_last_log: u64,
    last_logged_at: Instant,
}

impl AlertSender {
    pub fn new(tx: mpsc::Sender<Alert>) -> Self {
        Self {
            tx,
            drop_state: Arc::new(Mutex::new(DropState {
                dropped_since_last_log: 0,
                last_logged_at: Instant::now(),
            })),
        }
    }

    pub fn try_send(&self, alert: Alert) {
        match self.tx.try_send(alert) {
            Ok(()) => {}
            Err(err) => match err {
                mpsc::error::TrySendError::Full(_) => self.record_drop("queue_full"),
                mpsc::error::TrySendError::Closed(_) => self.record_drop("queue_closed"),
            },
        }
    }

    fn record_drop(&self, reason: &'static str) {
        let mut guard = match self.drop_state.lock() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };
        guard.dropped_since_last_log += 1;
        if guard.last_logged_at.elapsed() < Duration::from_secs(DROP_LOG_INTERVAL_SECS) {
            return;
        }

        let dropped = guard.dropped_since_last_log;
        guard.dropped_since_last_log = 0;
        guard.last_logged_at = Instant::now();

        eprintln!(
            "[{}] ERROR event=tg_alert_drop dropped_since_last_log={dropped} reason={reason}",
            Utc::now().to_rfc3339()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn alert_sender_drops_when_queue_full() {
        let (tx, mut rx) = mpsc::channel::<Alert>(1);
        let sender = AlertSender::new(tx);

        sender.try_send(Alert::error("a", AlertContext::default()));
        sender.try_send(Alert::error("b", AlertContext::default()));

        let first = rx.recv().await.expect("first alert should arrive");
        assert_eq!(first.body.as_deref(), Some("a"));

        assert!(rx.try_recv().is_err(), "second alert should be dropped");
    }
}
