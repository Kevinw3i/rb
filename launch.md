BINANCE_API_KEY="$(op read "op://orpdc57dz3dvbmazstsbnvhkgm/BinanceFuturesMainnet/api_key")" \
BINANCE_API_SECRET="$(op read "op://orpdc57dz3dvbmazstsbnvhkgm/BinanceFuturesMainnet/api_secret")" \
cargo run -- --trigger 95450 --order 93658.7
