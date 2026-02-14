BINANCE_API_KEY="$(op read "op://orpdc57dz3dvbmazstsbnvhkgm/BinanceFuturesMainnet/api_key")" \
BINANCE_API_SECRET="$(op read "op://orpdc57dz3dvbmazstsbnvhkgm/BinanceFuturesMainnet/api_secret")" \
cargo run -- --symbol BTC/USDC --market futures --trigger 95450 --order 93658.7

用法重點：
- 必填：--symbol <pair>、--market <futures|spot>、--trigger <price>、--order <price>
- 進階買入/止損：--entry <price> --stop <price> --side <long|short> 三個必須一起給
- 掛單啟動門檻：--entry-arm <price>（需搭配 entry/stop/side）
  - 未觸發前不會新掛 entry；會取消既有 entry 單
  - 未持倉時才會取消既有 stop 單；已持倉時保留並同步 stop
  - 若未觸發前已持倉，仍會維持 stop 保護單
  - 觸發條件固定依 side：
    - long：entry-arm 要高於 entry，價格必須下穿/碰到 entry-arm 才會啟動
    - short：entry-arm 要低於 entry，價格必須上穿/碰到 entry-arm 才會啟動
- 若沒有現成的買入單（預設只認 rb-entry- 前綴），就必須提供買入金額：--entry-usdc <amount> 或 RB_ENTRY_USDC
- 槓桿：--leverage <n> 或 RB_ENTRY_LEVERAGE，未提供就用 100
- 買入數量：qty = entry_usdc * leverage / entry_price
- 偵測模式：--entry-detect <prefix|any> 或 RB_ENTRY_DETECT（預設 prefix）
  - prefix：只認 clientOrderId 為 rb-entry- 的單
  - any：符合 entry 價格/方向的 LIMIT 單都當成 entry（可偵測網頁下單）
- 取消門檻：--entry-abort <price>（需搭配 entry/stop/side）
  - entry 未成交前若價格先觸碰到 entry-abort，會取消未成交 entry 與止損單並結束任務
  - 若啟動時已存在 entry 單（依偵測模式），entry-abort 本次不生效
- 關閉 log 檔寫入：--no-log（EVENT 仍會輸出到 stderr）
- TG 告警（非同步、不影響下單）：設定 TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID（可選 TELEGRAM_MESSAGE_THREAD_ID）

範例（只跑 trigger/order）：
rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500

範例（用 CLI 參數給買入金額與槓桿）：
rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500 \
  --entry 70000 --stop 69000 --side long \
  --entry-usdc 50 --leverage 20

範例（用環境變數）：
RB_ENTRY_USDC=50 RB_ENTRY_LEVERAGE=20 \
rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500 \
  --entry 70000 --stop 69000 --side long

範例（偵測網頁下單的 entry 單）：
rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500 \
  --entry 70000 --stop 69000 --side long \
  --entry-detect any

範例（到價後才開始掛 entry/stop）：
rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500 \
  --entry 70000 --stop 69000 --side long \
  --entry-arm 70100 --entry-usdc 50 --leverage 20

範例（關閉 log 檔寫入）：
rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500 --no-log

範例（啟用 TG 告警）：
TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=... \
rb --symbol BTC/USDC --market futures --trigger 70000 --order 70500
