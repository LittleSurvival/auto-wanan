# auto-wanan

小遊戲 : [借用凱達格蘭大道大作戰](https://sansword.github.io/no_party_for_wanna/)

## 啟動指令

```bash
py main.py
```

## `-np` 參數使用方式

使用 `-np` 指定並行玩家名稱（以逗號 `,` 分隔），會同時開啟對應數量的並行執行緒分別送出請求；若名稱內含空白，可用雙引號 `"..."` 括起來：

```bash
py main.py -np xxx,yyy,zzz
py main.py -np '"player one","player two",player3'
```

## 自動校準系統概述

- **連線與計時基準**：為每位玩家維持跨輪復用的 Keep-Alive TLS/HTTP 連線（首次連線與斷線重連皆在送出 `POST /api/runs` 前完成握手），並透過跨執行緒的 Rate Limiter 限制每次送出間隔至少 `RATE_LIMIT_SECOND` 秒（避免觸發伺服器 rate limit），於送出瞬間起算時間；倒數前 30 ms 停止刷新終端進度條以避免 I/O 延遲，最後區段以高精準等待對齊目標送出時刻。
- **閉迴路動態校準**：以 `+0.4 ms` 初始邊界偏移啟動，取得伺服器回傳 `record.timeMs` 後即計算「伺服器實際耗時 − 計畫間隔」的中位數誤差作為基準補償（上限 `±5 ms`），並搭配邊界逼近控制（高於 `0.36 s` 時微幅下修、命中 `360 ms` 時維持、觸發 `too_fast` 時自適應上調並縮半步距），避免過去單尾 95% CI 緩衝過於保守而長期停在 `361 ms`。
