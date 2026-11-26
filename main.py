"""15 秒 HTTP 計時器（CLI，含信賴區間、自動對齊與多玩家並行）。Python 3.9+，僅使用標準函式庫。

py main.py
py main.py -l
py main.py -np xxx,yyy,zzz
py main.py -np '"player one","player two"' -l
"""
import argparse
from collections import deque
import csv
from http.client import HTTPConnection, HTTPSConnection, HTTPException
import json
import math
import statistics
import sys
import threading
import time
from urllib.parse import urlsplit
from uuid import UUID

TARGET = 15_000_000_000
NS = 1_000_000_000
MS = 1_000_000
MAX_MARGIN = 1 * MS
BAR_WIDTH = 30
clock = time.perf_counter_ns
TARGET_URL = "https://no-party-leaderboard.no-party-leaderboard.workers.dev"
DEFAULT_PLAYER = "LittleSurvival的Dots"

# One-sided 95% t critical values (df=1 capped to avoid huge spikes on 2 samples).
T_95 = {1: 2.50, 2: 2.92, 3: 2.35, 4: 2.13, 5: 2.02, 6: 1.94, 7: 1.89, 8: 1.86, 9: 1.83}


def supports_text(text):
    encoding = sys.stdout.encoding or "utf-8"
    try:
        text.encode(encoding)
        return True
    except UnicodeEncodeError:
        return False


BAR_FILL, BAR_EMPTY = ("█", "░") if supports_text("█░") else ("#", "-")
_last_progress_len = 0


def _write_stdout(text):
    try:
        sys.stdout.write(text)
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "ascii"
        sys.stdout.write(text.encode(encoding, "backslashreplace").decode(encoding))
    sys.stdout.flush()


def clear_progress_line():
    global _last_progress_len
    if _last_progress_len:
        _write_stdout("\r" + " " * (_last_progress_len + 12) + "\r")
        _last_progress_len = 0


def finish_progress_line(lock=None):
    global _last_progress_len
    if lock is not None:
        with lock:
            _write_stdout("\n")
            _last_progress_len = 0
    else:
        _write_stdout("\n")
        _last_progress_len = 0


def safe_print(message="", lock=None):
    if lock is not None:
        with lock:
            clear_progress_line()
            _write_stdout(message + "\n")
    else:
        clear_progress_line()
        _write_stdout(message + "\n")


def seconds(ns):
    whole, fraction = divmod(abs(ns), NS)
    return f"{'-' if ns < 0 else ''}{whole}.{fraction:09d} s"


def encode_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def prepare_post(conn, path, data):
    body = encode_json(data)
    conn.putrequest("POST", path)
    conn.putheader("Content-Type", "application/json")
    conn.putheader("Content-Length", str(len(body)))
    return body


def send_prepared(conn, body):
    sent = clock()
    conn.endheaders(body)
    response = conn.getresponse()
    raw = response.read()
    received = clock()
    return sent, received, response.status, raw.decode("utf-8", errors="replace")


def render_progress(elapsed_ns, total_ns, suffix="", lock=None):
    global _last_progress_len
    total = max(1, total_ns)
    clamped = max(0, min(total, elapsed_ns))
    ratio = clamped / total
    filled = int(BAR_WIDTH * ratio)
    bar = BAR_FILL * filled + BAR_EMPTY * (BAR_WIDTH - filled)
    line = f"\r等待中 [{bar}] {ratio * 100:5.1f}%  {clamped / NS:5.2f} / {total / NS:.2f} s{suffix}"
    if lock is not None:
        with lock:
            _last_progress_len = max(_last_progress_len, len(line) - 1)
            _write_stdout(line)
    else:
        _last_progress_len = max(_last_progress_len, len(line) - 1)
        _write_stdout(line)


def wait_until(deadline, stop_event=None):
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        left = deadline - clock()
        if left <= 0:
            return True
        if left > MS:
            delay = (left - 500_000) / NS
            if stop_event is not None:
                if stop_event.wait(delay):
                    return False
            else:
                time.sleep(delay)


def wait_with_progress(start, deadline, stop_event=None):
    total_ns = max(1, deadline - start)
    # Stop updating stdout 50 ms before deadline so terminal I/O won't delay finish.
    guard = 50 * MS
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        now = clock()
        left = deadline - now
        if left <= guard:
            break
        render_progress(now - start, total_ns)
        delay = min(0.05, (left - guard) / NS)
        if stop_event is not None:
            if stop_event.wait(delay):
                return False
        else:
            time.sleep(delay)
    return wait_until(deadline, stop_event)


def extract_elapsed_ns(raw):
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    candidates = [data]
    for nested in ("record", "run"):
        if isinstance(data.get(nested), dict):
            candidates.append(data[nested])
    for obj in candidates:
        val = obj.get("elapsedNs")
        if type(val) is int and val >= 0:
            return val
        for key in ("timeMs", "elapsedMs"):
            val = obj.get(key)
            if isinstance(val, (int, float)) and not isinstance(val, bool) and val >= 0:
                return int(round(val * MS))
    return None


def format_finish_summary(raw):
    try:
        data = json.loads(raw)
    except ValueError:
        return raw, ""
    if not isinstance(data, dict):
        return raw, ""
    if "rank" in data or isinstance(data.get("record"), dict):
        summary = {}
        if "rank" in data:
            summary["rank"] = data["rank"]
        if "total" in data:
            summary["total"] = data["total"]
        if isinstance(data.get("record"), dict):
            summary["record"] = data["record"]
        badge = ""
        if "rank" in data:
            total = f" / {data['total']}" if "total" in data else ""
            time_ms = data["record"].get("timeMs") if isinstance(data.get("record"), dict) else None
            time_part = f"｜{time_ms} ms" if time_ms is not None else ""
            badge = f"｜第 {data['rank']}{total} 名{time_part}"
        return json.dumps(summary, ensure_ascii=False, separators=(",", ":")), badge
    return raw, ""


def is_too_fast(status, raw):
    if status != 422:
        return False
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and "error" in data:
            return data["error"] == "too_fast"
    except ValueError:
        pass
    return "too_fast" in raw


class TimingAligner:
    """Estimates wait offset within ±1 ms using a 95% one-sided CI and closed-loop alignment."""

    def __init__(self, initial_offset_ns=500_000):
        self.rtt_samples = deque(maxlen=20)
        self.bias_samples = deque(maxlen=10)
        self.offset_ns = min(MAX_MARGIN, max(0, initial_offset_ns))
        self.step_ns = 200_000
        self.last_outcome = None

    def _ci_margin(self, samples):
        n = len(samples)
        if n < 2:
            return 0
        sigma = statistics.stdev(samples)
        t_val = T_95.get(n - 1, 1.645)
        return int(round(t_val * sigma))

    def plan(self, rtt_start_ns):
        self.rtt_samples.append(rtt_start_ns)
        if len(self.bias_samples) >= 2:
            # Direct feedback from server elapsed time: server = planned + bias.
            median_bias = int(round(statistics.median(self.bias_samples)))
            margin = min(MAX_MARGIN, self._ci_margin(self.bias_samples))
            planned = max(TARGET - MAX_MARGIN, min(TARGET + MAX_MARGIN, TARGET - median_bias + margin))
            return planned, margin, f"伺服器誤差中位數 {median_bias:+,} ns，95% CI 緩衝 +{margin:,} ns"

        # Fallback before >=2 server timings: RTT jitter CI hint + adaptive offset (capped at 1 ms).
        rtt_sigma = statistics.stdev(self.rtt_samples) / math.sqrt(2) if len(self.rtt_samples) >= 2 else 0.0
        rtt_ci = min(MAX_MARGIN, int(round(1.645 * rtt_sigma))) if rtt_sigma > 0 else 0
        margin = min(MAX_MARGIN, max(self.offset_ns, rtt_ci if self.last_outcome == "too_fast" else self.offset_ns))
        planned = TARGET + margin
        return planned, margin, f"自適應偏移 +{margin:,} ns（RTT 抖動 CI 參考 +{rtt_ci:,} ns）"

    def record_finish_rtt(self, rtt_finish_ns):
        self.rtt_samples.append(rtt_finish_ns)

    def update(self, planned_ns, elapsed_ns, too_fast):
        if elapsed_ns is not None:
            self.bias_samples.append(elapsed_ns - planned_ns)
            self.last_outcome = "too_fast" if too_fast else "ok"
            return
        if too_fast:
            if self.last_outcome == "ok":
                self.step_ns = max(50_000, self.step_ns // 2)
            else:
                self.step_ns = min(400_000, int(round(self.step_ns * 1.5)))
            self.offset_ns = min(MAX_MARGIN, self.offset_ns + self.step_ns)
            self.last_outcome = "too_fast"
        else:
            if self.last_outcome == "too_fast":
                self.step_ns = max(50_000, self.step_ns // 2)
            # Gently trim downward after passing so offset stays near the 15s boundary.
            self.offset_ns = max(0, self.offset_ns - max(50_000, self.step_ns // 2))
            self.last_outcome = "ok"


def parse_players(raw_args):
    text = " ".join(raw_args) if isinstance(raw_args, (list, tuple)) else str(raw_args)
    names = [item.strip() for item in next(csv.reader([text], skipinitialspace=True), [])]
    players = [name for name in names if name]
    if not players:
        raise ValueError("請至少指定一個非空白玩家名稱")
    return players


def measure(url, player, aligner, stop_event=None, show_progress=True, prefix="", lock=None, on_planned=None):
    target = urlsplit(url)
    if (target.scheme not in ("http", "https") or not target.hostname
            or target.path not in ("", "/") or target.query or target.fragment or target.username):
        raise ValueError("目標網址需為 http(s)://主機[:連接埠]")
    factory = HTTPSConnection if target.scheme == "https" else HTTPConnection
    conn = factory(target.hostname, target.port, timeout=25)
    try:
        conn.connect()  # DNS, TCP and optional TLS precede the first timestamp.
        conn.auto_open = False
        body = prepare_post(conn, "/api/runs", {})
        start, receipt, status, raw = send_prepared(conn, body)
        safe_print(f"{prefix}start　HTTP {status}\n{raw}", lock=lock)
        if not 200 <= status < 300:
            raise ValueError(f"start 失敗（HTTP {status}）")
        data = json.loads(raw)
        if not isinstance(data, dict) or not isinstance(data.get("runId"), str):
            raise ValueError("start 回應缺少 runId")
        run_id = str(UUID(data["runId"]))

        rtt_start = receipt - start
        planned, _, plan_desc = aligner.plan(rtt_start)
        deadline = start + planned
        if on_planned is not None:
            on_planned(start, deadline)
        payload = {"player": player, "penaltyMs": 0, "errors": 0}
        body = prepare_post(conn, f"/api/runs/{run_id}/finish", payload)
        safe_print(
            f"{prefix}start 往返　{seconds(rtt_start)}\n"
            f"{prefix}目標間隔　{seconds(planned)}（{plan_desc}）\n"
            f"{prefix}回應抵達時剩餘　{seconds(max(0, deadline - receipt))}",
            lock=lock,
        )

        ok = wait_with_progress(start, deadline, stop_event) if show_progress else wait_until(deadline, stop_event)
        if not ok:
            return "已取消"

        finish, finish_receipt, status, raw = send_prepared(conn, body)
        aligner.record_finish_rtt(finish_receipt - finish)
        if show_progress:
            render_progress(planned, planned, lock=lock)
            finish_progress_line(lock=lock)

        finish_summary, rank_badge = format_finish_summary(raw)
        safe_print(
            f"{prefix}finish　HTTP {status}\n{finish_summary}\n"
            f"{prefix}finish 往返　{seconds(finish_receipt - finish)}\n"
            f"{prefix}本機送出間隔　{seconds(finish - start)}",
            lock=lock,
        )

        elapsed_ns = extract_elapsed_ns(raw)
        too_fast = is_too_fast(status, raw)
        if status in (200, 201) or too_fast or status == 422:
            aligner.update(planned, elapsed_ns, too_fast)
        if elapsed_ns is not None:
            safe_print(
                f"{prefix}伺服器時間　{seconds(elapsed_ns)}（相對 15 秒　{elapsed_ns - TARGET:+,} ns）",
                lock=lock,
            )

        if status in (200, 201):
            return f"完成（HTTP {status}{rank_badge}）"
        if too_fast:
            return "太快了（HTTP 422 too_fast）"
        if status == 422:
            return "時間已過關，但伺服器回傳 422（請確認 finish payload）"
        return f"finish 失敗（HTTP {status}）"
    finally:
        conn.close()


def run_round(url, players, aligners, stop_event):
    if len(players) == 1:
        player = players[0]
        try:
            result = measure(url, player, aligners[0], stop_event=stop_event, show_progress=True)
        except (OSError, HTTPException, ValueError, KeyError, TypeError) as exc:
            result = f"量測失敗　{exc}"
        safe_print(f"[{player}] {result}")
        return

    print_lock = threading.Lock()
    state_lock = threading.Lock()
    windows = []
    results = {}

    def on_planned(start, deadline):
        with state_lock:
            windows.append((start, deadline))

    def worker(idx, player):
        try:
            res = measure(
                url,
                player,
                aligners[idx],
                stop_event=stop_event,
                show_progress=False,
                prefix=f"[{player}] ",
                lock=print_lock,
                on_planned=on_planned,
            )
        except (OSError, HTTPException, ValueError, KeyError, TypeError) as exc:
            res = f"量測失敗　{exc}"
        results[idx] = (player, res)

    threads = [
        threading.Thread(target=worker, args=(idx, player), daemon=True)
        for idx, player in enumerate(players)
    ]
    for thread in threads:
        thread.start()

    suffix = f"（{len(players)} 並行）"
    guard = 50 * MS
    while any(thread.is_alive() for thread in threads):
        with state_lock:
            snapshot = list(windows)
        if snapshot:
            now = clock()
            min_start = min(s for s, _ in snapshot)
            earliest_deadline = min(d for _, d in snapshot)
            max_deadline = max(d for _, d in snapshot)
            if earliest_deadline - now > guard:
                render_progress(now - min_start, max_deadline - min_start, suffix=suffix, lock=print_lock)
        time.sleep(0.05)

    for thread in threads:
        thread.join()
    with state_lock:
        snapshot = list(windows)
    if snapshot and not stop_event.is_set():
        total = max(d for _, d in snapshot) - min(s for s, _ in snapshot)
        render_progress(total, total, suffix=suffix, lock=print_lock)
        finish_progress_line(lock=print_lock)
    for idx in range(len(players)):
        player, res = results.get(idx, (players[idx], "已取消"))
        safe_print(f"[{player}] {res}", lock=print_lock)


def ask_next_action():
    while True:
        choice = input("繼續下一次？[y: 繼續 / n: 結束 / l: 自動循環 (auto loop)]: ").strip().lower()
        if choice in ("y", "n", "l"):
            return choice
        safe_print("請輸入 y、n 或 l。")


def run_cli(url, players, input_fn=ask_next_action, auto_loop=False):
    safe_print(f"目標網址　{url}")
    safe_print(f"玩家名單（{len(players)} 並行）　{', '.join(players)}")
    if auto_loop:
        safe_print("自動循環模式已啟用（按 Ctrl+C 停止）。")
    aligners = [TimingAligner() for _ in players]
    stop_event = threading.Event()
    run_count = 0
    try:
        while True:
            run_count += 1
            safe_print(f"\n── 第 {run_count} 次量測 ──")
            run_round(url, players, aligners, stop_event)
            if auto_loop:
                continue
            action = input_fn()
            if action == "n":
                safe_print("已結束。")
                break
            if action == "l":
                auto_loop = True
                safe_print("已進入自動循環模式（按 Ctrl+C 停止）。")
    except (KeyboardInterrupt, EOFError):
        stop_event.set()
        clear_progress_line()
        safe_print("\n已停止。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-np",
        nargs="+",
        metavar="NAMES",
        help='並行玩家名稱，以逗號分隔（可用 "..." 括號含空格名稱），例如 xxx,yyy,zzz',
    )
    parser.add_argument(
        "-l",
        "--loop",
        action="store_true",
        help="啟動後直接進入自動循環（auto loop）模式",
    )
    args = parser.parse_args()
    try:
        player_list = parse_players(args.np) if args.np is not None else [DEFAULT_PLAYER]
    except ValueError as exc:
        parser.error(str(exc))
    run_cli(TARGET_URL, player_list, auto_loop=args.loop)
