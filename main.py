"""
蔣寶不乖
"""
import argparse
from collections import deque
import csv
from http.client import HTTPConnection, HTTPSConnection, HTTPException
import json
import re
import statistics
import sys
import threading
import time
from urllib.parse import urlencode, urlsplit

TARGET = 360_000_000
NS = 1_000_000_000
MS = 1_000_000
MAX_MARGIN = 1 * MS
MAX_COMPENSATION = 5 * MS
RATE_LIMIT_SECOND = 1.5
PROGRESS_GUARD_NS = 30 * MS
PROGRESS_INTERVAL_S = 0.02
BAR_WIDTH = 30
RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]{36,130}$")
clock = time.perf_counter_ns
TARGET_URL = "https://no-party-leaderboard.no-party-leaderboard.workers.dev"
DEFAULT_PLAYER = "LittleSurvival的Dots"


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
    line = f"\r等待中 [{bar}] {ratio * 100:5.1f}%  {clamped / NS:5.3f} / {total / NS:.3f} s{suffix}"
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
        if left > 20 * MS and stop_event is not None:
            if stop_event.wait((left - 20 * MS) / NS):
                return False
        elif left > 2 * MS:
            # time.sleep uses high-res waitable timers on Windows; Event.wait can overshoot ~15 ms.
            time.sleep((left - 2 * MS) / NS)


def wait_with_progress(start, deadline, stop_event=None):
    total_ns = max(1, deadline - start)
    # Stop updating stdout before deadline so terminal I/O won't delay finish.
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        now = clock()
        left = deadline - now
        if left <= PROGRESS_GUARD_NS:
            break
        render_progress(now - start, total_ns)
        delay = min(PROGRESS_INTERVAL_S, (left - PROGRESS_GUARD_NS) / NS)
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


def _row_total_ms(row):
    if isinstance(row.get("totalMs"), int):
        return row["totalMs"]
    time_ms = row.get("timeMs")
    penalty_ms = row.get("penaltyMs", 0)
    if isinstance(time_ms, (int, float)) and isinstance(penalty_ms, (int, float)):
        return time_ms + penalty_ms
    return None


def resolve_current_rank(conn, raw, max_pages=10):
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(data, dict):
        return None
    best_rank = data.get("rank")
    record = data.get("record")
    best = data.get("best")
    if data.get("improved") is True and isinstance(best_rank, int):
        return best_rank
    if not isinstance(record, dict) or not isinstance(data.get("page"), dict):
        return None
    current_total = _row_total_ms(record)
    if current_total is None:
        return None
    best_id = best.get("id") if isinstance(best, dict) else None
    division = data.get("division")
    cur_page = data["page"]
    rows = cur_page.get("rows")
    if not isinstance(rows, list) or not rows:
        return None
    first_rank = rows[0].get("rank")
    if not isinstance(first_rank, int) or first_rank < 1:
        return None
    ahead = first_rank - 1

    conn.auto_open = True
    for page_idx in range(max_pages + 1):
        for row in rows:
            if not isinstance(row, dict):
                continue
            if best_id is not None and row.get("id") == best_id:
                continue
            row_total = _row_total_ms(row)
            if row_total is None:
                continue
            if row_total <= current_total:
                ahead += 1
            else:
                return ahead + 1
        next_cursor = cur_page.get("next")
        if not next_cursor or page_idx == max_pages:
            return None if next_cursor else ahead + 1
        params = {"limit": "50", "after": str(next_cursor)}
        if isinstance(division, str) and division:
            params["division"] = division
        try:
            conn.request("GET", f"/api/leaderboard?{urlencode(params)}")
            resp = conn.getresponse()
            body = resp.read()
            if not 200 <= resp.status < 300:
                return None
            cur_page = json.loads(body.decode("utf-8", errors="replace"))
        except (OSError, HTTPException, ValueError):
            conn.close()
            return None
        rows = cur_page.get("rows") if isinstance(cur_page, dict) else None
        if not isinstance(rows, list) or not rows:
            return None
    return None


def _format_entry(label, time_ms, rank=None, total=None):
    if time_ms is None and rank is None:
        return None
    time_part = f"{time_ms} ms" if time_ms is not None else ""
    if rank is not None:
        total_part = f" / {total}" if total is not None else ""
        rank_part = f"第 {rank}{total_part} 名"
        detail = f"{time_part} ({rank_part})" if time_part else rank_part
    else:
        detail = time_part
    return f"{label} {detail}"


def format_finish_summary(raw, current_rank=None):
    try:
        data = json.loads(raw)
    except ValueError:
        return raw, ""
    if not isinstance(data, dict):
        return raw, ""
    if "rank" in data or isinstance(data.get("record"), dict):
        summary = {}
        for key in ("division", "rank", "total", "improved", "best", "record"):
            if key in data:
                summary[key] = data[key]
        current_ms = data["record"].get("timeMs") if isinstance(data.get("record"), dict) else None
        best_ms = data["best"].get("timeMs") if isinstance(data.get("best"), dict) else None
        best_rank = data.get("rank")
        total = data.get("total")
        if current_rank is None and data.get("improved") is True:
            current_rank = best_rank
        parts = [
            entry
            for entry in (
                _format_entry("本次", current_ms, current_rank, total),
                _format_entry("最佳", best_ms, best_rank, total),
            )
            if entry is not None
        ]
        badge = f"｜{'｜'.join(parts)}" if parts else ""
        return json.dumps(summary, ensure_ascii=False, separators=(",", ":")), badge
    return raw, ""


def extract_error(raw):
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("error"), str):
            return data["error"]
    except ValueError:
        pass
    return None


def is_too_fast(status, raw):
    if status != 422:
        return False
    err = extract_error(raw)
    if err is not None:
        return err == "too_fast"
    return "too_fast" in raw


class TimingAligner:
    """Closed-loop boundary seeker that compensates median server bias and hunts TARGET."""

    def __init__(self, initial_offset_ns=400_000):
        self.rtt_samples = deque(maxlen=20)
        self.bias_samples = deque(maxlen=10)
        self.offset_ns = min(MAX_MARGIN, max(-MAX_MARGIN, initial_offset_ns))
        self.step_ns = 200_000
        self.last_outcome = None

    def plan(self, rtt_start_ns):
        self.rtt_samples.append(rtt_start_ns)
        if self.bias_samples:
            # Direct feedback from server elapsed time: server = planned + bias.
            median_bias = int(round(statistics.median(self.bias_samples)))
            planned = max(
                TARGET - MAX_COMPENSATION,
                min(TARGET + MAX_COMPENSATION, TARGET - median_bias + self.offset_ns),
            )
            return planned, self.offset_ns, f"伺服器誤差中位數 {median_bias:+,} ns，邊界偏移 {self.offset_ns:+,} ns"

        planned = TARGET + self.offset_ns
        return planned, self.offset_ns, f"初始邊界偏移 {self.offset_ns:+,} ns"

    def record_finish_rtt(self, rtt_finish_ns):
        self.rtt_samples.append(rtt_finish_ns)

    def update(self, planned_ns, elapsed_ns, too_fast):
        if too_fast:
            if self.last_outcome == "ok":
                self.step_ns = max(50_000, self.step_ns // 2)
            else:
                self.step_ns = min(400_000, int(round(self.step_ns * 1.5)))
            self.offset_ns = min(MAX_MARGIN, self.offset_ns + self.step_ns)
            self.last_outcome = "too_fast"
            return
        if elapsed_ns is not None:
            self.bias_samples.append(elapsed_ns - planned_ns)
            over_ns = elapsed_ns - TARGET
            if over_ns > 0:
                if self.last_outcome == "too_fast":
                    self.step_ns = max(50_000, self.step_ns // 2)
                trim = max(50_000, min(150_000, over_ns // 8))
                self.offset_ns = max(-MAX_MARGIN, self.offset_ns - trim)
            self.last_outcome = "ok"


class RateLimiter:
    """Thread-safe reservation rate limiter enforcing a minimum interval (in seconds) between sends."""

    def __init__(self, interval_s=RATE_LIMIT_SECOND):
        self.interval_ns = max(0, int(round(interval_s * NS)))
        self._lock = threading.Lock()
        self._next_allowed_ns = 0

    def wait(self, stop_event=None):
        if self.interval_ns <= 0:
            return True
        with self._lock:
            now = clock()
            slot = max(now, self._next_allowed_ns)
            self._next_allowed_ns = slot + self.interval_ns
        delay_ns = slot - now
        if delay_ns <= 0:
            return not (stop_event is not None and stop_event.is_set())
        if stop_event is not None:
            return not stop_event.wait(delay_ns / NS)
        time.sleep(delay_ns / NS)
        return True


class KeepAliveClient:
    """Manages a persistent HTTP/HTTPS connection with pre-connect and auto-reconnect."""

    def __init__(self, url, timeout=10):
        target = urlsplit(url)
        if (target.scheme not in ("http", "https") or not target.hostname
                or target.path not in ("", "/") or target.query or target.fragment or target.username):
            raise ValueError("目標網址需為 http(s)://主機[:連接埠]")
        self.factory = HTTPSConnection if target.scheme == "https" else HTTPConnection
        self.host = target.hostname
        self.port = target.port
        self.timeout = timeout
        self.conn = None

    def ensure_connected(self):
        if self.conn is None:
            self.conn = self.factory(self.host, self.port, timeout=self.timeout)
        reused = self.conn.sock is not None
        if not reused:
            self.conn.connect()  # DNS, TCP and optional TLS precede the first timestamp.
        self.conn.auto_open = False
        return self.conn, reused

    def reset(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except OSError:
                pass
            self.conn = None


def parse_players(raw_args):
    text = " ".join(raw_args) if isinstance(raw_args, (list, tuple)) else str(raw_args)
    names = [item.strip() for item in next(csv.reader([text], skipinitialspace=True), [])]
    players = [name for name in names if name]
    if not players:
        raise ValueError("請至少指定一個非空白玩家名稱")
    return players


def measure(
    url,
    player,
    aligner,
    stop_event=None,
    show_progress=True,
    prefix="",
    lock=None,
    on_planned=None,
    rate_limiter=None,
    client=None,
):
    if stop_event is not None and stop_event.is_set():
        return "已取消"
    owns_client = client is None
    if owns_client:
        client = KeepAliveClient(url)
    keep_connection = False
    try:
        conn, reused = client.ensure_connected()
        if rate_limiter is not None and not rate_limiter.wait(stop_event):
            keep_connection = True
            return "已取消"
        body = prepare_post(conn, "/api/runs", {})
        try:
            start, receipt, status, raw = send_prepared(conn, body)
        except (OSError, HTTPException):
            if not reused:
                raise
            # Reused Keep-Alive socket may have been closed while idle; reconnect once.
            client.reset()
            conn, _ = client.ensure_connected()
            body = prepare_post(conn, "/api/runs", {})
            start, receipt, status, raw = send_prepared(conn, body)
        safe_print(f"{prefix}start　HTTP {status}\n{raw}", lock=lock)
        if not 200 <= status < 300:
            err = extract_error(raw)
            detail = f"：{err}" if err else ""
            raise ValueError(f"start 失敗（HTTP {status}{detail}）")
        data = json.loads(raw)
        if not isinstance(data, dict) or not isinstance(data.get("runId"), str):
            raise ValueError("start 回應缺少 runId")
        run_id = data["runId"]
        if not RUN_ID_RE.fullmatch(run_id):
            raise ValueError("start 回應 runId 格式不正確")

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
        if status in (200, 201) or too_fast:
            aligner.update(planned, elapsed_ns, too_fast)
        if elapsed_ns is not None:
            safe_print(
                f"{prefix}伺服器時間　{seconds(elapsed_ns)}（相對 {TARGET / NS:g} 秒　{elapsed_ns - TARGET:+,} ns）",
                lock=lock,
            )

        if status in (200, 201):
            current_rank = resolve_current_rank(conn, raw)
            if current_rank is not None:
                _, rank_badge = format_finish_summary(raw, current_rank=current_rank)
            keep_connection = conn.sock is not None
            return f"完成（HTTP {status}{rank_badge}）"
        keep_connection = conn.sock is not None
        if too_fast:
            return "太快了（HTTP 422 too_fast）"
        err = extract_error(raw)
        if status == 422:
            detail = f" {err}" if err else ""
            return f"伺服器拒絕（HTTP 422{detail}）"
        detail = f" {err}" if err else ""
        return f"finish 失敗（HTTP {status}{detail}）"
    finally:
        if owns_client or not keep_connection:
            client.reset()


def run_round(url, players, aligners, stop_event, rate_limiter=None, clients=None):
    if len(players) == 1:
        player = players[0]
        client = clients[0] if clients is not None else None
        try:
            result = measure(
                url,
                player,
                aligners[0],
                stop_event=stop_event,
                show_progress=True,
                rate_limiter=rate_limiter,
                client=client,
            )
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
        client = clients[idx] if clients is not None else None
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
                rate_limiter=rate_limiter,
                client=client,
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
    while any(thread.is_alive() for thread in threads):
        with state_lock:
            snapshot = list(windows)
        if snapshot:
            now = clock()
            min_start = min(s for s, _ in snapshot)
            earliest_deadline = min(d for _, d in snapshot)
            max_deadline = max(d for _, d in snapshot)
            if earliest_deadline - now > PROGRESS_GUARD_NS:
                render_progress(now - min_start, max_deadline - min_start, suffix=suffix, lock=print_lock)
        time.sleep(PROGRESS_INTERVAL_S)

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
    clients = [KeepAliveClient(url) for _ in players]
    rate_limiter = RateLimiter()
    stop_event = threading.Event()
    run_count = 0
    try:
        while True:
            run_count += 1
            safe_print(f"\n── 第 {run_count} 次量測 ──")
            run_round(url, players, aligners, stop_event, rate_limiter=rate_limiter, clients=clients)
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
    finally:
        for client in clients:
            client.reset()


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
