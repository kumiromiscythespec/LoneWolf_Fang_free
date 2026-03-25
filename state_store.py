# BUILD_ID: 2026-03-15_replay_state_fast_path_v1
# BUILD_ID: 2026-03-05_context_state_store_refactor_v1
# state_store.py
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Optional


# =========================
# Data classes
# =========================
@dataclass
class StopState:
    emergency_stop: int
    stop_mode: str
    stop_reason: str


# =========================
# StateStore
# =========================
class StateStore:
    """
    SQLite-backed state store.

    Tables:
      - positions(symbol PK, entry, qty, stop, take_profit, candle_ts_open)
      - equity(date PK, equity_jpy, peak, dd)
      - trade_plan(id PK, symbol, regime, action, entry, stop, take_profit, qty, reason, candle_ts)
      - stop_state(id=1 singleton)
      - weekly_base(id=1 singleton)
      - bot_kv(key PK, value)  # timeframe-specific guards etc.
    """

    def __init__(self, db_path: str, context_id: str = ""):
        self.db_path = str(db_path)
        self.context_id = str(context_id or "")
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._replay_fast_path = False
        self._kv_cache: dict[str, Optional[str]] = {}
        self._positions_cache: dict[str, Optional[dict]] = {}
        self._positions_cache_ready = False
        self._daily_metrics_cache: dict[str, dict] = {}
        self._init_db()
        if self.context_id:
            self.set_context_metadata(
                {
                    "context_id": str(self.context_id),
                }
            )

    # -------------------------
    # DB init / schema
    # -------------------------
    def _init_db(self) -> None:
        conn = self.conn
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                entry REAL,
                qty REAL,
                stop REAL,
                take_profit REAL,
                candle_ts_open INTEGER,
                stop_kind TEXT
            )
            """
        )

        # Backward-compatible migration (older DBs won't have stop_kind).
        cols = [r[1] for r in cur.execute("PRAGMA table_info(positions)").fetchall()]
        if "stop_kind" not in cols:
            cur.execute("ALTER TABLE positions ADD COLUMN stop_kind TEXT")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS equity (
                date TEXT PRIMARY KEY,
                equity_jpy REAL,
                peak REAL,
                dd REAL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_plan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                regime TEXT,
                action TEXT,
                entry REAL,
                stop REAL,
                take_profit REAL,
                qty REAL,
                reason TEXT,
                candle_ts INTEGER
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stop_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                emergency_stop INTEGER,
                stop_mode TEXT,
                stop_reason TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_base (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                week_start_date TEXT,
                base_equity_jpy REAL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_kv (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_metrics (
                day TEXT PRIMARY KEY,
                payload TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS context_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                initial_equity REAL,
                current_equity REAL,
                peak_equity REAL,
                equity_currency TEXT,
                realized_pnl REAL,
                unrealized_pnl REAL,
                weekly_base REAL,
                daily_base REAL,
                dd_stop_flags TEXT,
                week_key TEXT,
                day_key TEXT,
                last_updated_ts INTEGER
            )
            """
        )

        # Seed singleton rows
        cur.execute(
            "INSERT OR IGNORE INTO stop_state(id, emergency_stop, stop_mode, stop_reason) VALUES (1, 0, '', '')"
        )
        cur.execute(
            "INSERT OR IGNORE INTO weekly_base(id, week_start_date, base_equity_jpy) VALUES (1, '', 0)"
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO equity_state(
                id,
                initial_equity,
                current_equity,
                peak_equity,
                equity_currency,
                realized_pnl,
                unrealized_pnl,
                weekly_base,
                daily_base,
                dd_stop_flags,
                week_key,
                day_key,
                last_updated_ts
            )
            VALUES (1, 0, 0, 0, '', 0, 0, 0, 0, '{}', '', '', 0)
            """
        )

        conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def enable_replay_fast_path(self) -> None:
        self._replay_fast_path = True
        self._kv_cache.clear()
        self._positions_cache.clear()
        self._positions_cache_ready = False
        self._daily_metrics_cache.clear()
        try:
            self.conn.execute("PRAGMA journal_mode=MEMORY")
        except Exception:
            pass
        try:
            self.conn.execute("PRAGMA synchronous=OFF")
        except Exception:
            pass
        try:
            self.conn.execute("PRAGMA temp_store=MEMORY")
        except Exception:
            pass
        try:
            self.conn.execute("PRAGMA cache_size=-65536")
        except Exception:
            pass

    @staticmethod
    def _normalize_kv_value(value: object) -> Optional[str]:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _normalize_position_row(row: object) -> Optional[dict]:
        if row is None:
            return None
        try:
            data = dict(row)
        except Exception:
            return None
        if not data.get("stop_kind"):
            data["stop_kind"] = "init"
        return data

    # -------------------------
    # Context metadata
    # -------------------------
    def get_context_metadata(self) -> dict:
        rows = self.conn.execute("SELECT key, value FROM context_meta").fetchall()
        out: dict[str, str] = {}
        for row in rows:
            key = str(row["key"] or "")
            if not key:
                continue
            out[key] = str(row["value"] or "")
        return out

    def set_context_metadata(self, meta: dict) -> None:
        if not isinstance(meta, dict):
            return
        rows: list[tuple[str, str]] = []
        for k, v in meta.items():
            key = str(k or "").strip()
            if not key:
                continue
            rows.append((key, str(v if v is not None else "")))
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT INTO context_meta(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            rows,
        )
        self.conn.commit()

    # -------------------------
    # Equity state (context scoped)
    # -------------------------
    def get_equity_state(self) -> dict:
        row = self.conn.execute("SELECT * FROM equity_state WHERE id=1").fetchone()
        if row is None:
            return {
                "initial_equity": 0.0,
                "current_equity": 0.0,
                "peak_equity": 0.0,
                "equity_currency": "",
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "weekly_base": 0.0,
                "daily_base": 0.0,
                "dd_stop_flags": {},
                "week_key": "",
                "day_key": "",
                "last_updated_ts": 0,
            }
        out = dict(row)
        raw_flags = out.get("dd_stop_flags")
        try:
            parsed = json.loads(str(raw_flags or "{}"))
            out["dd_stop_flags"] = parsed if isinstance(parsed, dict) else {}
        except Exception:
            out["dd_stop_flags"] = {}
        return out

    def upsert_equity_state(self, **fields: object) -> dict:
        current = self.get_equity_state()
        merged = dict(current)
        merged.update(fields or {})
        dd_flags = merged.get("dd_stop_flags")
        if not isinstance(dd_flags, dict):
            dd_flags = {}
        merged["dd_stop_flags"] = dd_flags
        self.conn.execute(
            """
            UPDATE equity_state
            SET
                initial_equity=?,
                current_equity=?,
                peak_equity=?,
                equity_currency=?,
                realized_pnl=?,
                unrealized_pnl=?,
                weekly_base=?,
                daily_base=?,
                dd_stop_flags=?,
                week_key=?,
                day_key=?,
                last_updated_ts=?
            WHERE id=1
            """,
            (
                float(merged.get("initial_equity") or 0.0),
                float(merged.get("current_equity") or 0.0),
                float(merged.get("peak_equity") or 0.0),
                str(merged.get("equity_currency") or ""),
                float(merged.get("realized_pnl") or 0.0),
                float(merged.get("unrealized_pnl") or 0.0),
                float(merged.get("weekly_base") or 0.0),
                float(merged.get("daily_base") or 0.0),
                json.dumps(dd_flags, ensure_ascii=False),
                str(merged.get("week_key") or ""),
                str(merged.get("day_key") or ""),
                int(merged.get("last_updated_ts") or 0),
            ),
        )
        self.conn.commit()
        return self.get_equity_state()

    # -------------------------
    # KV (generic)
    # -------------------------
    def get_kv(self, key: str, default: Optional[str] = None) -> Optional[str]:
        if self._replay_fast_path and key in self._kv_cache:
            cached = self._kv_cache.get(key)
            return default if cached is None else cached
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM bot_kv WHERE key = ?", (key,))
        row = cur.fetchone()
        if row is None:
            if self._replay_fast_path:
                self._kv_cache[key] = None
            return default
        value = self._normalize_kv_value(row["value"])
        if self._replay_fast_path:
            self._kv_cache[key] = value
        return value

    def set_kv(self, key: str, value: str) -> None:
        normalized = self._normalize_kv_value(value)
        if self._replay_fast_path:
            current = self._kv_cache.get(key) if key in self._kv_cache else None
            if (key in self._kv_cache) and current == normalized:
                return
            self._kv_cache[key] = normalized
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO bot_kv(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def del_kv(self, key: str) -> None:
        """Delete a key from bot_kv.

        runner.py calls this when clearing per-symbol transient state.
        """
        if self._replay_fast_path:
            self._kv_cache[key] = None
        cur = self.conn.cursor()
        cur.execute("DELETE FROM bot_kv WHERE key = ?", (key,))
        self.conn.commit()

    def get_last_candle_ts(self, timeframe: str) -> int:
        v = self.get_kv(f"last_candle_ts:{timeframe}", default="0")
        try:
            return int(v) if v is not None else 0
        except Exception:
            return 0

    def set_last_candle_ts(self, timeframe: str, ts: int) -> None:
        self.set_kv(f"last_candle_ts:{timeframe}", str(int(ts)))

    # Backward compatible wrappers (old code may call these)
    def get_last_15m_candle_ts(self) -> int:
        return self.get_last_candle_ts("15m")

    def set_last_15m_candle_ts(self, ts: int) -> None:
        self.set_last_candle_ts("15m", ts)

    # -------------------------
    # Stop state
    # -------------------------
    def get_stop_state(self) -> StopState:
        row = self.conn.execute(
            "SELECT emergency_stop, stop_mode, stop_reason FROM stop_state WHERE id=1"
        ).fetchone()
        if row is None:
            # Should not happen due to seed, but safe fallback
            self.conn.execute(
                "INSERT OR IGNORE INTO stop_state(id, emergency_stop, stop_mode, stop_reason) VALUES (1, 0, '', '')"
            )
            self.conn.commit()
            return StopState(0, "", "")
        return StopState(
            emergency_stop=int(row["emergency_stop"]),
            stop_mode=str(row["stop_mode"] or ""),
            stop_reason=str(row["stop_reason"] or ""),
        )

    # -------------------------
    # Stop state (compat helpers for safety.py)
    # -------------------------
    def set_stop(self, mode: str, reason: str, phase: str = "") -> None:
        # phase は現状DBに持ってないのでログ用途。必要なら schema 拡張で追加。
        self.set_stop_state(1, stop_mode=str(mode), stop_reason=str(reason))

    def clear_stop(self) -> None:
        self.set_stop_state(0, stop_mode="", stop_reason="")


    def set_stop_state(self, emergency_stop: int, stop_mode: str = "", stop_reason: str = "") -> None:
        self.conn.execute(
            "UPDATE stop_state SET emergency_stop=?, stop_mode=?, stop_reason=? WHERE id=1",
            (int(emergency_stop), str(stop_mode), str(stop_reason)),
        )
        self.conn.commit()

    # -------------------------
    # Weekly base
    # -------------------------
    def get_weekly_base(self):
        row = self.conn.execute("SELECT week_start_date, base_equity_jpy FROM weekly_base WHERE id=1").fetchone()
        if row is None:
            self.conn.execute(
                "INSERT OR IGNORE INTO weekly_base(id, week_start_date, base_equity_jpy) VALUES (1, '', 0)"
            )
            self.conn.commit()
            return {"week_start_date": "", "base_equity_jpy": 0.0}
        return row

    def set_weekly_base(self, week_start_date: str, base_equity: float) -> None:
        self.conn.execute(
            "UPDATE weekly_base SET week_start_date=?, base_equity_jpy=? WHERE id=1",
            (week_start_date, float(base_equity)),
        )
        self.conn.commit()

    # -------------------------
    # Equity
    # -------------------------
    def get_equity_row(self):
        """
        Returns latest equity row (by date desc).
        If none exists, returns a default-like row with equity_jpy=0.
        runner.py uses equity_row["equity_jpy"].
        """
        row = self.conn.execute("SELECT * FROM equity ORDER BY date DESC LIMIT 1").fetchone()
        if row is None:
            # return a dict-like object
            return {"date": "", "equity_jpy": 0.0, "peak": 0.0, "dd": 0.0}
        return row

    def upsert_equity(self, equity: float, date: str):
        """
        Upsert equity at 'date'. Track peak and drawdown.
        Returns the row after upsert (dict-like).
        """
        equity = float(equity)

        # Get previous peak (latest row)
        prev = self.conn.execute("SELECT peak FROM equity ORDER BY date DESC LIMIT 1").fetchone()
        prev_peak = float(prev["peak"]) if prev and prev["peak"] is not None else 0.0

        peak = max(prev_peak, equity)
        dd = 0.0
        if peak > 0:
            dd = (peak - equity) / peak

        self.conn.execute(
            """
            INSERT INTO equity(date, equity_jpy, peak, dd)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
              equity_jpy=excluded.equity_jpy,
              peak=excluded.peak,
              dd=excluded.dd
            """,
            (date, equity, peak, dd),
        )
        self.conn.commit()

        row = self.conn.execute("SELECT * FROM equity WHERE date=?", (date,)).fetchone()
        return row if row is not None else {"date": date, "equity_jpy": equity, "peak": peak, "dd": dd}

    # -------------------------
    # Positions
    # -------------------------
    def list_positions(self) -> list[dict]:
        """
        全ポジションを返す（runnerの同時建玉制限/強制クローズ用）
        return: [{"symbol":..., "entry":..., "qty":..., "stop":..., "take_profit":..., "candle_ts_open":...}, ...]
        """
        if self._replay_fast_path and self._positions_cache_ready:
            return [dict(pos) for pos in self._positions_cache.values() if isinstance(pos, dict)]
        rows = self.conn.execute("SELECT * FROM positions").fetchall()
        out = [dict(r) for r in rows]
        if self._replay_fast_path:
            self._positions_cache = {
                str(pos.get("symbol") or ""): self._normalize_position_row(pos)
                for pos in out
                if str(pos.get("symbol") or "")
            }
            self._positions_cache_ready = True
        return out

    def get_position(self, symbol: str):
        if self._replay_fast_path and symbol in self._positions_cache:
            pos_cached = self._positions_cache.get(symbol)
            return dict(pos_cached) if isinstance(pos_cached, dict) else None
        row = self.conn.execute("SELECT * FROM positions WHERE symbol=?", (symbol,)).fetchone()
        pos = self._normalize_position_row(row)
        if self._replay_fast_path:
            self._positions_cache[symbol] = (dict(pos) if isinstance(pos, dict) else None)
        return dict(pos) if isinstance(pos, dict) else None

    def open_position(
        self,
        symbol: str,
        entry: float,
        qty: float,
        stop: float,
        take_profit: float,
        candle_ts_open: int,
        stop_kind: str = "init",
    ) -> None:
        if self._replay_fast_path:
            self._positions_cache[symbol] = {
                "symbol": symbol,
                "entry": float(entry),
                "qty": float(qty),
                "stop": float(stop),
                "take_profit": float(take_profit),
                "candle_ts_open": int(candle_ts_open),
                "stop_kind": str(stop_kind),
            }
            self._positions_cache_ready = True
        self.conn.execute(
            """
            INSERT INTO positions(symbol, entry, qty, stop, take_profit, candle_ts_open, stop_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
              entry=excluded.entry,
              qty=excluded.qty,
              stop=excluded.stop,
              take_profit=excluded.take_profit,
              candle_ts_open=excluded.candle_ts_open,
              stop_kind=excluded.stop_kind
            """,
            (symbol, float(entry), float(qty), float(stop), float(take_profit), int(candle_ts_open), str(stop_kind)),
        )
        self.conn.commit()
        
    def update_position(
        self,
        symbol: str,
        stop: float | None = None,
        take_profit: float | None = None,
        qty: float | None = None,
        stop_kind: str | None = None,
    ) -> None:
        """
        Update existing position fields (partial update).
        """
        if self._replay_fast_path:
            pos = self.get_position(symbol)
            if pos is None:
                return
            if stop is not None:
                pos["stop"] = float(stop)
            if take_profit is not None:
                pos["take_profit"] = float(take_profit)
            if qty is not None:
                pos["qty"] = float(qty)
            if stop_kind is not None:
                pos["stop_kind"] = str(stop_kind)
            self._positions_cache[symbol] = dict(pos)
            self._positions_cache_ready = True
        fields = []
        params = []

        if stop is not None:
            fields.append("stop=?")
            params.append(float(stop))
        if take_profit is not None:
            fields.append("take_profit=?")
            params.append(float(take_profit))
        if qty is not None:
            fields.append("qty=?")
            params.append(float(qty))
        if stop_kind is not None:
            fields.append("stop_kind=?")
            params.append(str(stop_kind))

        if not fields:
            return

        params.append(symbol)
        sql = f"UPDATE positions SET {', '.join(fields)} WHERE symbol=?"
        self.conn.execute(sql, tuple(params))
        self.conn.commit()

    def close_position(
        self,
        symbol: str,
        exit_price: float,
        candle_ts_exit: int,
        reason: str,
        fee_rate: float = 0.0,              # legacy (both-sides)
        fee_rate_entry: float | None = None,
        fee_rate_exit: float | None = None,
    ):
        """
        Closes an existing long position.
        Fee model supports split entry/exit rates. If fee_rate_entry/exit are None,
        falls back to legacy fee_rate for both sides.
        """
        pos = self.get_position(symbol)
        if pos is None:
            return {"pnl": 0.0, "fee": 0.0, "new_equity": None, "reason": "no_position"}

        entry = float(pos["entry"])
        qty = float(pos["qty"])
        exit_price = float(exit_price)

        # pnl
        pnl = (exit_price - entry) * qty

        # fee
        if fee_rate_entry is None or fee_rate_exit is None:
            r = float(fee_rate or 0.0)
            fee = (entry * qty + exit_price * qty) * r
        else:
            re = float(fee_rate_entry or 0.0)
            rx = float(fee_rate_exit or 0.0)
            fee = (entry * qty) * re + (exit_price * qty) * rx
        # runner/backtest parity: expose net (pnl minus fees)
        net = pnl - fee

        # remove position
        if self._replay_fast_path:
            self._positions_cache[symbol] = None
            self._positions_cache_ready = True
        self.conn.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
        self.conn.commit()

        return {
            "symbol": symbol,
            "entry": entry,
            "exit": exit_price,
            "qty": qty,
            "pnl": pnl,
            "fee": fee,
            "net": net,
            "reason": reason,
            "candle_ts_exit": int(candle_ts_exit),
            "new_equity": None,
        }


    # -------------------------
    # Trade plan logging
    # -------------------------
    def insert_trade_plan(
        self,
        symbol: str,
        regime: str,
        action: str,
        entry,
        stop,
        take_profit,
        qty,
        reason: str,
        candle_ts: int,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO trade_plan(symbol, regime, action, entry, stop, take_profit, qty, reason, candle_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                str(regime),
                str(action),
                None if entry is None else float(entry),
                None if stop is None else float(stop),
                None if take_profit is None else float(take_profit),
                None if qty is None else float(qty),
                str(reason),
                int(candle_ts),
            ),
        )
        self.conn.commit()

    # -------------------------
    # Daily metrics
    # -------------------------
    def record_daily_metrics(self, day: str, payload: dict) -> None:
        data = json.dumps(payload or {}, ensure_ascii=False)
        if self._replay_fast_path:
            try:
                parsed = json.loads(data)
            except Exception:
                parsed = {}
            self._daily_metrics_cache[str(day)] = parsed if isinstance(parsed, dict) else {}
        self.conn.execute(
            """
            INSERT INTO daily_metrics(day, payload)
            VALUES (?, ?)
            ON CONFLICT(day) DO UPDATE SET payload=excluded.payload
            """,
            (str(day), data),
        )
        self.conn.commit()

    def get_daily_metrics(self, day: str) -> dict:
        if self._replay_fast_path and str(day) in self._daily_metrics_cache:
            return dict(self._daily_metrics_cache.get(str(day)) or {})
        row = self.conn.execute(
            "SELECT payload FROM daily_metrics WHERE day=?",
            (str(day),),
        ).fetchone()
        if row is None:
            if self._replay_fast_path:
                self._daily_metrics_cache[str(day)] = {}
            return {}
        # sqlite row が dict/tuple どっちでも動くように
        payload = row["payload"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]
        if payload in (None, ""):
            if self._replay_fast_path:
                self._daily_metrics_cache[str(day)] = {}
            return {}
        try:
            data = json.loads(payload)
            out = data if isinstance(data, dict) else {}
            if self._replay_fast_path:
                self._daily_metrics_cache[str(day)] = dict(out)
            return out
        except Exception:
            if self._replay_fast_path:
                self._daily_metrics_cache[str(day)] = {}
            return {}

    def prune_daily_metrics(self, keep_days: int) -> None:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return
        if self._replay_fast_path:
            keys = sorted(self._daily_metrics_cache.keys(), reverse=True)
            for day in keys[keep_days:]:
                self._daily_metrics_cache.pop(day, None)
            return
        rows = self.conn.execute(
            "SELECT day FROM daily_metrics ORDER BY day DESC"
        ).fetchall()
        if len(rows) <= keep_days:
            return
        # sqlite row が dict/tuple どっちでも動くように
        def _day(r):
            return r["day"] if isinstance(r, dict) or hasattr(r, "keys") else r[0]
        to_delete = [_day(r) for r in rows[keep_days:]]
        self.conn.executemany(
            "DELETE FROM daily_metrics WHERE day=?",
            [(str(d),) for d in to_delete],
        )
        self.conn.commit()
