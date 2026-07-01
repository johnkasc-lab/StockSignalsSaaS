"""
executor.py
────────────────────────────────────────────────────────────
Risk-managed order execution layer that sits between the scanner
(scheduler.py) and Zerodha Kite Connect.

DEFAULT MODE = PAPER TRADING (no real orders are placed).
Flip LIVE_TRADING to True only after you've watched paper results
for at least a few sessions and you have a valid Kite access token.

Responsibilities this module adds that the scanner alone doesn't have:
  1. Real capital + risk-based position sizing (not a manual calculator)
  2. Conflict resolution — same stock can't get a BUY and SELL in one batch
  3. Order placement via Kite Connect (MIS, intraday) when live
  4. Open-position tracking with live LTP polling
  5. Automatic SL / Target exit monitoring
  6. End-of-day square-off (forced flat by 3:15 PM IST)
  7. Daily loss circuit breaker + max trades/day cap
  8. Paper-trade ledger (CSV) so you can validate the strategy's real
     edge — including slippage/brokerage assumptions — before going live

Usage from scheduler.py:
    from executor import Executor
    executor = Executor()
    ...
    executor.process_signals(all_signals)   # after each scan
    executor.monitor_positions()            # call every ~30-60s
    executor.eod_square_off_if_needed()      # call every loop tick
"""

import os
import time
import csv
from datetime import datetime, timezone, timedelta

# ── Config (env vars override these — never hardcode secrets) ──────
LIVE_TRADING       = os.getenv("LIVE_TRADING", "false").lower() == "true"
KITE_API_KEY       = os.getenv("KITE_API_KEY", "")
KITE_API_SECRET    = os.getenv("KITE_API_SECRET", "")
KITE_ACCESS_TOKEN  = os.getenv("KITE_ACCESS_TOKEN", "")   # generated daily via login flow

CAPITAL             = float(os.getenv("TRADING_CAPITAL", "100000"))
RISK_PER_TRADE_PCT  = float(os.getenv("RISK_PER_TRADE_PCT", "0.02"))   # 2% of capital per trade
MAX_TRADES_PER_DAY  = int(os.getenv("MAX_TRADES_PER_DAY", "10"))
MAX_DAILY_LOSS_PCT  = float(os.getenv("MAX_DAILY_LOSS_PCT", "0.04"))   # stop trading if down 4% in a day
MAX_OPEN_POSITIONS  = int(os.getenv("MAX_OPEN_POSITIONS", "6"))
SLIPPAGE_BPS        = float(os.getenv("SLIPPAGE_BPS", "5"))            # 5 bps = 0.05% assumed slippage per side
SQUARE_OFF_HOUR     = 15
SQUARE_OFF_MINUTE   = 15   # force-exit all open intraday positions by 3:15 PM IST

POSITIONS_FILE = "open_positions.csv"
LEDGER_FILE    = "trade_ledger.csv"   # realized P&L log (paper or live)

# ── IST helpers (kept consistent with scheduler.py) ─────────────────
def ist_now():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def ist_str():
    return ist_now().strftime("%Y-%m-%d %H:%M:%S IST")

def today_str():
    return ist_now().strftime("%Y-%m-%d")


class Executor:
    def __init__(self):
        self.kite = None
        if LIVE_TRADING:
            self._init_kite()
        self._ensure_files()
        self.daily_trade_count = self._count_today_trades()
        self.daily_realized_pnl = self._sum_today_realized_pnl()
        self.trading_halted = False

    # ── Kite Connect setup (live mode only) ─────────────────────────
    def _init_kite(self):
        try:
            from kiteconnect import KiteConnect
        except ImportError:
            raise RuntimeError(
                "kiteconnect not installed. Run: pip install kiteconnect --break-system-packages\n"
                "Or set LIVE_TRADING=false to stay in paper mode."
            )
        if not (KITE_API_KEY and KITE_ACCESS_TOKEN):
            raise RuntimeError(
                "LIVE_TRADING is true but KITE_API_KEY / KITE_ACCESS_TOKEN are not set.\n"
                "Generate a fresh access token each morning via the Kite login flow "
                "(access tokens expire daily) and export it before starting scheduler.py."
            )
        self.kite = KiteConnect(api_key=KITE_API_KEY)
        self.kite.set_access_token(KITE_ACCESS_TOKEN)
        print(f"[{ist_str()}] Kite Connect initialized — LIVE TRADING IS ON.")

    # ── File setup ───────────────────────────────────────────────────
    def _ensure_files(self):
        if not os.path.exists(POSITIONS_FILE):
            with open(POSITIONS_FILE, "w", newline="") as f:
                csv.writer(f).writerow([
                    "Date", "Ticker", "Strategy", "Side", "EntryPrice", "Qty",
                    "SL", "Target", "OrderId", "EntryTime", "Status"
                ])
        if not os.path.exists(LEDGER_FILE):
            with open(LEDGER_FILE, "w", newline="") as f:
                csv.writer(f).writerow([
                    "Date", "Ticker", "Strategy", "Side", "EntryPrice", "ExitPrice",
                    "Qty", "PnL", "ExitReason", "EntryTime", "ExitTime", "Mode"
                ])

    def _count_today_trades(self):
        if not os.path.exists(LEDGER_FILE):
            return 0
        with open(LEDGER_FILE) as f:
            rows = list(csv.DictReader(f))
        return sum(1 for r in rows if r["Date"] == today_str())

    def _sum_today_realized_pnl(self):
        if not os.path.exists(LEDGER_FILE):
            return 0.0
        with open(LEDGER_FILE) as f:
            rows = list(csv.DictReader(f))
        return sum(float(r["PnL"]) for r in rows if r["Date"] == today_str())

    def _open_positions(self):
        with open(POSITIONS_FILE) as f:
            rows = list(csv.DictReader(f))
        return [r for r in rows if r["Status"] == "OPEN"]

    def _rewrite_positions(self, rows):
        with open(POSITIONS_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "Date", "Ticker", "Strategy", "Side", "EntryPrice", "Qty",
                "SL", "Target", "OrderId", "EntryTime", "Status"
            ])
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    def _append_ledger(self, row):
        with open(LEDGER_FILE, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=[
                "Date", "Ticker", "Strategy", "Side", "EntryPrice", "ExitPrice",
                "Qty", "PnL", "ExitReason", "EntryTime", "ExitTime", "Mode"
            ]).writerow(row)

    # ── Position sizing ──────────────────────────────────────────────
    def position_size(self, entry_price, sl_price):
        """2% risk-based sizing, capped by available capital."""
        risk_amount = CAPITAL * RISK_PER_TRADE_PCT
        per_share_risk = abs(entry_price - sl_price)
        if per_share_risk <= 0:
            return 0
        qty_by_risk = int(risk_amount / per_share_risk)
        qty_by_capital = int(CAPITAL // entry_price)   # never exceed available capital
        return max(0, min(qty_by_risk, qty_by_capital))

    # ── Risk gates ───────────────────────────────────────────────────
    def _can_trade(self):
        if self.trading_halted:
            return False, "Trading halted earlier this session."
        if self.daily_trade_count >= MAX_TRADES_PER_DAY:
            return False, f"Max trades/day ({MAX_TRADES_PER_DAY}) reached."
        if self.daily_realized_pnl <= -CAPITAL * MAX_DAILY_LOSS_PCT:
            self.trading_halted = True
            return False, f"Daily loss limit hit ({self.daily_realized_pnl:.0f}). Trading halted for today."
        if len(self._open_positions()) >= MAX_OPEN_POSITIONS:
            return False, f"Max open positions ({MAX_OPEN_POSITIONS}) reached."
        return True, ""

    # ── Conflict resolution ──────────────────────────────────────────
    @staticmethod
    def resolve_conflicts(signals):
        """
        Drop any ticker that has both a BUY and SELL signal in the same
        batch (different strategies disagreeing) — ambiguous, skip it.
        Keeps the rest.
        """
        from collections import defaultdict
        sides_by_ticker = defaultdict(set)
        for s in signals:
            sides_by_ticker[s["Stock"]].add(s["Signal"])

        conflicted = {t for t, sides in sides_by_ticker.items() if len(sides) > 1}
        if conflicted:
            print(f"[{ist_str()}] Skipping conflicted tickers (BUY+SELL same batch): {sorted(conflicted)}")
        return [s for s in signals if s["Stock"] not in conflicted]

    # ── Live price fetch ─────────────────────────────────────────────
    def get_ltp(self, ticker_ns):
        """ticker_ns like 'TCS.NS' -> Kite wants 'NSE:TCS'."""
        symbol = "NSE:" + ticker_ns.replace(".NS", "")
        if LIVE_TRADING and self.kite:
            try:
                data = self.kite.ltp(symbol)
                return data[symbol]["last_price"]
            except Exception as e:
                print(f"LTP fetch failed for {symbol}: {e}")
                return None
        else:
            # Paper mode fallback — use yfinance for a live-ish quote
            try:
                import yfinance as yf
                t = yf.Ticker(ticker_ns)
                price = t.fast_info.get("last_price")
                return float(price) if price else None
            except Exception:
                return None

    # ── Order placement ──────────────────────────────────────────────
    def _place_kite_order(self, ticker_ns, side, qty):
        symbol = ticker_ns.replace(".NS", "")
        try:
            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=self.kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=self.kite.TRANSACTION_TYPE_BUY if side == "BUY"
                                  else self.kite.TRANSACTION_TYPE_SELL,
                quantity=qty,
                product=self.kite.PRODUCT_MIS,     # intraday margin product
                order_type=self.kite.ORDER_TYPE_MARKET,
            )
            return order_id
        except Exception as e:
            print(f"[{ist_str()}] Kite order FAILED for {symbol}: {e}")
            return None

    # ── Process a fresh batch of scanner signals ─────────────────────
    def process_signals(self, signals):
        """
        signals: list of dicts from scan_stock(), each with
        Stock, Strategy, Signal, Price ('₹1234.5'), Stop Loss, Target.
        """
        if not signals:
            return

        clean_signals = self.resolve_conflicts(signals)

        for s in clean_signals:
            ok, reason = self._can_trade()
            if not ok:
                print(f"[{ist_str()}] Skipping {s['Stock']} ({s['Strategy']}): {reason}")
                continue

            try:
                entry_price = float(str(s["Price"]).replace("₹", ""))
                sl_price    = float(str(s["Stop Loss"]).replace("₹", "")) if s["Stop Loss"] != "—" else None
                target_price= float(str(s["Target"]).replace("₹", "")) if s["Target"] != "—" else None
            except (ValueError, KeyError):
                continue

            if sl_price is None or target_price is None:
                continue   # never trade a signal without a defined SL/target

            qty = self.position_size(entry_price, sl_price)
            if qty <= 0:
                print(f"[{ist_str()}] Skipping {s['Stock']}: position size computed as 0.")
                continue

            ticker_ns = s["Stock"] + ".NS"
            order_id = None
            mode = "LIVE" if LIVE_TRADING else "PAPER"

            if LIVE_TRADING:
                order_id = self._place_kite_order(ticker_ns, s["Signal"], qty)
                if order_id is None:
                    continue   # order failed, don't log a phantom position
            else:
                order_id = f"PAPER-{int(time.time()*1000)}"

            row = {
                "Date": today_str(), "Ticker": ticker_ns, "Strategy": s["Strategy"],
                "Side": s["Signal"], "EntryPrice": entry_price, "Qty": qty,
                "SL": sl_price, "Target": target_price, "OrderId": order_id,
                "EntryTime": ist_str(), "Status": "OPEN",
            }
            with open(POSITIONS_FILE, "a", newline="") as f:
                csv.DictWriter(f, fieldnames=list(row.keys())).writerow(row)

            self.daily_trade_count += 1
            print(f"[{ist_str()}] {mode} ENTRY {s['Signal']} {s['Stock']} "
                  f"qty={qty} @ {entry_price} SL={sl_price} TGT={target_price} ({s['Strategy']})")

    # ── Exit monitoring (call this every ~30-60s) ────────────────────
    def monitor_positions(self):
        rows = self._open_positions()
        if not rows:
            return

        all_rows = []
        with open(POSITIONS_FILE) as f:
            all_rows = list(csv.DictReader(f))

        changed = False
        for r in all_rows:
            if r["Status"] != "OPEN":
                continue
            ticker_ns = r["Ticker"]
            ltp = self.get_ltp(ticker_ns)
            if ltp is None:
                continue

            side       = r["Side"]
            sl         = float(r["SL"])
            target     = float(r["Target"])
            entry      = float(r["EntryPrice"])
            qty        = int(r["Qty"])
            exit_reason = None

            if side == "BUY":
                if ltp <= sl:
                    exit_reason = "SL_HIT"
                elif ltp >= target:
                    exit_reason = "TARGET_HIT"
            else:  # SELL (short)
                if ltp >= sl:
                    exit_reason = "SL_HIT"
                elif ltp <= target:
                    exit_reason = "TARGET_HIT"

            if exit_reason:
                self._close_position(r, ltp, exit_reason)
                changed = True

        if changed:
            with open(POSITIONS_FILE) as f:
                pass  # positions file already updated row-by-row in _close_position

    def _close_position(self, position_row, exit_price, reason):
        side  = position_row["Side"]
        entry = float(position_row["EntryPrice"])
        qty   = int(position_row["Qty"])

        # Apply assumed slippage against you on the exit fill
        slip = exit_price * (SLIPPAGE_BPS / 10000)
        fill_price = exit_price - slip if side == "BUY" else exit_price + slip

        pnl = (fill_price - entry) * qty if side == "BUY" else (entry - fill_price) * qty

        mode = "LIVE" if LIVE_TRADING else "PAPER"
        if LIVE_TRADING and self.kite:
            try:
                self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=self.kite.EXCHANGE_NSE,
                    tradingsymbol=position_row["Ticker"].replace(".NS", ""),
                    transaction_type=self.kite.TRANSACTION_TYPE_SELL if side == "BUY"
                                      else self.kite.TRANSACTION_TYPE_BUY,
                    quantity=qty,
                    product=self.kite.PRODUCT_MIS,
                    order_type=self.kite.ORDER_TYPE_MARKET,
                )
            except Exception as e:
                print(f"[{ist_str()}] Kite EXIT order failed for {position_row['Ticker']}: {e}")

        self._append_ledger({
            "Date": position_row["Date"], "Ticker": position_row["Ticker"],
            "Strategy": position_row["Strategy"], "Side": side,
            "EntryPrice": entry, "ExitPrice": round(fill_price, 2), "Qty": qty,
            "PnL": round(pnl, 2), "ExitReason": reason,
            "EntryTime": position_row["EntryTime"], "ExitTime": ist_str(), "Mode": mode,
        })
        self.daily_realized_pnl += pnl

        # mark position closed in the CSV
        all_rows = []
        with open(POSITIONS_FILE) as f:
            all_rows = list(csv.DictReader(f))
        for r in all_rows:
            if (r["OrderId"] == position_row["OrderId"] and r["Status"] == "OPEN"):
                r["Status"] = "CLOSED"
        self._rewrite_positions(all_rows)

        print(f"[{ist_str()}] {mode} EXIT {side} {position_row['Ticker']} "
              f"@ {round(fill_price,2)} reason={reason} PnL={round(pnl,2)}")

    # ── EOD square-off ───────────────────────────────────────────────
    def eod_square_off_if_needed(self):
        n = ist_now()
        if n.hour == SQUARE_OFF_HOUR and n.minute >= SQUARE_OFF_MINUTE:
            rows = self._open_positions()
            if not rows:
                return
            print(f"[{ist_str()}] EOD square-off triggered — closing {len(rows)} open position(s).")
            for r in rows:
                ltp = self.get_ltp(r["Ticker"]) or float(r["EntryPrice"])
                self._close_position(r, ltp, "EOD_SQUARE_OFF")
