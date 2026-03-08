"""
MTF Hunter — управление PAPER позициями
"""

import uuid
import json
import logging
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional
import db

logger = logging.getLogger(__name__)
TZ = timedelta(hours=2)


def now_str() -> str:
    return (datetime.utcnow() + TZ).strftime('%Y-%m-%d %H:%M:%S')


@dataclass
class Position:
    id: str
    symbol: str
    side: str           # LONG / SHORT
    entry_price: float
    current_price: float
    stop_loss: float
    take_profit: float
    size_usdt: float
    sl_pct: float
    tp_pct: float
    rsi_at_entry: float
    trend_at_entry: str
    opened_at: str = ''
    closed_at: str = ''
    close_reason: str = ''
    status: str = 'OPEN'
    pnl_usdt: float = 0.0
    pnl_pct: float = 0.0

    def calc_pnl(self) -> tuple[float, float]:
        if self.side == 'LONG':
            pct = (self.current_price - self.entry_price) / self.entry_price * 100
        else:
            pct = (self.entry_price - self.current_price) / self.entry_price * 100
        pct -= 0.08  # комиссия
        return round(self.size_usdt * pct / 100, 4), round(pct, 3)

    def to_dict(self) -> dict:
        d = asdict(self)
        d['pnl_usdt'], d['pnl_pct'] = self.calc_pnl()
        return d


class MTFTrader:
    def __init__(self, config: dict):
        self.config      = config
        self.positions: Dict[str, Position] = {}
        self.closed:    List[dict]          = []
        self.lock       = threading.Lock()
        self.pos_size   = config.get('trading', {}).get('position_size', 100)
        self.max_pos    = config.get('trading', {}).get('max_positions', 10)
        self.max_hold_h = config.get('strategy', {}).get('max_hold_hours', 48)

    # ── Открытие позиции ─────────────────────────────────────────────────────

    def open_position(self, symbol: str, signal: dict) -> Optional[Position]:
        with self.lock:
            for p in self.positions.values():
                if p.symbol == symbol and p.status == 'OPEN':
                    return None
            if len(self.positions) >= self.max_pos:
                return None

            pos = Position(
                id            = str(uuid.uuid4())[:8],
                symbol        = symbol,
                side          = signal['direction'],
                entry_price   = signal['price'],
                current_price = signal['price'],
                stop_loss     = signal['sl'],
                take_profit   = signal['tp'],
                size_usdt     = self.pos_size,
                sl_pct        = signal['sl_pct'],
                tp_pct        = signal['tp_pct'],
                rsi_at_entry  = signal['rsi'],
                trend_at_entry= signal['trend'],
                opened_at     = now_str(),
                status        = 'OPEN',
            )
            self.positions[pos.id] = pos
            db.save_position_open(pos)
            logger.info(f"[OPEN] {symbol} {pos.side} @ {pos.entry_price} "
                        f"SL={pos.stop_loss} TP={pos.take_profit}")
            return pos

    # ── Обновление цен + проверка SL/TP/TIME ────────────────────────────────

    def update_prices(self, prices: dict):
        """prices: {symbol: current_price}"""
        with self.lock:
            to_close = []
            for pid, pos in self.positions.items():
                sym = pos.symbol.replace('/USDT:USDT', '/USDT').replace(':USDT', '')
                # Пробуем разные форматы ключа
                price = (prices.get(pos.symbol) or
                         prices.get(sym) or
                         prices.get(pos.symbol.split('/')[0]))
                if not price:
                    continue
                pos.current_price = price

                # TIME limit
                opened = datetime.strptime(pos.opened_at, '%Y-%m-%d %H:%M:%S')
                age_h  = (datetime.utcnow() + TZ - opened).total_seconds() / 3600

                reason = None
                if pos.side == 'LONG':
                    if price <= pos.stop_loss:
                        reason = 'SL'
                    elif price >= pos.take_profit:
                        reason = 'TP'
                else:
                    if price >= pos.stop_loss:
                        reason = 'SL'
                    elif price <= pos.take_profit:
                        reason = 'TP'

                if reason is None and age_h >= self.max_hold_h:
                    reason = 'TIME'

                if reason:
                    to_close.append((pid, reason, price))

            for pid, reason, price in to_close:
                self._close(pid, reason, price)

    def _close(self, pid: str, reason: str, price: float):
        pos = self.positions.pop(pid, None)
        if not pos:
            return
        pos.status        = 'CLOSED'
        pos.close_reason  = reason
        pos.closed_at     = now_str()
        pos.current_price = price
        pos.pnl_usdt, pos.pnl_pct = pos.calc_pnl()
        self.closed.append(pos.to_dict())
        db.save_position_close(pos)
        logger.info(f"[CLOSE] {pos.symbol} {pos.side} reason={reason} "
                    f"pnl={pos.pnl_usdt:+.2f}$")

    # ── Данные для UI ────────────────────────────────────────────────────────

    def get_open_positions(self) -> List[dict]:
        with self.lock:
            result = []
            for p in self.positions.values():
                d = p.to_dict()
                result.append(d)
            return result

    def get_closed_positions(self, limit: int = 50) -> List[dict]:
        with self.lock:
            return self.closed[-limit:]

    def get_stats(self) -> dict:
        with self.lock:
            closed = self.closed
            if not closed:
                return {'total': 0, 'wins': 0, 'losses': 0,
                        'wr': 0, 'total_pnl': 0, 'open': len(self.positions)}
            wins   = [t for t in closed if t['pnl_usdt'] > 0]
            losses = [t for t in closed if t['pnl_usdt'] <= 0]
            total_pnl = sum(t['pnl_usdt'] for t in closed)
            return {
                'total':     len(closed),
                'wins':      len(wins),
                'losses':    len(losses),
                'wr':        round(len(wins) / len(closed) * 100, 1),
                'total_pnl': round(total_pnl, 2),
                'open':      len(self.positions),
            }
