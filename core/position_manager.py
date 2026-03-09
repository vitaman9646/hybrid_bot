from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, TYPE_CHECKING

from execution.order_executor import OrderExecutor
from storage.trade_exporter import TradeExporter
from core.volatility_tracker import VolatilityTracker

if TYPE_CHECKING:
    from analyzers.signal_aggregator import AggregatedSignal

logger = logging.getLogger(__name__)


class PositionState(Enum):
    OPENING   = "opening"
    OPEN      = "open"
    CLOSING   = "closing"
    CLOSED    = "closed"
    ERROR     = "error"


@dataclass
class Position:
    """Одна открытая позиция."""
    symbol: str
    direction: str          # 'long' / 'short'
    entry_price: float
    qty: float
    tp_price: float
    sl_price: float
    state: PositionState = PositionState.OPENING
    timestamp: float = field(default_factory=time.time)

    # Order IDs
    entry_order_id: str = ""
    tp_order_id: str = ""
    sl_order_id: str = ""

    # Trailing
    trailing_enabled: bool = False
    trailing_stop_price: float = 0.0
    peak_price: float = 0.0      # лучшая цена с момента открытия
    scenario: str = ""

    # v3: Partial TP
    partial_tp_enabled: bool = False
    partial_tp_price: float = 0.0    # цена первого частичного TP
    partial_tp_fraction: float = 0.5 # доля позиции для закрытия
    partial_tp_done: bool = False     # уже исполнен

    # P&L
    realized_pnl: float = 0.0
    close_price: float = 0.0
    close_reason: str = ""

    @property
    def side(self) -> str:
        return 'Buy' if self.direction == 'long' else 'Sell'

    @property
    def close_side(self) -> str:
        return 'Sell' if self.direction == 'long' else 'Buy'

    @property
    def unrealized_pnl_pct(self) -> float:
        """Текущий P&L в % от entry (требует текущую цену)."""
        return 0.0  # обновляется снаружи

    def sl_triggered(self, current_price: float) -> bool:
        if self.direction == 'long':
            return current_price <= self.sl_price
        return current_price >= self.sl_price

    def tp_triggered(self, current_price: float) -> bool:
        if self.direction == 'long':
            return current_price >= self.tp_price
        return current_price <= self.tp_price


class PositionManager:
    """
    Фаза 3: Управление позициями.

    Функции:
    - open_position(signal) — открыть позицию по сигналу
    - update_price(symbol, price) — обновить trailing stop
    - close_position(symbol, reason) — закрыть позицию
    - Лимит одновременных позиций (max_positions)
    - Max drawdown per position
    - Trailing stop (адаптивный через VolatilityTracker)
    - Блокировка повторных входов по символу
    """

    def __init__(
        self,
        config: dict,
        executor: OrderExecutor,
        volatility_tracker: VolatilityTracker,
    ):
        order_cfg = config.get('order', {})
        trailing_cfg = order_cfg.get('trailing', {})
        sl_cfg = order_cfg.get('stop_loss', {})
        tp_cfg = order_cfg.get('take_profit', {})

        # Размер позиции
        self._size_usdt: float = order_cfg.get('size_usdt', 50.0)

        # TP / SL
        self._tp_pct: float = tp_cfg.get('percent', 0.8)
        self._sl_pct: float = sl_cfg.get('percent', 1.5)
        self._sl_type: str = sl_cfg.get('type', 'market')      # market / limit

        # Trailing
        self._trailing_enabled: bool = trailing_cfg.get('enabled', True)
        self._trailing_adaptive: bool = trailing_cfg.get('adaptive', True)
        self._trailing_spread: float = trailing_cfg.get('spread', 0.3)  # %
        # Trailing активируется только после движения в плюс на X%
        self._trailing_activation_pct: float = trailing_cfg.get('activation_pct', 0.4)

        # Лимиты
        self._max_positions: int = config.get('max_positions', 5)
        self._max_drawdown_pct: float = order_cfg.get('max_drawdown_pct', 5.0)

        # Компоненты
        self._executor = executor
        self._volatility = volatility_tracker

        # RiskManager (опционально, подключается извне)
        self._risk_manager = None

        # CircuitBreaker (опционально, подключается извне)
        self._circuit_breaker = None

        # TelegramAlerts (опционально, подключается извне)
        self._alerts = None

        # CircuitBreaker (опционально, подключается извне)
        self._circuit_breaker = None

        # Состояние
        self._positions: dict[str, Position] = {}   # symbol → Position
        self._closed_positions: list[Position] = []

        # Stats
        self._exporter = TradeExporter()
        self._stats = {
            'total_opened': 0,
            'total_closed': 0,
            'blocked_max_positions': 0,
            'blocked_already_open': 0,
            'trailing_updates': 0,
            'sl_hits': 0,
            'tp_hits': 0,
            'manual_closes': 0,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def open_position(self, signal: "AggregatedSignal") -> Optional[Position]:
        """
        Открыть позицию по сигналу от агрегатора.
        Возвращает Position или None если заблокировано.
        """
        symbol = signal.symbol
        direction = signal.direction.value  # 'long' / 'short'

        # Блокировка: уже есть позиция по символу
        if symbol in self._positions:
            self._stats['blocked_already_open'] += 1
            logger.info(f"[{symbol}] Position already open, skipping signal")
            return None

        # Блокировка: превышен лимит позиций
        if len(self._positions) >= self._max_positions:
            self._stats['blocked_max_positions'] += 1
            logger.warning(
                f"[{symbol}] Max positions reached "
                f"({self._max_positions}), skipping"
            )
            return None

        # Считаем цены
        entry = signal.entry_price
        tp = self._calc_tp(direction, entry, signal.tp_price, symbol)
        sl = self._calc_sl(direction, entry, symbol)

        # Получаем qty — используем size_usdt из RiskManager если задан
        size_usdt = signal.size_usdt if signal.size_usdt > 0 else self._size_usdt
        qty = await self._calc_qty(symbol, entry, size_usdt=size_usdt)
        if qty is None or qty <= 0:
            logger.error(f"[{symbol}] Failed to calculate qty")
            return None

        logger.info(
            f"Opening position: {symbol} {direction} "
            f"entry={entry:.4f} tp={tp:.4f} sl={sl:.4f} "
            f"qty={qty} usdt={size_usdt:.2f}"
        )

        # Создаём объект позиции
        pos = Position(
            symbol=symbol,
            direction=direction,
            entry_price=entry,
            qty=qty,
            tp_price=tp,
            sl_price=sl,
            trailing_enabled=self._trailing_enabled,
            peak_price=entry,
            trailing_stop_price=sl,
            scenario=getattr(signal, "scenario", "").value if hasattr(getattr(signal, "scenario", ""), "value") else getattr(signal, "scenario", ""),
            # v3: Partial TP — первый уровень на середине до TP
            partial_tp_enabled=True,
            partial_tp_price=(entry + tp) / 2 if direction == 'long' else (entry + tp) / 2,
            partial_tp_fraction=0.5,
        )

        self._positions[symbol] = pos

        # Размещаем entry ордер
        entry_side = 'Buy' if direction == 'long' else 'Sell'
        entry_result = await self._executor.place_order(
            symbol=symbol,
            side=entry_side,
            order_type='Market',
            qty=qty,
        )

        if not entry_result:
            logger.error(f"[{symbol}] Entry order failed")
            pos.state = PositionState.ERROR
            del self._positions[symbol]
            return None

        pos.entry_order_id = entry_result.get('orderId', '')
        pos.state = PositionState.OPEN
        self._stats['total_opened'] += 1

        logger.info(
            f"[{symbol}] Position OPEN: "
            f"order_id={pos.entry_order_id}"
        )

        # Размещаем TP и SL параллельно
        await asyncio.gather(
            self._place_tp(pos),
            self._place_sl(pos),
        )

        return pos

    async def update_price(self, symbol: str, price: float):
        """
        Вызывается на каждый трейд. Обновляет trailing stop,
        проверяет SL/TP если ордера не были размещены на бирже.
        """
        pos = self._positions.get(symbol)
        if not pos or pos.state != PositionState.OPEN:
            return

        # Обновляем peak price
        if pos.direction == 'long':
            if price > pos.peak_price:
                pos.peak_price = price
        else:
            if price < pos.peak_price:
                pos.peak_price = price

        # Trailing stop
        if pos.trailing_enabled:
            await self._update_trailing(pos, price)

        # v3: Partial TP
        if pos.partial_tp_enabled and not pos.partial_tp_done and pos.partial_tp_price > 0:
            hit = (pos.direction == 'long' and price >= pos.partial_tp_price) or                   (pos.direction == 'short' and price <= pos.partial_tp_price)
            if hit:
                partial_qty = round(pos.qty * pos.partial_tp_fraction, 8)
                logger.info(
                    "[%s] Partial TP hit at %.4f — closing %.4f (%.0f%% of position)",
                    symbol, price, partial_qty, pos.partial_tp_fraction * 100
                )
                close_fn = getattr(self._executor, 'close_position', None)
                if close_fn is None:
                    ok = False
                else:
                    import asyncio
                    if asyncio.iscoroutinefunction(close_fn):
                        ok = await close_fn(symbol=symbol, qty=partial_qty,
                                           side='sell' if pos.direction == 'long' else 'buy')
                    else:
                        ok = close_fn(symbol=symbol, qty=partial_qty,
                                     side='sell' if pos.direction == 'long' else 'buy')
                if ok:
                    pos.qty -= partial_qty
                    pos.partial_tp_done = True
                    # Переносим SL на breakeven
                    be_price = pos.entry_price * 1.001 if pos.direction == 'long' else pos.entry_price * 0.999
                    if pos.direction == 'long' and be_price > pos.trailing_stop_price:
                        pos.trailing_stop_price = be_price
                        pos._breakeven_set = True
                    elif pos.direction == 'short' and be_price < pos.trailing_stop_price:
                        pos.trailing_stop_price = be_price
                        pos._breakeven_set = True
                    logger.info("[%s] SL moved to breakeven %.4f after partial TP", symbol, be_price)

        # Проверяем max drawdown (hard stop независимо от SL ордера)
        drawdown = self._calc_drawdown_pct(pos, price)
        if drawdown >= self._max_drawdown_pct:
            logger.warning(
                f"[{symbol}] Max drawdown hit: {drawdown:.2f}% "
                f"(limit={self._max_drawdown_pct}%)"
            )
            await self.close_position(symbol, reason='max_drawdown', current_price=price)

    async def close_position(
        self,
        symbol: str,
        reason: str = 'manual',
        current_price: float = 0.0,
    ) -> bool:
        """Закрыть позицию по символу."""
        pos = self._positions.get(symbol)
        if not pos:
            logger.warning(f"[{symbol}] No position to close")
            return False

        if pos.state == PositionState.CLOSING:
            return False  # уже закрывается

        pos.state = PositionState.CLOSING
        pos.close_reason = reason

        logger.info(f"[{symbol}] Closing position: reason={reason}")

        # Отменяем TP и SL ордера
        await asyncio.gather(
            self._executor.cancel_order(symbol, pos.tp_order_id),
            self._executor.cancel_order(symbol, pos.sl_order_id),
        )

        # Закрываем позицию маркет ордером
        close_result = await self._executor.place_order(
            symbol=symbol,
            side=pos.close_side,
            order_type='Market',
            qty=pos.qty,
            reduce_only=True,
        )

        if close_result:
            pos.state = PositionState.CLOSED
            pos.close_price = current_price or pos.entry_price
            pos.realized_pnl = self._calc_pnl(pos)

            logger.info(
                f"[{symbol}] Position CLOSED: "
                f"reason={reason} "
                f"pnl={pos.realized_pnl:+.2f} USDT"
            )

            # Обновляем статы
            self._stats['total_closed'] += 1
            if reason == 'sl':
                self._stats['sl_hits'] += 1
            elif reason == 'tp':
                self._stats['tp_hits'] += 1
            elif reason == 'manual':
                self._stats['manual_closes'] += 1

            # Архивируем
            self._closed_positions.append(pos)
            if len(self._closed_positions) > 500:
                self._closed_positions = self._closed_positions[-500:]

            del self._positions[symbol]

            # Уведомляем агрегатор об исходе сделки (умный cooldown)
            if hasattr(self, '_aggregator') and self._aggregator is not None:
                exit_reason = reason if reason in ('tp', 'sl') else 'sl'
                self._aggregator.notify_exit(symbol, exit_reason)

            # Уведомляем RiskManager если он подключён
            if self._risk_manager is not None:
                self._risk_manager.record_close(symbol, pos.realized_pnl)
                if self._circuit_breaker is not None:
                    avg_slip = self._executor.get_avg_slippage()
                    balance = self._risk_manager._balance_usdt
                    self._circuit_breaker.on_trade_closed(pos.realized_pnl, avg_slip, balance)
                if self._circuit_breaker is not None:
                    avg_slip = self._executor.get_avg_slippage()
                    balance = self._risk_manager._balance_usdt
                    self._circuit_breaker.on_trade_closed(pos.realized_pnl, avg_slip, balance)
                self._exporter.export(pos)

            return True

        else:
            logger.error(f"[{symbol}] Close order FAILED, position stays open")
            pos.state = PositionState.OPEN  # откатываем
            return False

    async def sync_with_exchange(self):
        """
        Синхронизирует внутреннее состояние с биржей.
        Если позиция закрыта на бирже — убираем из _positions.
        Вызывается периодически из engine.
        """
        if not self._positions:
            return

        try:
            loop = asyncio.get_running_loop()
            from functools import partial
            result = await loop.run_in_executor(
                None,
                partial(
                    self._executor._client.get_positions,
                    category='linear',
                    settleCoin='USDT',
                ),
            )

            if result['retCode'] != 0:
                return

            # Символы с открытыми позициями на бирже
            open_on_exchange = {
                p['symbol']
                for p in result['result']['list']
                if float(p['size']) > 0
            }

            # Находим позиции которые закрылись на бирже
            closed = [
                sym for sym in list(self._positions.keys())
                if sym not in open_on_exchange
            ]

            # Получаем реальный PnL закрытых позиций с биржи
            real_pnl: dict[str, float] = {}
            if closed:
                try:
                    pnl_result = await loop.run_in_executor(
                        None,
                        partial(
                            self._executor._client.get_closed_pnl,
                            category='linear',
                            limit=50,
                        ),
                    )
                    if pnl_result.get('retCode') == 0:
                        for item in pnl_result['result']['list']:
                            s = item['symbol']
                            if s in closed:
                                real_pnl[s] = float(item.get('closedPnl', 0.0))
                except Exception as e:
                    logger.warning(f"Failed to fetch closed PnL: {e}")

            for sym in closed:
                pos = self._positions.pop(sym)
                pos.state = PositionState.CLOSED
                if sym in real_pnl:
                    pos.realized_pnl = real_pnl[sym]
                # Определяем причину по PnL
                if pos.realized_pnl > 0:
                    pos.close_reason = 'tp'
                    self._stats['tp_hits'] += 1
                elif pos.realized_pnl < 0:
                    pos.close_reason = 'sl'
                    self._stats['sl_hits'] += 1
                else:
                    pos.close_reason = 'exchange_sync'
                self._closed_positions.append(pos)
                self._stats['total_closed'] += 1
                logger.info(
                    f"[{sym}] Position CLOSED: "
                    f"entry={pos.entry_price:.4f} "
                    f"pnl={pos.realized_pnl:+.2f} USDT "
                    f"(detected via sync)"
                )
                if self._risk_manager is not None:
                    self._risk_manager.record_close(sym, pos.realized_pnl)
                    self._exporter.export(pos)

        except Exception as e:
            logger.warning(f"Position sync error: {e}")

    def get_position(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol)

    def get_all_positions(self) -> list[Position]:
        return list(self._positions.values())

    def has_position(self, symbol: str) -> bool:
        return symbol in self._positions

    def get_direction(self, symbol: str):
        """Возвращает Direction открытой позиции или None."""
        pos = self._positions.get(symbol)
        return pos.direction if pos else None

    def get_stats(self) -> dict:
        return {
            **self._stats,
            'open_positions': len(self._positions),
            'open_symbols': list(self._positions.keys()),
        }

    # ------------------------------------------------------------------
    # Internal — TP / SL placement
    # ------------------------------------------------------------------

    async def _place_tp(self, pos: Position):
        tp_id = await self._executor.place_tp(
            symbol=pos.symbol,
            side=pos.close_side,
            price=pos.tp_price,
            qty=pos.qty,
        )
        if tp_id:
            pos.tp_order_id = tp_id
            logger.info(f"[{pos.symbol}] TP placed at {pos.tp_price:.4f} id={tp_id}")
        else:
            logger.warning(f"[{pos.symbol}] TP placement failed")

    async def _place_sl(self, pos: Position):
        sl_id = await self._executor.place_sl(
            symbol=pos.symbol,
            side=pos.close_side,
            trigger_price=pos.sl_price,
            qty=pos.qty,
        )
        if sl_id:
            pos.sl_order_id = sl_id
            logger.info(f"[{pos.symbol}] SL placed at {pos.sl_price:.4f} id={sl_id}")
        else:
            logger.warning(f"[{pos.symbol}] SL placement failed")
            if self._alerts is not None:
                import asyncio
                asyncio.create_task(self._alerts.send(
                    "\U0001f6a8 <b>SL FAILURE</b>\n"
                    f"Symbol: {pos.symbol}\n"
                    f"Direction: {pos.direction}\n"
                    f"SL price: {pos.sl_price:.4f}\n"
                    "Position is UNPROTECTED!"
                ))

    # ------------------------------------------------------------------
    # Internal — Trailing stop
    # ------------------------------------------------------------------

    async def _update_trailing(self, pos: Position, price: float):
        """
        Двигаем trailing stop за ценой.
        Активируется только после движения на activation_pct в плюс.
        Адаптивный spread через VolatilityTracker если включён.
        """
        # Проверяем активацию trailing — только после +activation_pct
        if pos.direction == 'long':
            profit_pct = (price - pos.entry_price) / pos.entry_price * 100
        else:
            profit_pct = (pos.entry_price - price) / pos.entry_price * 100

        if profit_pct < self._trailing_activation_pct:
            return  # ещё не активирован

        # v3: Breakeven SL при +1.5×ATR
        atr_pct = 0.0
        if self._volatility:
            vol = self._volatility.get_volatility(pos.symbol)
            if isinstance(vol, (int, float)) and vol > 0:
                atr_pct = vol
        breakeven_trigger_pct = atr_pct * 1.5 if atr_pct > 0 else self._trailing_activation_pct * 2
        if profit_pct >= breakeven_trigger_pct and not getattr(pos, '_breakeven_set', False):
            breakeven_price = pos.entry_price * 1.001 if pos.direction == 'long' else pos.entry_price * 0.999
            if pos.direction == 'long' and breakeven_price > pos.trailing_stop_price:
                pos.trailing_stop_price = breakeven_price
                pos._breakeven_set = True
                logger.info("[%s] Breakeven SL set at %.4f (+0.1%% from entry, profit=%.2f%%)",
                            pos.symbol, breakeven_price, profit_pct)
            elif pos.direction == 'short' and breakeven_price < pos.trailing_stop_price:
                pos.trailing_stop_price = breakeven_price
                pos._breakeven_set = True
                logger.info("[%s] Breakeven SL set at %.4f (-0.1%% from entry, profit=%.2f%%)",
                            pos.symbol, breakeven_price, profit_pct)

        spread_pct = self._get_trailing_spread(pos.symbol)

        if pos.direction == 'long':
            new_stop = pos.peak_price * (1 - spread_pct / 100)
            if new_stop > pos.trailing_stop_price:
                old_stop = pos.trailing_stop_price
                pos.trailing_stop_price = new_stop
                self._stats['trailing_updates'] += 1
                logger.debug(
                    f"[{pos.symbol}] Trailing stop: "
                    f"{old_stop:.4f} → {new_stop:.4f} "
                    f"(peak={pos.peak_price:.4f})"
                )
                # Обновляем SL ордер на бирже
                if pos.sl_order_id and pos.state == PositionState.OPEN:
                    ok = await self._executor.modify_order(
                        symbol=pos.symbol,
                        order_id=pos.sl_order_id,
                        trigger_price=new_stop,
                    )
                    if not ok:
                        pos.sl_order_id = ""  # ордер уже не существует
        else:
            new_stop = pos.peak_price * (1 + spread_pct / 100)
            if new_stop < pos.trailing_stop_price or pos.trailing_stop_price == pos.sl_price:
                old_stop = pos.trailing_stop_price
                pos.trailing_stop_price = new_stop
                self._stats['trailing_updates'] += 1
                logger.debug(
                    f"[{pos.symbol}] Trailing stop: "
                    f"{old_stop:.4f} → {new_stop:.4f} "
                    f"(peak={pos.peak_price:.4f})"
                )
                if pos.sl_order_id and pos.state == PositionState.OPEN:
                    ok = await self._executor.modify_order(
                        symbol=pos.symbol,
                        order_id=pos.sl_order_id,
                        trigger_price=new_stop,
                    )
                    if not ok:
                        pos.sl_order_id = ""  # ордер уже не существует

    def _get_trailing_spread(self, symbol: str) -> float:
        """Адаптивный spread на основе волатильности."""
        if not self._trailing_adaptive:
            return self._trailing_spread

        return self._volatility.get_adaptive_trailing_spread(
            symbol, base_spread=self._trailing_spread
        )

    # ------------------------------------------------------------------
    # Internal — calculations
    # ------------------------------------------------------------------

    def _calc_tp(
        self, direction: str, entry: float, signal_tp: float, symbol: str = ''
    ) -> float:
        """v3: адаптивный TP = max(SL×2, ATR×1.8, config_tp).
        Если сигнал даёт разумный TP — используем его если он >= SL×1.5.
        """
        # Считаем SL distance для минимального RR
        sl_pct = self._sl_pct
        if symbol and self._volatility:
            vol_pct = self._volatility.get_volatility(symbol)
            if isinstance(vol_pct, (int, float)) and vol_pct > 0:
                sl_pct = max(sl_pct, vol_pct * 2.5)

        # Адаптивный TP
        atr_tp_pct = 0.0
        if symbol and self._volatility:
            vol_pct = self._volatility.get_volatility(symbol)
            if isinstance(vol_pct, (int, float)) and vol_pct > 0:
                atr_tp_pct = vol_pct * 1.8

        tp_pct = max(
            sl_pct * 2.0,       # минимум RR 1:2
            atr_tp_pct,         # ATR×1.8
            self._tp_pct,       # config floor
        )

        # Если сигнал даёт TP и он >= SL×1.5 — используем
        pct_from_signal = abs(signal_tp - entry) / entry * 100
        if 0.05 < pct_from_signal < 10.0 and pct_from_signal >= sl_pct * 1.5:
            logger.debug("[%s] TP from signal: %.2f%% (sl=%.2f%%)", symbol, pct_from_signal, sl_pct)
            return signal_tp

        logger.debug("[%s] TP adaptive: %.2f%% (sl×2=%.2f%% atr×1.8=%.2f%% cfg=%.2f%%)",
                     symbol, tp_pct, sl_pct * 2.0, atr_tp_pct, self._tp_pct)
        if direction == 'long':
            return entry * (1 + tp_pct / 100)
        return entry * (1 - tp_pct / 100)

    def _calc_sl(self, direction: str, entry: float, symbol: str = '') -> float:
        """SL = max(min_sl_pct, ATR×2.5). ATR = volatility_tracker.get_volatility()."""
        min_sl_pct = self._sl_pct  # из конфига (1.0% по умолчанию, рекомендуем 1.2%)
        atr_mult = 2.5

        if symbol and self._volatility:
            vol_pct = self._volatility.get_volatility(symbol)
            if isinstance(vol_pct, (int, float)) and vol_pct > 0:
                atr_sl_pct = vol_pct * atr_mult
                sl_pct = max(min_sl_pct, atr_sl_pct)
                logger.debug(
                    "[%s] SL: min=%.2f%% atr=%.2f%% (vol=%.2f%%×%.1f) → used=%.2f%%",
                    symbol, min_sl_pct, atr_sl_pct, vol_pct, atr_mult, sl_pct,
                )
            else:
                sl_pct = min_sl_pct
        else:
            sl_pct = min_sl_pct

        if direction == 'long':
            return entry * (1 - sl_pct / 100)
        return entry * (1 + sl_pct / 100)

    async def _calc_qty(
        self, symbol: str, price: float, size_usdt: float = 0.0
    ) -> Optional[float]:
        """Рассчитать qty из size_usdt с учётом минимального лота."""
        if price <= 0:
            return None
        if size_usdt <= 0:
            size_usdt = self._size_usdt
        qty = size_usdt / price

        # Получаем минимальный лот и шаг из instrument info
        lot_info = await self._get_min_qty(symbol)
        if lot_info:
            min_qty, qty_step = lot_info
            # Округляем вниз до шага
            if qty_step > 0:
                import math
                qty = math.floor(qty / qty_step) * qty_step
            if qty < min_qty:
                logger.info(
                    f"[{symbol}] qty {qty:.6f} < minOrderQty {min_qty}, "
                    f"adjusting to {min_qty}"
                )
                qty = min_qty

        return round(qty, 6)

    async def _get_min_qty(self, symbol: str) -> Optional[float]:
        """Получить минимальный размер ордера для символа."""
        try:
            import asyncio
            from functools import partial
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                partial(
                    self._executor._client.get_instruments_info,
                    category='linear',
                    symbol=symbol,
                ),
            )
            if result['retCode'] == 0:
                items = result['result']['list']
                if items:
                    lot_filter = items[0].get('lotSizeFilter', {})
                    min_qty = float(lot_filter.get('minOrderQty', 0))
                    qty_step = float(lot_filter.get('qtyStep', 0.001))
                    return (min_qty, qty_step) if min_qty > 0 else None
        except Exception as e:
            logger.warning(f"[{symbol}] Failed to get minOrderQty: {e}")
        return None

    def _calc_drawdown_pct(self, pos: Position, price: float) -> float:
        if pos.entry_price <= 0:
            return 0.0
        if pos.direction == 'long':
            return (pos.entry_price - price) / pos.entry_price * 100
        return (price - pos.entry_price) / pos.entry_price * 100

    def _calc_pnl(self, pos: Position) -> float:
        if pos.close_price <= 0 or pos.entry_price <= 0:
            return 0.0
        if pos.direction == 'long':
            return (pos.close_price - pos.entry_price) / pos.entry_price * pos.qty * pos.entry_price
        return (pos.entry_price - pos.close_price) / pos.entry_price * pos.qty * pos.entry_price
