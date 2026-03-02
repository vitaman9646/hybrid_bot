# monitoring/telegram_alerts.py
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class TelegramAlerts:
    """
    Отправка уведомлений в Telegram.
    Использует httpx для async HTTP (без зависимости от python-telegram-bot).
    """
    
    def __init__(self, config: dict):
        self.enabled = config.get('enabled', False)
        self.bot_token = config.get('bot_token', '')
        self.chat_id = config.get('chat_id', '')
        self.alert_on_trade = config.get('alert_on_trade', True)
        self.alert_on_error = config.get('alert_on_error', True)
        self.alert_on_latency = config.get(
            'alert_on_latency', True
        )
        
        self._base_url = (
            f"https://api.telegram.org/bot{self.bot_token}"
        )
        
        # Rate limiting для telegram (30 msg/sec)
        self._last_sent = 0.0
        self._min_interval = 0.5  # не чаще чем раз в 500мс
        
        # Дедупликация
        self._recent_messages: list[str] = []
        self._dedup_window = 60  # не повторять одинаковые за 60с
    
    async def send(self, message: str, urgent: bool = False):
        """Отправить сообщение"""
        if not self.enabled:
            return
        
        # Rate limiting
        now = time.time()
        if not urgent and now - self._last_sent < self._min_interval:
            await asyncio.sleep(
                self._min_interval - (now - self._last_sent)
            )
        
        # Дедупликация (не для urgent)
        if not urgent:
            msg_hash = message[:100]
            if msg_hash in self._recent_messages:
                return
            self._recent_messages.append(msg_hash)
            if len(self._recent_messages) > 100:
                self._recent_messages = (
                    self._recent_messages[-50:]
                )
        
        try:
            import httpx
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self._base_url}/sendMessage",
                    json={
                        'chat_id': self.chat_id,
                        'text': message,
                        'parse_mode': 'HTML',
                    },
                    timeout=10,
                )
                
                if response.status_code != 200:
                    logger.error(
                        f"Telegram send failed: "
                        f"{response.status_code}"
                    )
            
            self._last_sent = time.time()
            
        except ImportError:
            logger.warning(
                "httpx not installed, Telegram alerts disabled"
            )
            self.enabled = False
        except Exception as e:
            logger.error(f"Telegram error: {e}")
    
    async def alert_trade(
        self,
        symbol: str,
        side: str,
        price: float,
        qty: float,
        pnl: Optional[float] = None,
    ):
        if not self.alert_on_trade:
            return
        
        emoji = "🟢" if side == "Buy" else "🔴"
        msg = (
            f"{emoji} <b>{side} {symbol}</b>\n"
            f"Price: {price}\n"
            f"Qty: {qty}"
        )
        if pnl is not None:
            pnl_emoji = "✅" if pnl >= 0 else "❌"
            msg += f"\nP&L: {pnl_emoji} {pnl:.4f} USDT"
        
        await self.send(msg)
    
    async def alert_error(self, error: str):
        if not self.alert_on_error:
            return
        
        msg = f"⚠️ <b>ERROR</b>\n{error}"
        await self.send(msg, urgent=True)
    
    async def alert_latency(
        self, level: str, latency_ms: float
    ):
        if not self.alert_on_latency:
            return
        
        emoji_map = {
            'warning': '🟡',
            'critical': '🟠',
            'emergency': '🔴',
            'normal': '🟢',
        }
        emoji = emoji_map.get(level, '❓')
        
        msg = (
            f"{emoji} <b>Latency {level.upper()}</b>\n"
            f"WS Latency: {latency_ms:.0f}ms"
        )
        await self.send(msg, urgent=(level in ('critical', 'emergency')))
    
    async def alert_daily_summary(self, stats: dict):
        msg = (
            f"📊 <b>Daily Summary</b>\n"
            f"Trades: {stats.get('trades', 0)}\n"
            f"Win Rate: {stats.get('win_rate', 0):.1f}%\n"
            f"P&L: {stats.get('pnl', 0):.4f} USDT\n"
            f"Avg Slippage: {stats.get('avg_slippage', 0):.4f}%\n"
            f"API Remaining: {stats.get('api_remaining', 0)}"
        )
        await self.send(msg)
