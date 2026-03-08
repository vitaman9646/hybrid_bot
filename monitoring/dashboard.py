"""
monitoring/dashboard.py — Streamlit dashboard

Запуск:
    streamlit run monitoring/dashboard.py --server.port 8501 --server.address 0.0.0.0
"""

import streamlit as st
import pandas as pd
import json
import re
from pathlib import Path
from datetime import datetime

LOG_FILE = "/root/hybrid_bot/logs/bot.log"
MAX_LINES = 5000

st.set_page_config(
    page_title="Hybrid Bot Dashboard",
    page_icon="??",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ------------------------------------------------------------------
# Парсинг лога
# ------------------------------------------------------------------

def read_log_lines(path: str, max_lines: int = MAX_LINES) -> list[str]:
    try:
        lines = Path(path).read_text(errors="ignore").splitlines()
        return lines[-max_lines:]
    except Exception:
        return []


def parse_log(lines: list[str]) -> dict:
    trades_open = []
    trades_closed = []
    signals = []
    filtered = []
    errors = []
    balance = None
    risk_checks = []

    for line in lines:
        # Открытие позиции
        if "Position OPEN" in line:
            trades_open.append(line)

        # Закрытие позиции
        if "Position CLOSED" in line:
            m = re.search(r"(\w+)\] Position CLOSED.*pnl=([+-]?\d+\.\d+)", line)
            if m:
                trades_closed.append({
                    'symbol': m.group(1),
                    'pnl': float(m.group(2)),
                    'time': line[:19],
                    'line': line,
                })

        # Сигналы
        if "AGGREGATED SIGNAL" in line:
            m = re.search(r"SIGNAL: \[(\w+)\] (\w+) (\w+) entry=([\d.]+).*confidence=([\d.]+)", line)
            if m:
                signals.append({
                    'scenario': m.group(1),
                    'symbol': m.group(2),
                    'direction': m.group(3),
                    'entry': float(m.group(4)),
                    'confidence': float(m.group(5)),
                    'time': line[:19],
                })

        # Отфильтрованные
        if "SIGNAL FILTERED" in line:
            m = re.search(r"FILTERED \[(\w+)\]: (.+)$", line)
            if m:
                filtered.append({
                    'symbol': m.group(1),
                    'reason': m.group(2),
                    'time': line[:19],
                })

        # Ошибки
        if "[ERROR]" in line:
            errors.append({'time': line[:19], 'msg': line[24:]})

        # Баланс
        if "Initial balance:" in line or "balance updated" in line:
            m = re.search(r"([\d.]+) USDT", line)
            if m:
                balance = float(m.group(1))

        # RiskManager check
        if "RiskManager check" in line:
            m = re.search(r"check (\w+):.*balance=([\d.]+).*size=([\d.]+)", line)
            if m:
                risk_checks.append({
                    'symbol': m.group(1),
                    'balance': float(m.group(2)),
                    'size': float(m.group(3)),
                    'time': line[:19],
                })

    return dict(
        trades_open=trades_open,
        trades_closed=trades_closed,
        signals=signals,
        filtered=filtered,
        errors=errors,
        balance=balance,
        risk_checks=risk_checks,
    )


# ------------------------------------------------------------------
# UI
# ------------------------------------------------------------------

st.title("?? Hybrid Trading Bot — Dashboard")

# Auto-refresh
refresh = st.sidebar.selectbox("Auto-refresh", [5, 10, 30, 60], index=1)
st.sidebar.markdown(f"Refresh every **{refresh}s**")

lines = read_log_lines(LOG_FILE)
data = parse_log(lines)

# Последняя активность
last_line = lines[-1] if lines else ""
last_time = last_line[:19] if last_line else "N/A"

# ------------------------------------------------------------------
# Метрики — верхняя строка
# ------------------------------------------------------------------

col1, col2, col3, col4, col5 = st.columns(5)

closed = data['trades_closed']
total_pnl = sum(t['pnl'] for t in closed)
wins = [t for t in closed if t['pnl'] > 0]
win_rate = len(wins) / len(closed) * 100 if closed else 0.0

col1.metric("Balance", f"${data['balance']:.2f}" if data['balance'] else "N/A")
col2.metric("Total P&L", f"${total_pnl:+.2f}", delta=f"{total_pnl:+.2f}")
col3.metric("Closed Trades", len(closed))
col4.metric("Win Rate", f"{win_rate:.1f}%")
col5.metric("Last Activity", last_time[11:] if len(last_time) > 11 else last_time)

st.divider()

# ------------------------------------------------------------------
# P&L график
# ------------------------------------------------------------------

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("?? Equity Curve")
    if closed:
        equity = 0.0
        equity_data = []
        for t in closed:
            equity += t['pnl']
            equity_data.append({
                'time': t['time'],
                'equity': round(equity, 4),
                'pnl': t['pnl'],
                'symbol': t['symbol'],
            })
        df_equity = pd.DataFrame(equity_data)
        st.line_chart(df_equity.set_index('time')['equity'])
    else:
        st.info("No closed trades yet")

with col_right:
    st.subheader("?? Stats")
    if closed:
        losses = [t for t in closed if t['pnl'] <= 0]
        avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t['pnl'] for t in losses) / len(losses) if losses else 0
        gross_profit = sum(t['pnl'] for t in wins)
        gross_loss = abs(sum(t['pnl'] for t in losses))
        pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        st.metric("Wins / Losses", f"{len(wins)} / {len(losses)}")
        st.metric("Avg Win", f"${avg_win:+.2f}")
        st.metric("Avg Loss", f"${avg_loss:+.2f}")
        st.metric("Profit Factor", f"{pf:.2f}" if pf != float('inf') else "∞")
    else:
        st.info("No data")

st.divider()

# ------------------------------------------------------------------
# Сигналы и фильтры
# ------------------------------------------------------------------

col_sig, col_filt = st.columns(2)

with col_sig:
    st.subheader(f"⚡ Signals ({len(data['signals'])})")
    if data['signals']:
        df_sig = pd.DataFrame(data['signals'][-20:])
        df_sig = df_sig[['time', 'symbol', 'direction', 'scenario', 'confidence']].iloc[::-1]
        st.dataframe(df_sig, use_container_width=True, hide_index=True)
    else:
        st.info("No signals")

with col_filt:
    st.subheader(f"?? Filtered ({len(data['filtered'])})")
    if data['filtered']:
        df_filt = pd.DataFrame(data['filtered'][-20:])
        # Считаем топ причин
        reason_counts = df_filt['reason'].apply(
            lambda r: r.split(':')[0].split(' ')[0]
        ).value_counts()
        st.bar_chart(reason_counts)
    else:
        st.info("No filtered signals")

st.divider()

# ------------------------------------------------------------------
# Закрытые сделки
# ------------------------------------------------------------------

st.subheader(f"?? Closed Trades ({len(closed)})")
if closed:
    df_closed = pd.DataFrame(closed[-50:]).iloc[::-1]
    df_closed['pnl'] = df_closed['pnl'].apply(lambda x: f"${x:+.4f}")
    st.dataframe(
        df_closed[['time', 'symbol', 'pnl']],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No closed trades yet")

st.divider()

# ------------------------------------------------------------------
# Ошибки
# ------------------------------------------------------------------

with st.expander(f"⚠️ Errors ({len(data['errors'])})"):
    if data['errors']:
        for e in data['errors'][-20:]:
            st.text(f"{e['time']} {e['msg']}")
    else:
        st.success("No errors")

# ------------------------------------------------------------------
# Raw log
# ------------------------------------------------------------------

with st.expander("?? Recent Log (last 50 lines)"):
    st.text("\n".join(lines[-50:]))

# Auto-refresh
st.markdown(
    f"""
    <script>
        setTimeout(function() {{ window.location.reload(); }}, {refresh * 1000});
    </script>
    """,
    unsafe_allow_html=True,
)
