import sqlite3
from collections import deque
from datetime import datetime, timezone

TAKER_FEE=0.00055
SIZE_USDT=50.0
DB='/home/vitaman/tick_bars_1s.db'
CONFIGS=[('ETHUSDT',0.015,1.0),('SOLUSDT',0.015,1.0),('DOGEUSDT',0.015,1.0),('AVAXUSDT',0.015,1.0)]

conn=sqlite3.connect(DB)
ALL_BARS={sym:conn.execute('SELECT ts,price,buy_vol,sell_vol FROM bars1s WHERE symbol=? ORDER BY ts ASC',(sym,)).fetchall() for sym,_,_ in CONFIGS}
conn.close()

# Середина: 22.02.2026
SPLIT=datetime(2026,2,22,tzinfo=timezone.utc).timestamp()

def backtest(sym, sl_pct, tp_mult, ts_from=0, ts_to=9e12, mom_window=15, mom_thresh=0.002, cooldown=120):
    bars=ALL_BARS[sym]
    price_win=deque()
    open_trade=None; trades=[]; last_close_ts=0
    for ts,price,buy_vol,sell_vol in bars:
        if ts<ts_from or ts>ts_to: continue
        price_win.append((ts,price))
        while price_win and ts-price_win[0][0]>mom_window:
            price_win.popleft()
        if open_trade:
            entry,sl,tp,d,ets=open_trade
            if d=='long':
                if price<=sl: trades.append(-sl_pct*SIZE_USDT-TAKER_FEE*2*SIZE_USDT); open_trade=None; last_close_ts=ts
                elif price>=tp: trades.append(sl_pct*tp_mult*SIZE_USDT-TAKER_FEE*2*SIZE_USDT); open_trade=None; last_close_ts=ts
                elif ts-ets>3600: trades.append(0); open_trade=None; last_close_ts=ts
            else:
                if price>=sl: trades.append(-sl_pct*SIZE_USDT-TAKER_FEE*2*SIZE_USDT); open_trade=None; last_close_ts=ts
                elif price<=tp: trades.append(sl_pct*tp_mult*SIZE_USDT-TAKER_FEE*2*SIZE_USDT); open_trade=None; last_close_ts=ts
                elif ts-ets>3600: trades.append(0); open_trade=None; last_close_ts=ts
            continue
        if ts-last_close_ts<cooldown or len(price_win)<2: continue
        p_start=price_win[0][1]; p_end=price_win[-1][1]
        move=(p_end-p_start)/p_start
        if abs(move)<mom_thresh: continue
        d='long' if move>0 else 'short'
        entry=price
        sl=entry*(1-sl_pct) if d=='long' else entry*(1+sl_pct)
        tp=entry*(1+sl_pct*tp_mult) if d=='long' else entry*(1-sl_pct*tp_mult)
        open_trade=(entry,sl,tp,d,ts)
    n=len(trades)
    if n==0: return 0,0,0
    wins=sum(1 for t in trades if t>0)
    return n,wins/n*100,sum(trades)

print("=== IS: 09.02 - 22.02 (первые 2 недели) ===")
total_is=0
for sym,sl,tp in CONFIGS:
    n,wr,pnl=backtest(sym,sl,tp,ts_to=SPLIT)
    total_is+=pnl
    print(f"  {sym}: N={n} WR={wr:.1f}% PnL={pnl:+.2f}")
print(f"  TOTAL IS: {total_is:+.2f}")

print("\n=== OOS: 22.02 - 10.03 (последние 2 недели) ===")
total_oos=0
for sym,sl,tp in CONFIGS:
    n,wr,pnl=backtest(sym,sl,tp,ts_from=SPLIT)
    total_oos+=pnl
    print(f"  {sym}: N={n} WR={wr:.1f}% PnL={pnl:+.2f}")
print(f"  TOTAL OOS: {total_oos:+.2f}")
