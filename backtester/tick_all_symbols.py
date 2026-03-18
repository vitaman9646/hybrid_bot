import sqlite3
from collections import deque

TAKER_FEE=0.00055
SIZE_USDT=50.0
DB='/home/vitaman/tick_bars_1s.db'
SYMBOLS=['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT','DOGEUSDT','AVAXUSDT','ADAUSDT']

conn=sqlite3.connect(DB)
ALL_BARS={sym:conn.execute('SELECT ts,price,buy_vol,sell_vol FROM bars1s WHERE symbol=? ORDER BY ts ASC',(sym,)).fetchall() for sym in SYMBOLS}
conn.close()

def backtest(sym, sl_pct=0.015, tp_mult=1.0, mom_window=15, mom_thresh=0.002, cooldown=120):
    bars=ALL_BARS[sym]
    price_win=deque()
    open_trade=None; trades=[]; last_close_ts=0
    for ts,price,buy_vol,sell_vol in bars:
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

print(f"\n{'символ':>10s} {'N':>6s} {'WR':>6s} {'PnL':>8s}")
print("-"*34)
total=0
for sym in SYMBOLS:
    n,wr,pnl=backtest(sym)
    total+=pnl
    print(f"{sym:>10s} {n:>6d} {wr:>6.1f}% {pnl:+8.2f}")
print(f"{'TOTAL':>10s} {'':>6s} {'':>6s} {total:+8.2f}")
