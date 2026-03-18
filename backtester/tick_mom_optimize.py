import sqlite3
from collections import deque

TAKER_FEE=0.00055
SIZE_USDT=50.0
DB='/home/vitaman/tick_bars_1s.db'
CONFIGS=[('ETHUSDT',0.010,1.2),('SOLUSDT',0.012,1.8),('DOGEUSDT',0.010,1.2),('AVAXUSDT',0.012,1.2)]

conn=sqlite3.connect(DB)
ALL_BARS={sym:conn.execute('SELECT ts,price,buy_vol,sell_vol FROM bars1s WHERE symbol=? ORDER BY ts ASC',(sym,)).fetchall() for sym,_,_ in CONFIGS}
conn.close()

def backtest(sym, sl_pct, tp_mult, mom_window=15, mom_thresh=0.002, cooldown=120):
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

# Grid: sl% и tp_mult при window=15, thresh=0.2%
print("=== SL/TP grid (window=15s, thresh=0.2%) ===")
print(f"{'sl%':>6s} {'tp_x':>6s} {'N':>6s} {'WR':>6s} {'PnL':>8s}")
print("-"*36)
best=(0,None)
for sl in [0.008,0.010,0.012,0.015]:
    for tp in [1.0,1.2,1.5,2.0,2.5]:
        total_pnl=0; total_n=0; total_wins=0
        for sym,_,_ in CONFIGS:
            n,wr,pnl=backtest(sym,sl,tp,mom_window=15,mom_thresh=0.002)
            total_pnl+=pnl; total_n+=n; total_wins+=int(n*wr/100)
        wr=total_wins/total_n*100 if total_n else 0
        marker=" ←" if total_pnl>best[0] else ""
        if total_pnl>best[0]: best=(total_pnl,(sl,tp))
        print(f"{sl*100:>5.1f}% {tp:>6.1f} {total_n:>6d} {wr:>6.1f}% {total_pnl:+8.2f}{marker}")
    print()

# По символам с лучшими параметрами
sl,tp=best[1]
print(f"\n=== По символам (sl={sl*100:.1f}% tp_x={tp}) ===")
for sym,_,_ in CONFIGS:
    n,wr,pnl=backtest(sym,sl,tp,mom_window=15,mom_thresh=0.002)
    print(f"  {sym}: N={n} WR={wr:.1f}% PnL={pnl:+.2f}")
