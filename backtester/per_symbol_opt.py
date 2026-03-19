import sqlite3
from collections import deque
from datetime import datetime, timezone

TAKER_FEE=0.00055
SIZE_USDT=50.0
DB='/home/vitaman/tick_bars_1s.db'
SYMBOLS=['BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','DOGEUSDT','AVAXUSDT']
SPLIT=datetime(2026,2,22,tzinfo=timezone.utc).timestamp()

conn=sqlite3.connect(DB)
ALL_BARS={sym:conn.execute('SELECT ts,price,buy_vol,sell_vol FROM bars1s WHERE symbol=? ORDER BY ts ASC',(sym,)).fetchall() for sym in SYMBOLS}
conn.close()

def backtest(sym, sl_pct, tp_mult, window, thresh, cooldown=120, ts_from=0, ts_to=9e12):
    bars=ALL_BARS[sym]; price_win=deque()
    open_trade=None; trades=[]; last_close_ts=0
    for ts,price,bv,sv in bars:
        if ts<ts_from or ts>ts_to: continue
        price_win.append((ts,price))
        while price_win and ts-price_win[0][0]>window: price_win.popleft()
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
        move=(price_win[-1][1]-price_win[0][1])/price_win[0][1]
        if abs(move)<thresh: continue
        d='long' if move>0 else 'short'
        entry=price
        sl=entry*(1-sl_pct) if d=='long' else entry*(1+sl_pct)
        tp=entry*(1+sl_pct*tp_mult) if d=='long' else entry*(1-sl_pct*tp_mult)
        open_trade=(entry,sl,tp,d,ts)
    n=len(trades)
    if n==0: return 0,0,0
    wins=sum(1 for t in trades if t>0)
    return n,wins/n*100,sum(trades)

print(f"{'символ':>10s} {'window':>7s} {'thresh':>7s} {'sl':>5s} {'tp':>5s} {'IS_PnL':>8s} {'OOS_PnL':>8s}")
print("-"*58)

best_params={}
for sym in SYMBOLS:
    best_oos=(-999,None)
    for window in [10,15,20,30]:
        for thresh in [0.0015,0.002,0.0025,0.003]:
            for sl in [0.010,0.012,0.015]:
                for tp in [1.0,1.2,1.5]:
                    _,_,is_pnl=backtest(sym,sl,tp,window,thresh,ts_to=SPLIT)
                    n,wr,oos_pnl=backtest(sym,sl,tp,window,thresh,ts_from=SPLIT)
                    if n>=10 and oos_pnl>best_oos[0]:
                        best_oos=(oos_pnl,(window,thresh,sl,tp,is_pnl))
    if best_oos[1]:
        w,t,sl,tp,is_p=best_oos[1]
        best_params[sym]=(w,t,sl,tp)
        print(f"{sym:>10s} {w:>7d} {t*100:>6.2f}% {sl*100:>4.1f}% {tp:>5.1f} {is_p:>+8.2f} {best_oos[0]:>+8.2f}")

print("\n=== СУММАРНО с per-symbol параметрами ===")
total_is=0; total_oos=0
for sym in SYMBOLS:
    if sym not in best_params: continue
    w,t,sl,tp=best_params[sym]
    _,_,ip=backtest(sym,sl,tp,w,t,ts_to=SPLIT)
    _,_,op=backtest(sym,sl,tp,w,t,ts_from=SPLIT)
    total_is+=ip; total_oos+=op
print(f"IS: {total_is:+.2f}  OOS: {total_oos:+.2f}")
