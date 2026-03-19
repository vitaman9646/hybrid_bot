import sqlite3
from collections import deque
from datetime import datetime, timezone

TAKER_FEE=0.00055
SIZE_USDT=50.0
DB='/home/vitaman/tick_bars_1s.db'
SPLIT=datetime(2026,2,22,tzinfo=timezone.utc).timestamp()

BEST={
    'BTCUSDT': (20,0.0015,0.012,1.2),
    'ETHUSDT': (20,0.002, 0.010,1.2),
    'SOLUSDT': (30,0.0025,0.015,1.5),
    'XRPUSDT': (20,0.002, 0.012,1.0),
    'DOGEUSDT':(30,0.002, 0.015,1.0),
    'AVAXUSDT':(10,0.002, 0.015,1.2),
}

conn=sqlite3.connect(DB)
ALL_BARS={sym:conn.execute('SELECT ts,price,buy_vol,sell_vol FROM bars1s WHERE symbol=? ORDER BY ts ASC',(sym,)).fetchall() for sym in BEST}
conn.close()

def backtest(sym, burst_thresh=None, ema_alpha=0.05, ts_from=0, ts_to=9e12):
    window,thresh,sl_pct,tp_mult=BEST[sym]
    bars=ALL_BARS[sym]
    price_win=deque(); cont_win=deque()
    ema_vol=None; open_trade=None; trades=[]; last_close_ts=0

    for ts,price,bv,sv in bars:
        if ts<ts_from or ts>ts_to: continue
        tot_vol=bv+sv
        # EMA объёма
        if ema_vol is None: ema_vol=tot_vol
        else: ema_vol=ema_alpha*tot_vol+(1-ema_alpha)*ema_vol

        price_win.append((ts,price))
        cont_win.append((ts,price))
        while price_win and ts-price_win[0][0]>window: price_win.popleft()
        while cont_win and ts-cont_win[0][0]>3: cont_win.popleft()

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

        if ts-last_close_ts<120 or len(price_win)<2: continue
        move=(price_win[-1][1]-price_win[0][1])/price_win[0][1]
        if abs(move)<thresh: continue

        # Continuation 0.02%
        if len(cont_win)>=2:
            move_3s=(cont_win[-1][1]-cont_win[0][1])/cont_win[0][1]
            if move>0 and move_3s<0.0002: continue
            if move<0 and move_3s>-0.0002: continue

        # Burst filter
        if burst_thresh and ema_vol>0:
            burst=tot_vol/ema_vol
            if burst<burst_thresh: continue

        d='long' if move>0 else 'short'
        entry=price
        sl=entry*(1-sl_pct) if d=='long' else entry*(1+sl_pct)
        tp=entry*(1+sl_pct*tp_mult) if d=='long' else entry*(1-sl_pct*tp_mult)
        open_trade=(entry,sl,tp,d,ts)

    n=len(trades)
    if n==0: return 0,0,0
    wins=sum(1 for t in trades if t>0)
    return n,wins/n*100,sum(trades)

print(f"{'burst':>8s} {'N':>6s} {'WR':>6s} {'IS':>8s} {'OOS':>8s}")
print("-"*40)
for bt in [None,1.2,1.5,1.8,2.0,2.5,3.0]:
    ti=0; to=0; tn=0; tw=0
    for sym in BEST:
        _,_,ip=backtest(sym,bt,ts_to=SPLIT)
        n,wr,op=backtest(sym,bt,ts_from=SPLIT)
        ti+=ip; to+=op; tn+=n; tw+=int(n*wr/100)
    wr=tw/tn*100 if tn else 0
    label=str(bt) if bt else 'нет'
    print(f"{label:>8s} {tn:>6d} {wr:>6.1f}% {ti:>+8.2f} {to:>+8.2f}")
