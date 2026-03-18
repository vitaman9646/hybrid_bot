import sqlite3
from datetime import datetime, timezone
from collections import deque

TAKER_FEE=0.00055
SIZE_USDT=50.0
DB='/home/vitaman/tick_bars_1s.db'

CONFIGS=[
    ('ETHUSDT', 0.010, 1.2),
    ('SOLUSDT', 0.012, 1.8),
    ('DOGEUSDT',0.010, 1.2),
    ('AVAXUSDT',0.012, 1.2),
]

print("Загружаем 1s бары...")
ALL_BARS={}
conn=sqlite3.connect(DB)
for sym,sl,tp in CONFIGS:
    rows=conn.execute('SELECT ts,price,buy_vol,sell_vol FROM bars1s WHERE symbol=? ORDER BY ts ASC',(sym,)).fetchall()
    ALL_BARS[sym]=rows
    print(f"  {sym}: {len(rows):,} баров")
conn.close()
print("Готово!\n")

def backtest(sym, sl_pct, tp_mult, cvd_window=60, cvd_thresh=0.6, cooldown=120):
    bars=ALL_BARS[sym]
    window=deque()  # (ts, cvd_delta)
    cvd=0.0; abs_vol=0.0
    open_trade=None; trades=[]; last_close_ts=0

    for ts,price,buy_vol,sell_vol in bars:
        delta=buy_vol-sell_vol
        tot=buy_vol+sell_vol

        # Обновляем скользящее CVD окно
        window.append((ts,delta,tot))
        cvd+=delta; abs_vol+=tot
        while window and ts-window[0][0]>cvd_window:
            old=window.popleft()
            cvd-=old[1]; abs_vol-=old[2]

        # Проверяем открытую сделку по цене
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

        if ts-last_close_ts<cooldown: continue
        if abs_vol<0.1: continue
        directional=abs(cvd)/abs_vol
        if directional<cvd_thresh: continue

        d='long' if cvd>0 else 'short'
        entry=price
        sl=entry*(1-sl_pct) if d=='long' else entry*(1+sl_pct)
        tp=entry*(1+sl_pct*tp_mult) if d=='long' else entry*(1-sl_pct*tp_mult)
        open_trade=(entry,sl,tp,d,ts)

    n=len(trades)
    if n==0: return 0,0,0
    wins=sum(1 for t in trades if t>0)
    return n,wins/n*100,sum(trades)

print(f"{'thresh':>8s} {'window':>8s} {'cooldown':>10s} {'N':>6s} {'WR':>6s} {'PnL':>8s}")
print("-"*52)
for thresh in [0.5,0.6,0.7,0.8]:
    for window in [30,60,120]:
        for cooldown in [60,120,300]:
            total_pnl=0; total_n=0; total_wins=0
            for sym,sl,tp in CONFIGS:
                n,wr,pnl=backtest(sym,sl,tp,cvd_window=window,cvd_thresh=thresh,cooldown=cooldown)
                total_pnl+=pnl; total_n+=n; total_wins+=int(n*wr/100)
            wr=total_wins/total_n*100 if total_n else 0
            print(f"{thresh:>8.1f} {window:>8d} {cooldown:>10d} {total_n:>6d} {wr:>6.1f}% {total_pnl:+8.2f}")
    print()
