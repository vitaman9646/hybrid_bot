# Проверяем исправленную версию с 3-period walk-forward
import sqlite3
from collections import deque
from datetime import datetime, timezone

TAKER_FEE=0.00055; SIZE_USDT=50.0
DB='/home/vitaman/tick_bars_1s.db'
BEST={'BTCUSDT':(20,0.0015,0.012,1.2),'ETHUSDT':(20,0.002,0.010,1.2),
      'SOLUSDT':(30,0.0025,0.015,1.5),'XRPUSDT':(20,0.002,0.012,1.0),
      'DOGEUSDT':(30,0.002,0.015,1.0),'AVAXUSDT':(10,0.002,0.015,1.2)}

conn=sqlite3.connect(DB)
ALL_BARS={sym:conn.execute('SELECT ts,price,buy_vol,sell_vol FROM bars1s WHERE symbol=? ORDER BY ts ASC',(sym,)).fetchall() for sym in BEST}
conn.close()

P1=datetime(2026,2,19,tzinfo=timezone.utc).timestamp()
P2=datetime(2026,3,1,tzinfo=timezone.utc).timestamp()

def backtest(sym,ts_from=0,ts_to=9e12):
    window,thresh,sl_pct,tp_mult=BEST[sym]
    bars=ALL_BARS[sym]; pw=deque(); cw=deque()
    ot=None; trades=[]; lct=0; max_pnl=0
    pyramided=False; pyr_entry=None; pyr_size=0

    for ts,price,bv,sv in bars:
        if ts<ts_from or ts>ts_to: continue
        pw.append((ts,price)); cw.append((ts,price))
        while pw and ts-pw[0][0]>window: pw.popleft()
        while cw and ts-cw[0][0]>3: cw.popleft()

        if ot:
            entry,sl,tp,d,ets,size,strength=ot
            cur_pnl=(price-entry)/entry if d=='long' else (entry-price)/entry
            max_pnl=max(max_pnl,cur_pnl)

            if not pyramided and cur_pnl>0.008:
                pyramided=True; pyr_entry=price; pyr_size=size*0.5

            # Честный PnL
            base_pnl=cur_pnl*size
            if pyramided and pyr_entry:
                pyr_pnl=(price-pyr_entry)/pyr_entry if d=='long' else (pyr_entry-price)/pyr_entry
                total=base_pnl+pyr_pnl*pyr_size-TAKER_FEE*2*(size+pyr_size)
            else:
                total=base_pnl-TAKER_FEE*2*size

            trail_exit=max_pnl>sl_pct and cur_pnl<max_pnl*0.6
            closed=False; cpnl=None
            if d=='long':
                if price<=sl: cpnl=-sl_pct*size-TAKER_FEE*2*size; closed=True
                elif trail_exit or price>=tp: cpnl=total; closed=True
                elif ts-ets>3600: cpnl=total; closed=True
            else:
                if price>=sl: cpnl=-sl_pct*size-TAKER_FEE*2*size; closed=True
                elif trail_exit or price<=tp: cpnl=total; closed=True
                elif ts-ets>3600: cpnl=total; closed=True
            if closed:
                trades.append(cpnl); ot=None; lct=ts
                pyramided=False; pyr_entry=None; pyr_size=0; max_pnl=0
            continue

        if ts-lct<120 or len(pw)<2: continue
        move=(pw[-1][1]-pw[0][1])/pw[0][1]
        if abs(move)<thresh: continue
        if len(cw)>=2:
            m3=(cw[-1][1]-cw[0][1])/cw[0][1]
            if move>0 and m3<0.0002: continue
            if move<0 and m3>-0.0002: continue
        d='long' if move>0 else 'short'
        strength=min(abs(move)/thresh,2.0)
        size=SIZE_USDT*2.0 if strength<1.3 else SIZE_USDT*0.5
        entry=price
        sl=entry*(1-sl_pct) if d=='long' else entry*(1+sl_pct)
        tp=entry*(1+sl_pct*tp_mult) if d=='long' else entry*(1-sl_pct*tp_mult)
        ot=(entry,sl,tp,d,ts,size,strength); lct=ts; max_pnl=0

    equity=0; peak=0; dd=0
    for t in trades:
        equity+=t; peak=max(peak,equity); dd=max(dd,peak-equity)
    n=len(trades); wins=sum(1 for t in trades if t>0)
    return n,wins/n*100 if n else 0,sum(trades),dd

print("=== 3-period walk-forward (честный код) ===")
print(f"{'период':>20s} {'N':>6s} {'WR':>6s} {'PnL':>8s} {'MaxDD':>8s}")
print("-"*52)
for label,t1,t2 in [('IS: 09.02-22.02',0,P1),('OOS1: 22.02-01.03',P1,P2),('OOS2: 01.03-10.03',P2,9e12)]:
    tn=0;tw=0;tp=0;td=0
    for sym in BEST:
        n,wr,pnl,dd=backtest(sym,t1,t2)
        tn+=n;tw+=int(n*wr/100);tp+=pnl;td+=dd
    wr=tw/tn*100 if tn else 0
    print(f"{label:>20s} {tn:>6d} {wr:>6.1f}% {tp:>+8.2f} {td:>8.2f}")
