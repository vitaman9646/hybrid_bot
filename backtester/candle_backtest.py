"""
candle_backtest.py — свечной бэктест Hybrid Trend Impulse v4
Запуск:
    python3 backtester/candle_backtest.py --all --data-dir /home/vitaman/data
"""
import sqlite3, argparse, os
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone
from collections import deque

TAKER_FEE = 0.00055
SLIPPAGE = {'BTCUSDT':0.0001,'ETHUSDT':0.00015,'SOLUSDT':0.0002,'BNBUSDT':0.0002,'XRPUSDT':0.00025,'DOGEUSDT':0.0003,'ADAUSDT':0.0003,'AVAXUSDT':0.0003}
THRESHOLDS = {'all_three':0.48,'averages_vector':0.62,'averages_depth':0.68}
SL_PCT = 0.012
SIZE_USDT = 50.0
RISK_PCT = 0.0075
PHASE1 = ['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT']
DB_MAP = {'BTCUSDT':'klines_btc.db','ETHUSDT':'klines_eth.db','SOLUSDT':'klines_sol_bnb.db','BNBUSDT':'klines_sol_bnb.db','XRPUSDT':'klines_rest.db','DOGEUSDT':'klines_rest.db','ADAUSDT':'klines_rest.db','AVAXUSDT':'klines_rest.db'}

@dataclass
class Candle:
    symbol:str; ts:float; open:float; high:float; low:float; close:float; volume:float; turnover:float

@dataclass
class Trade:
    symbol:str; scenario:str; direction:str; entry_price:float; sl_price:float; tp1_price:float; tp2_price:float; entry_ts:float
    exit_ts:float=0.0; exit_price:float=0.0; exit_reason:str=''; pnl_usdt:float=0.0; pnl_pct:float=0.0; session:str=''; mfe_pct:float=0.0; mae_pct:float=0.0

def get_session(ts):
    h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
    if 2<=h<6: return 'DEAD'
    elif 6<=h<12: return 'ASIA'
    elif 12<=h<16: return 'LONDON'
    elif 16<=h<21: return 'NY'
    else: return 'QUIET'

def ema(prices, period):
    if len(prices)<period: return prices[-1] if prices else 0.0
    k=2/(period+1); e=prices[0]
    for p in prices[1:]: e=p*k+e*(1-k)
    return e

def calc_pnl(direction, entry, exit_p, qty, symbol):
    slip=SLIPPAGE.get(symbol,0.0003)
    if direction=='long':
        ae=entry*(1+slip); ax=exit_p*(1-slip); gross=(ax-ae)*qty
    else:
        ae=entry*(1-slip); ax=exit_p*(1+slip); gross=(ae-ax)*qty
    return gross-(ae+ax)*qty*TAKER_FEE

def load_candles(db_path, symbol):
    conn=sqlite3.connect(db_path)
    rows=conn.execute("SELECT symbol,ts,open,high,low,close,volume,turnover FROM klines WHERE symbol=? AND interval='1' ORDER BY ts ASC",(symbol,)).fetchall()
    conn.close()
    return [Candle(*r) for r in rows]

class AveragesAnalyzer:
    def __init__(self):
        self._closes=deque(maxlen=310)
    def update(self,c): self._closes.append(c.close)
    def get_trend(self):
        cl=list(self._closes)
        if len(cl)<300: return 'FLAT'
        ms=sum(cl[-60:])/60; ml=sum(cl[-300:])/300
        d=(ms-ml)/ml*100
        if d>0.05: return 'UP'
        elif d<-0.05: return 'DOWN'
        return 'FLAT'
    def get_score(self):
        cl=list(self._closes)
        if len(cl)<300: return 0.0
        ms=sum(cl[-60:])/60; ml=sum(cl[-300:])/300
        return min(abs((ms-ml)/ml*100)/0.2,1.0)

class MTFAnalyzer:
    def __init__(self):
        self._c15=deque(maxlen=30); self._c1h=deque(maxlen=30)
        self._b15=[]; self._b1h=[]
    def update(self,c):
        self._b15.append(c.close)
        if len(self._b15)==15: self._c15.append(self._b15[-1]); self._b15=[]
        self._b1h.append(c.close)
        if len(self._b1h)==60: self._c1h.append(self._b1h[-1]); self._b1h=[]
    def get_bias(self):
        c15=list(self._c15); c1h=list(self._c1h)
        if len(c15)<20 or len(c1h)<5: return 'NEUTRAL',0.0
        e15=ema(c15,min(20,len(c15))); e1h=ema(c1h,min(20,len(c1h)))
        b15=c15[-1]>e15; b1h=c1h[-1]>e1h
        if b15 and b1h: return 'LONG',1.0
        elif not b15 and not b1h: return 'SHORT',1.0
        elif b1h: return 'LONG',0.6
        elif b15: return 'SHORT',0.6
        return 'NEUTRAL',0.0
    def is_blocked(self,direction,scenario):
        bias,strength=self.get_bias()
        if bias=='NEUTRAL' or strength<0.6: return False
        mr='depth' in scenario
        if mr:
            if bias=='LONG' and direction=='long': return True
            if bias=='SHORT' and direction=='short': return True
        else:
            if bias=='LONG' and direction=='short': return True
            if bias=='SHORT' and direction=='long': return True
        return False

class ATRCalc:
    def __init__(self,period=14):
        self._trs=deque(maxlen=period); self._prev=None
    def update(self,c):
        if self._prev:
            self._trs.append(max(c.high-c.low,abs(c.high-self._prev),abs(c.low-self._prev)))
        self._prev=c.close
    def get_atr(self): return sum(self._trs)/len(self._trs) if self._trs else 0.0

class CandleBacktest:
    def __init__(self,symbol,candles,equity=500.0):
        self.symbol=symbol; self.candles=candles; self.equity=equity; self.initial_equity=equity
        self.avg=AveragesAnalyzer(); self.mtf=MTFAnalyzer(); self.atr=ATRCalc()
        self.trades=[]; self.open_trade=None; self._last_ts=0.0

    def run(self):
        for c in self.candles:
            self.avg.update(c); self.mtf.update(c); self.atr.update(c)
            if self.open_trade: self._check_exit(c)
            if not self.open_trade: self._check_entry(c)
        if self.open_trade:
            last=self.candles[-1]; self._close(last.close,last.ts,'end_of_data')
        return self.trades

    def _check_exit(self,c):
        t=self.open_trade
        if t.direction=='long':
            if c.low<=t.sl_price: self._close(t.sl_price,c.ts,'sl'); return
            if c.high>=t.tp2_price: self._close(t.tp2_price,c.ts,'tp2'); return
            if c.high>=t.tp1_price: self._close(t.tp1_price,c.ts,'tp1'); return
            fav=(c.high-t.entry_price)/t.entry_price*100; adv=(t.entry_price-c.low)/t.entry_price*100
        else:
            if c.high>=t.sl_price: self._close(t.sl_price,c.ts,'sl'); return
            if c.low<=t.tp2_price: self._close(t.tp2_price,c.ts,'tp2'); return
            if c.low<=t.tp1_price: self._close(t.tp1_price,c.ts,'tp1'); return
            fav=(t.entry_price-c.low)/t.entry_price*100; adv=(c.high-t.entry_price)/t.entry_price*100
        t.mfe_pct=max(t.mfe_pct,fav); t.mae_pct=max(t.mae_pct,adv)
        limit={'averages_depth':1200,'averages_vector':1200}.get(t.scenario,1800)
        if c.ts-t.entry_ts>limit: self._close(c.close,c.ts,'time_stop')

    def _check_entry(self,c):
        s=get_session(c.ts)
        if s=='DEAD' or c.ts-self._last_ts<60: return
        trend=self.avg.get_trend(); score=self.avg.get_score(); atr=self.atr.get_atr()
        if trend in('UP','DOWN'):
            d='long' if trend=='UP' else 'short'
            if score*0.8>=THRESHOLDS['all_three'] and not self.mtf.is_blocked(d,'all_three'):
                self._open(c,d,'all_three',atr); return
            if s in('LONDON','NY') and score*0.7>=THRESHOLDS['averages_vector'] and not self.mtf.is_blocked(d,'averages_vector'):
                self._open(c,d,'averages_vector',atr); return
            mr='short' if trend=='UP' else 'long'
            if s in('ASIA','LONDON','NY') and score*0.6>=THRESHOLDS['averages_depth'] and not self.mtf.is_blocked(mr,'averages_depth'):
                self._open(c,mr,'averages_depth',atr); return

    def _open(self,c,direction,scenario,atr):
        p=c.close; sl_pct=max(SL_PCT,atr*2.5/p if atr>0 else SL_PCT)
        tp1_pct=max(sl_pct*1.5,atr*1.5/p if atr>0 else sl_pct*1.5)
        tp2_pct=max(sl_pct*2.5,atr*2.5/p if atr>0 else sl_pct*2.5)
        sl=p*(1-sl_pct) if direction=='long' else p*(1+sl_pct)
        tp1=p*(1+tp1_pct) if direction=='long' else p*(1-tp1_pct)
        tp2=p*(1+tp2_pct) if direction=='long' else p*(1-tp2_pct)
        self.open_trade=Trade(self.symbol,scenario,direction,p,sl,tp1,tp2,c.ts,session=get_session(c.ts))
        self._last_ts=c.ts

    def _close(self,exit_p,exit_ts,reason):
        t=self.open_trade; qty=SIZE_USDT/t.entry_price
        pnl=calc_pnl(t.direction,t.entry_price,exit_p,qty,self.symbol)
        t.exit_price=exit_p; t.exit_ts=exit_ts; t.exit_reason=reason
        t.pnl_usdt=pnl; t.pnl_pct=pnl/SIZE_USDT*100; self.equity+=pnl
        self.trades.append(t); self.open_trade=None
        if reason=='sl': self._last_ts=exit_ts+120
        elif reason in('tp1','tp2'): self._last_ts=exit_ts+45

def analyze(trades,symbol,initial_equity):
    if not trades: print(f"\n{symbol}: No trades"); return
    total=len(trades); winners=[t for t in trades if t.pnl_usdt>0]; losers=[t for t in trades if t.pnl_usdt<=0]
    wr=len(winners)/total*100; total_pnl=sum(t.pnl_usdt for t in trades)
    gw=sum(t.pnl_usdt for t in winners) if winners else 0
    gl=abs(sum(t.pnl_usdt for t in losers)) if losers else 0.001
    pf=gw/gl
    equity=initial_equity; peak=equity; max_dd=0.0
    for t in trades:
        equity+=t.pnl_usdt; peak=max(peak,equity)
        max_dd=max(max_dd,(peak-equity)/peak*100)
    avg_mfe=sum(t.mfe_pct for t in trades)/total
    avg_mae=sum(t.mae_pct for t in trades)/total
    print(f"\n{'='*55}\n  {symbol} — {total} trades\n{'='*55}")
    print(f"  WR:           {wr:.1f}%")
    print(f"  Profit Factor:{pf:.2f}")
    print(f"  Total PnL:    ${total_pnl:+.2f}")
    print(f"  Max Drawdown: {max_dd:.1f}%")
    print(f"  Avg Win: ${gw/len(winners):.2f}  Avg Loss: ${gl/len(losers):.2f}" if winners and losers else "")
    print(f"  MFE avg: {avg_mfe:.2f}%  MAE avg: {avg_mae:.2f}%")
    print(f"\n  By scenario:")
    for sc in ['all_three','averages_vector','averages_depth']:
        st=[t for t in trades if t.scenario==sc]
        if st:
            sw=len([t for t in st if t.pnl_usdt>0])/len(st)*100
            print(f"    {sc:22s}: {len(st):3d} trades  WR={sw:.0f}%  PnL=${sum(t.pnl_usdt for t in st):+.2f}")
    print(f"\n  By session:")
    for ss in ['ASIA','LONDON','NY','QUIET']:
        st=[t for t in trades if t.session==ss]
        if st:
            sw=len([t for t in st if t.pnl_usdt>0])/len(st)*100
            print(f"    {ss:8s}: {len(st):3d} trades  WR={sw:.0f}%  PnL=${sum(t.pnl_usdt for t in st):+.2f}")
    print(f"\n  Exit reasons:")
    for r in ['sl','tp1','tp2','time_stop','end_of_data']:
        rt=[t for t in trades if t.exit_reason==r]
        if rt: print(f"    {r:12s}: {len(rt):3d} ({len(rt)/total*100:.0f}%)")

def main():
    parser=argparse.ArgumentParser(description='Candle Backtest HTI v4')
    parser.add_argument('--db'); parser.add_argument('--symbol')
    parser.add_argument('--all',action='store_true')
    parser.add_argument('--data-dir',default='/home/vitaman/data')
    parser.add_argument('--equity',type=float,default=500.0)
    args=parser.parse_args()
    if args.all:
        all_trades=[]
        for symbol in PHASE1:
            db=os.path.join(args.data_dir,DB_MAP[symbol])
            if not os.path.exists(db): print(f"DB not found: {db}"); continue
            candles=load_candles(db,symbol)
            if not candles: print(f"No candles for {symbol}"); continue
            print(f"Loaded {len(candles)} candles for {symbol}")
            bt=CandleBacktest(symbol,candles,equity=args.equity)
            trades=bt.run(); analyze(trades,symbol,args.equity); all_trades.extend(trades)
        if all_trades:
            total=len(all_trades); wr=len([t for t in all_trades if t.pnl_usdt>0])/total*100
            pnl=sum(t.pnl_usdt for t in all_trades)
            gw=sum(t.pnl_usdt for t in all_trades if t.pnl_usdt>0)
            gl=abs(sum(t.pnl_usdt for t in all_trades if t.pnl_usdt<=0))
            print(f"\n{'='*55}\n  TOTAL — {total} trades\n{'='*55}")
            print(f"  WR: {wr:.1f}%  PF: {gw/max(gl,0.001):.2f}  PnL: ${pnl:+.2f}")
    elif args.db and args.symbol:
        candles=load_candles(args.db,args.symbol)
        print(f"Loaded {len(candles)} candles for {args.symbol}")
        bt=CandleBacktest(args.symbol,candles,equity=args.equity)
        trades=bt.run(); analyze(trades,args.symbol,args.equity)
    else: parser.print_help()

if __name__=='__main__': main()
