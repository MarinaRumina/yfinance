import asyncio
import logging
import os
from typing import Optional, Any, List, Dict
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="YFinance Ultimate API", version="2.1.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- ГЛОБАЛЬНЫЙ КЭШ ДЛЯ СТАТИЧНЫХ ДАННЫХ ДНЯ ---
# Структура: {"AAPL": {"open": 150.0, "high": 155.0, ... "cache_date": "2025-12-29"}}
BASE_DATA_CACHE = {}

# --- СЛОВАРЬ БИРЖ ---
EXCHANGE_MAP = {
    "NMS": "NASDAQ", "NYQ": "NYSE", "NCM": "NASDAQ Capital Market",
    "NGM": "NASDAQ Global Market", "PCX": "NYSE Arca", "TOR": "Toronto Stock Exchange",
    "TAE": "Tel Aviv Stock Exchange", "LSE": "London Stock Exchange",
    "FRA": "Frankfurt Stock Exchange", "GER": "XETRA (Germany)",
    "PAR": "Euronext Paris", "AMS": "Euronext Amsterdam",
    "MIL": "Borsa Italiana", "EBS": "SIX Swiss Exchange",
    "MCW": "BME Spanish Exchanges", "MCX": "Moscow Exchange"
}

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def normalize_value(v: Any) -> Any:
    if isinstance(v, (pd.Timestamp, datetime)): return v.isoformat()
    if isinstance(v, (np.integer, int)): return int(v)
    if isinstance(v, (np.floating, float)):
        return None if np.isnan(v) or np.isinf(v) else float(v)
    if isinstance(v, pd.DataFrame): return v.reset_index().to_dict(orient="records")
    if isinstance(v, pd.Series): return v.to_dict()
    if isinstance(v, (dict, list)):
        if isinstance(v, dict): return {str(k): normalize_value(val) for k, val in v.items()}
        return [normalize_value(i) for i in v]
    return v

def get_ticker_base_data(symbol: str, t_obj):
    """Обновляет или создает базовую запись в кэше (раз в сутки)."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if symbol in BASE_DATA_CACHE and BASE_DATA_CACHE[symbol]['cache_date'] == today_str:
        return BASE_DATA_CACHE[symbol]

    try:
        # Берем 5 дней истории, чтобы гарантированно найти торговый день (даже после выходных)
        h = t_obj.history(period="5d")
        if h.empty: return None

        last_row = h.iloc[-1]
        prev_row = h.iloc[-2] if len(h) > 1 else last_row
        
        base_info = {
            "open": last_row['Open'],
            "prev_close": prev_row['Close'],
            "high": last_row['High'],
            "low": last_row['Low'],
            "volume": last_row['Volume'],
            "data_date": h.index[-1].strftime('%Y-%m-%d'),
            "cache_date": today_str
        }
        BASE_DATA_CACHE[symbol] = base_info
        return base_info
    except Exception as e:
        logger.error(f"Error caching data for {symbol}: {e}")
        return None

def get_combined_quote(symbol: str):
    """
    Основная логика сбора данных. 
    Объединяет быстрые данные с кэшем и обновляет High/Low/Volume в реальном времени.
    """
    t = yf.Ticker(symbol)
    f = t.fast_info
    base = get_ticker_base_data(symbol, t)
    
    if not base: return None

    curr = f.get('last_price') or base['prev_close']
    
    # --- ДИНАМИЧЕСКОЕ ОБНОВЛЕНИЕ КЭША ---
    # Обновляем максимумы/минимумы дня, если текущие данные выше/ниже кэшированных
    live_high = f.get('day_high')
    if live_high and (np.isnan(base['high']) or live_high > base['high']):
        base['high'] = live_high

    live_low = f.get('day_low')
    if live_low and (np.isnan(base['low']) or live_low < base['low']):
        base['low'] = live_low

    live_vol = f.get('last_volume')
    if live_vol and (np.isnan(base['volume']) or live_vol > base['volume']):
        base['volume'] = live_vol
    # -----------------------------------

    # Расчет процента изменения
    # Если есть цена открытия — считаем от нее, иначе от вчерашнего закрытия
    op = base['open']
    base_for_pct = op if op and not np.isnan(op) else base['prev_close']
    pct = ((curr - base_for_pct) / base_for_pct * 100) if curr and base_for_pct else 0

    return {
        "symbol": symbol,
        "current_price": curr,
        "change_percent": round(pct, 2),
        "open": base['open'],
        "high": base['high'],
        "low": base['low'],
        "volume": base['volume'],
        "previous_close": base['prev_close'],
        "date": base['data_date'],
        "currency": f.get('currency'),
        "exchange": EXCHANGE_MAP.get(f.get('exchange'), f.get('exchange'))
    }

# --- УТИЛИТЫ ---

@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название или тикер")):
    """Поиск с приоритетом тикеров, начинающихся на запрос"""
    try:
        q = query.strip().upper()
        s = yf.Search(q, max_results=15)
        quotes = s.quotes
        if not quotes: return {"results": []}
        sorted_quotes = sorted(
            quotes, 
            key=lambda x: (not x.get('symbol', '').startswith(q), -x.get('score', 0))
        )
        return {"results": normalize_value(sorted_quotes)}
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")


# --- РЫНОЧНЫЕ ДАННЫЕ ---

@app.get("/tickers/quote", tags=["Market Data"])
def get_multiple_quotes(symbols: str = Query(...)):
    """Получение котировок для списка тикеров с использованием кэша."""
    try:
        ticker_list = [s.strip().upper() for s in symbols.split(",")]
        result = {}
        for symbol in ticker_list:
            data = get_combined_quote(symbol)
            if data:
                result[symbol] = normalize_value(data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/price/{tickers}")
async def websocket_price(websocket: WebSocket, tickers: str):
    """
    Асинхронный WebSocket для нескольких тикеров (через запятую).
    Использует кэширование и обновляет High/Low в реальном времени.
    """
    await websocket.accept()
    ticker_list = [s.strip().upper() for s in tickers.split(",")]
    try:
        while True:
            updates = {}
            for sym in ticker_list:
                data = get_combined_quote(sym)
                if data:
                    updates[sym] = {
                        "price": normalize_value(data['current_price']),
                        "change_percent": normalize_value(data['change_percent']),
                        "open": normalize_value(data['open']),
                        "high": normalize_value(data['high']),
                        "low": normalize_value(data['low']),
                        "volume": normalize_value(data['volume']),
                        "date": data['date'],
                        "time": datetime.now().isoformat()
                    }
            await websocket.send_json(updates)
            await asyncio.sleep(2) 
    except WebSocketDisconnect:
        logger.info(f"Client disconnected: {tickers}")
    except Exception as e:
        logger.error(f"WS error: {e}")
        await websocket.close()


# --- ФИНАНСЫ И ОТЧЕТНОСТЬ ---
@app.get("/financials/{ticker}", tags=["Financials"])
def get_financials(ticker: str):
    """Баланс, Прибыли и Кэшфлоу."""
    try:
        t = yf.Ticker(ticker.upper())
        return normalize_value({
            "income_statement": t.income_stmt,
            "balance_sheet": t.balance_sheet,
            "cashflow": t.cashflow
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/info/{ticker}", tags=["Information"])
def get_info(ticker: str):
    """Полная информация о компании."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.info)

@app.get("/history/{ticker}", tags=["Market Data"])
def get_history(ticker: str, period: str = "1mo", interval: str = "1d"):
    """Исторические данные OHLC + Volume + Dividends + Splits."""
    t = yf.Ticker(ticker.upper())
    hist = t.history(period=period, interval=interval)
    return normalize_value(hist)

# --- КОРПОРАТИВНЫЕ СОБЫТИЯ ---
@app.get("/dividends/{ticker}", tags=["Corporate Actions"])
def get_dividends(ticker: str):
    """История дивидендов."""
    t = yf.Ticker(ticker.upper())
    divs = t.dividends
    if divs.empty:
        return {"symbol": ticker.upper(), "message": "No dividends found", "data": []}
    return normalize_value(divs)

@app.get("/splits/{ticker}", tags=["Corporate Actions"])
def get_splits(ticker: str):
    """История сплитов."""
    t = yf.Ticker(ticker.upper())
    splits = t.splits
    if splits.empty:
        return {"symbol": ticker.upper(), "message": "No splits found", "data": []}
    return normalize_value(splits)

@app.get("/actions/{ticker}", tags=["Corporate Actions"])
def get_actions(ticker: str):
    """Все действия (дивиденды + сплиты)."""
    t = yf.Ticker(ticker.upper())
    if t.actions.empty:
        return {"symbol": ticker.upper(), "message": "No actions found", "data": []}
    return normalize_value(t.actions)

@app.get("/holders/{ticker}", tags=["Information"])
def get_holders(ticker: str):
    """Крупнейшие держатели акций."""
    t = yf.Ticker(ticker.upper())
    return normalize_value({
        "major": t.major_holders,
        "institutional": t.institutional_holders
    })

# --- ДОПОЛНИТЕЛЬНЫЕ ЭНДПОИНТЫ ---

@app.get("/news/{ticker}", tags=["Information"])
def get_news(ticker: str):
    """Последние новости по тикеру."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.news)

@app.get("/recommendations/{ticker}", tags=["Information"])
def get_recommendations(ticker: str):
    """Рекомендации аналитиков."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.recommendations)

@app.get("/calendar/{ticker}", tags=["Information"])
def get_calendar(ticker: str):
    """Календарь событий (отчеты, дивиденды)."""
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.calendar)

@app.get("/health", tags=["Utility"])
def health():
    """Проверка состояния API и размера кэша."""
    return {
        "status": "online", 
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(BASE_DATA_CACHE)
    }

if __name__ == "__main__":
    import uvicorn
    # Render использует переменную окружения PORT
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
