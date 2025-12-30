import asyncio
import logging
import os
from typing import Optional, Any, List, Dict
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query, Path, WebSocket, WebSocketDisconnect
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
    """Обновляет кэш (open, prev_close, avg_vol, prev_vol) раз в сутки."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    if symbol in BASE_DATA_CACHE and BASE_DATA_CACHE[symbol]['cache_date'] == today_str:
        return BASE_DATA_CACHE[symbol]

    try:
        # Берем историю за 10 дней для расчета среднего объема и объема вчера
        h = t_obj.history(period="10d")
        if h.empty: return None

        last_row = h.iloc[-1]
        prev_row = h.iloc[-2] if len(h) > 1 else last_row
        
        # Средний объем (пытаемся взять из info, если нет - считаем среднее за 10 дней)
        avg_vol = None
        try:
            avg_vol = t_obj.info.get('averageVolume')
        except:
            pass
        if not avg_vol:
            avg_vol = h['Volume'].mean()
        
        base_info = {
            "open": last_row['Open'],
            "prev_close": prev_row['Close'],
            "prev_day_volume": prev_row['Volume'],
            "average_volume": avg_vol,
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
    t = yf.Ticker(symbol)
    
    # ЗАГРУЗКА БАЗОВЫХ ДАННЫХ (КЭШ)
    # Здесь лежат: open, prev_close, prev_day_volume, average_volume
    # А также high и low, зафиксированные на начало дня.
    base = get_ticker_base_data(symbol, t)
    if not base: return None

    # Попытка №1: Используем fast_info (вместо basic_info)
    f = t.fast_info
    curr = getattr(f, 'last_price', None)
    live_vol = getattr(f, 'last_volume', None)
    live_hi = getattr(f, 'day_high', None)
    live_lo = getattr(f, 'day_low', None)

    # Попытка №2: Если данные застыли (рынок открыт, но fast_info не обновляется)
    # Сравниваем текущую цену с закрытием вчера и объем с 0
    if curr is None or curr == base['prev_close'] or live_vol == 0:
        h_live = t.history(period="1d", interval="1m")
        if not h_live.empty:
            curr = h_live['Close'].iloc[-1]
            # Объем из истории часто более точный для не-US рынков
            live_vol = h_live['Volume'].sum() 
            live_hi = max(live_hi or 0, h_live['High'].max())
            live_lo = min(live_lo or 99999999, h_live['Low'].min())

    # --- ОБНОВЛЕНИЕ КЭША В РЕАЛЬНОМ ВРЕМЕНИ (High/Low/Volume) ---
    # Если за эту секунду мы увидели новый High, который выше того, 
    # что в нашем кэше base — перезаписываем его в кэш.
    if live_hi and (np.isnan(base['high']) or live_hi > base['high']): 
        base['high'] = live_hi
    if live_lo and (np.isnan(base['low']) or live_lo < base['low']): 
        base['low'] = live_lo
    
    # Объем: Yahoo иногда присылает 0 в середине дня. 
    # Мы берем максимальное значение (так как объем в течение дня только растет).
    current_vol = max(live_vol or 0, base['volume'] or 0)
    base['volume'] = current_vol # сохраняем в кэш
    # --------------------------------------------

    # 5. РАСЧЕТ ПРОЦЕНТА ИЗМЕНЕНИЯ
    # Считаем от цены открытия (open), если она есть. Если нет — от prev_close.
    op = base['open']
    base_price = op if op and not np.isnan(op) else base['prev_close']
    pct = ((curr - base_price) / base_price * 100) if curr and base_price else 0

    return {
        "symbol": symbol,
        "current_price": curr,
        "change_percent": round(pct, 2),
        "open": base['open'],
        "high": base['high'],
        "low": base['low'],
        "volume": current_vol,
        "previous_close": base['prev_close'],
        "previous_day_volume": base['prev_day_volume'],
        "average_volume": base['average_volume'],
        "date": base['data_date'],
        "currency": getattr(f, 'currency', 'USD'),
        "exchange": EXCHANGE_MAP.get(getattr(f, 'exchange', ''), getattr(f, 'exchange', ''))
    }


# --- ENDPOINTS ---

# --- РЫНОЧНЫЕ ДАННЫЕ ---

@app.get("/tickers/quote", tags=["Market Data"])
def get_multiple_quotes(symbols: str = Query(...)):
    """
    Получение котировок для списка тикеров с использованием кэша.
    Тикеры через запятую: AAPL,TSLA
    URL: /tickers/quote?symbols=aapl,goog
    """
    try:
        ticker_list = [s.strip().upper() for s in symbols.split(",")]
        result = {}
        for symbol in ticker_list:
            data = get_combined_quote(symbol)
            if data:
                result[symbol] = normalize_value(data)
        return result
    except Exception as e:
        logger.error(f"Quote error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws/price/{tickers}")
async def websocket_price(websocket: WebSocket, tickers: str):
    """
    Асинхронный WebSocket для нескольких тикеров (через запятую).
    Использует кэширование и обновляет High/Low в реальном времени.
    URL: wss://app.domain.name.or.ip/ws/price/eslt.ta,teva.ta,dfns.l,aapl
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
                        "previous_day_volume": normalize_value(data['previous_day_volume']),
                        "average_volume": normalize_value(data['average_volume']),
                        "date": data['date'],
                        "time": datetime.now().isoformat()
                    }
            await websocket.send_json(updates)
            await asyncio.sleep(2) 
    except WebSocketDisconnect:
        logger.info(f"WS Disconnected: {tickers}")
    except Exception as e:
        logger.error(f"WS error: {e}")
        await websocket.close()


# --- ИСТОРИЧЕСКИЕ ДАННЫЕ ---
# Важно: /history/tickerlist должен находится в коде перед /history/{ticker}

@app.get("/history/tickerlist", tags=["Historical Data"])
def get_multiple_histories(
    symbols: str = Query(
        ..., 
        description="Тикеры через запятую (1-20 символов каждый, буквы, цифры, точки или дефисы)", 
        example="AAPL,TSLA,TEVA.TA"
    ), 
    period: str = Query("1mo", description="Период (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"), 
    interval: str = Query("1d", description="Интервал свечи (1m, 5m, 1h, 1d, 1wk, 1mo)"),
    start: Optional[str] = Query(None, description="Начальная дата (YYYY-MM-DD). Если указана, 'period' игнорируется.", example="2023-01-01"),
    end: Optional[str] = Query(None, description="Конечная дата (YYYY-MM-DD)", example="2023-12-31")
):
    """
    Получение истории (данные OHLC + Volume + Dividends + Splits) для нескольких тикеров сразу.
    
    - **symbols**: Список тикеров через запятую.
    - **start/end**: Позволяют получить данные за конкретный промежуток. Если указан 'start', параметр 'period' игнорируется.
    - **interval**: Интервал агрегации (от 1 минуты до 3 месяцев).
    
    URL: /history/tickerlist?symbols=AAPL,MSFT&start=2023-01-01

    URL: /history/tickerlist?symbols=AAPL,MSFT&period=1mo&interval=1d
    
    Возвращает словарь: { "AAPL": [свечи], "TSLA": [свечи] }
    """
    ticker_list = [s.strip().upper() for s in symbols.split(",")]
    result = {}

    for symbol in ticker_list:
        try:
            t = yf.Ticker(symbol)
            # Если переданы даты, используем их, иначе используем period
            hist = t.history(period=period, interval=interval, start=start, end=end)
            
            if not hist.empty:
                result[symbol] = normalize_value(hist)
            else:
                result[symbol] = [] 
                
        except Exception as e:
            logger.error(f"Error fetching history for {symbol}: {e}")
            result[symbol] = {"error": str(e)}

    return result
    

@app.get("/history/{ticker}", tags=["Historical Data"])
def get_history(
    ticker: str = Path(
        ..., 
        description="Ticker symbol (1-20 alphanumeric, may include . or -)", 
        pattern="^[A-Za-z0-9\\.-]{1,20}$", # Это ограничение на ввод
        example="AAPL"
    ),
    period: str = Query("1mo", description="Data period (e.g., 1mo, 1y, max). Available periods: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max."),
    interval: str = Query("1d", description="Data aggregation interval (e.g., 1h, 1d, 1wk). Available intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo."),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)", example="2023-01-01"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)", example="2023-12-31")
):
    """
    Исторические данные OHLC + Volume + Dividends + Splits для одного тикера.
    
    - **ticker**: Биржевой символ компании.
    - **start/end**: Используются для точного выбора временного диапазона.
    
    URL: /history/AAPL?period=1mo&interval=1d

    URL: /history/AAPL?start=2023-01-01
    """
    try:
        t = yf.Ticker(ticker.upper())
        # Приоритет дат над периодом встроен в саму библиотеку yfinance
        hist = t.history(period=period, interval=interval, start=start, end=end)
        return normalize_value(hist)
    except Exception as e:
        logger.error(f"History error for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- УТИЛИТЫ ---
@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название или тикер")):
    """
    Поиск с приоритетом тикеров, начинающихся на запрос.
    
    URL: /search?query=aap
    """
    try:
        q = query.strip().upper()
        # Запрашиваем больше (например, 20), чтобы после фильтрации 
        # новостей осталось хотя бы 10-15 тикеров.
        s = yf.Search(q, max_results=25) 
        
        quotes = s.quotes
        if not quotes:
            return {"results": []}

        # Логируем для отладки, сколько реально пришло тикеров от Yahoo
        logger.info(f"Query '{q}' returned {len(quotes)} raw quotes from Yahoo")

        # Сортировка: 
        # 1. Сначала те, чей тикер начинается ровно на запрос
        # 2. Внутри групп - по весу (score)
        sorted_quotes = sorted(
            quotes, 
            key=lambda x: (
                not x.get('symbol', '').startswith(q), 
                -x.get('score', 0)
            )
        )

        # Возвращаем максимум 15, если пришло больше
        return {"results": normalize_value(sorted_quotes[:15])}
        
    except Exception as e:
        logger.error(f"Search error for '{query}': {e}")
        raise HTTPException(status_code=500, detail="Search failed")


@app.get("/info/{ticker}", tags=["Full Data"])
def get_info(ticker: str):
    """
    Полная информация о компании.
    
    URL: /info/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.info)


# --- ФИНАНСОВЫЕ ДАННЫЕ ---
@app.get("/financials/{ticker}", tags=["Financial Data"])
def get_financials(ticker: str):
    t = yf.Ticker(ticker.upper())
    return normalize_value({
        "income": t.income_stmt, 
        "balance": t.balance_sheet, 
        "cash": t.cashflow
    })


# --- КОРПОРАТИВНЫЕ СОБЫТИЯ ---

@app.get("/dividends/{ticker}", tags=["Corporate Actions"])
def get_dividends(ticker: str):
    """
    История дивидендов.
    
    URL: /dividends/aapl
    """
    t = yf.Ticker(ticker.upper())
    divs = t.dividends
    if divs.empty:
        return {"symbol": ticker.upper(), "message": "No dividends found", "data": []}
    return normalize_value(divs)

@app.get("/splits/{ticker}", tags=["Corporate Actions"])
def get_splits(ticker: str):
    """
    История сплитов.
    
    URL: /splits/aapl
    """
    t = yf.Ticker(ticker.upper())
    splits = t.splits
    if splits.empty:
        return {"symbol": ticker.upper(), "message": "No splits found", "data": []}
    return normalize_value(splits)

@app.get("/actions/{ticker}", tags=["Corporate Actions"])
def get_actions(ticker: str):
    """
    Все действия (дивиденды + сплиты).
    
    URL: /actions/aapl
    """
    t = yf.Ticker(ticker.upper())
    if t.actions.empty:
        return {"symbol": ticker.upper(), "message": "No actions found", "data": []}
    return normalize_value(t.actions)


# --- ДОПОЛНИТЕЛЬНЫЕ ЭНДПОИНТЫ ---

@app.get("/calendar/{ticker}", tags=["Information"])
def get_calendar(ticker: str):
    """
    Календарь событий (отчеты, дивиденды).
    
    URL: /calendar/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.calendar)

@app.get("/news/{ticker}", tags=["Information"])
def get_news(ticker: str):
    """
    Последние новости по тикеру.

    URL: /news/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.news)

@app.get("/holders/{ticker}", tags=["Information"])
def get_holders(ticker: str):
    """
    Крупнейшие держатели акций.
    
    URL: /holders/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value({
        "major": t.major_holders,
        "institutional": t.institutional_holders
    })

@app.get("/recommendations/{ticker}", tags=["Information"])
def get_recommendations(ticker: str):
    """
    Рекомендации аналитиков.
    
    URL: /recomendations/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.recommendations)


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
