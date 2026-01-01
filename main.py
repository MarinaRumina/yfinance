import asyncio
import json
import logging
import yfinance as yf
import pandas as pd
import numpy as np
import os
from typing import Optional, Any, List, Dict
from datetime import datetime, timezone, date
from fastapi import FastAPI, HTTPException, Query, Path, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from yfinance import AsyncWebSocket
from websockets.exceptions import ConnectionClosedError


# --- НАСТРОЙКА ЛОГОВ ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PRO_LEAD")

app = FastAPI(title="YFinance Enterprise Streamer v3")
app.add_middleware(CORSMiddleware, allow_origins=["*"])


# ПРОВЕРКА ВЕРСИИ ПРИ ЗАПУСКЕ
logger.info(f"🚀 YFINANCE VERSION INSTALLED: {yf.__version__}")


app = FastAPI(
    title="YFinance Ultimate API", 
    version="2.1.2",
    description="Professional API for Yahoo Finance. Fixed WebSocket streaming for real-time client visibility."
)

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_methods=["*"], 
    allow_headers=["*"]
)

# Расширенная карта состояний рынка
MARKET_STAGE_MAP = {
    "PRE": "Pre-Market",
    "POST": "After-Hours",
    "CLOSED": "Closed",
    "REGULAR": "Regular",
    "PREPRE": "Early-Morning",
    "POSTPOST": "Overnight",
    "OFF": "Holiday/Closed"
}


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

def clean_val(val):
    """Очистка данных для JSON."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (np.integer, int)): return int(val)
    if isinstance(val, (np.floating, float)): return float(val)
    return val

def safe_get(obj, attr, default=None):
    """Безопасное извлечение атрибута из объектов yfinance."""
    try:
        # Пытаемся получить как атрибут, потом как элемент словаря
        res = getattr(obj, attr, None)
        if res is None and hasattr(obj, 'get'):
            res = obj.get(attr)
        return res if res is not None else default
    except:
        return default


def normalize_value(v: Any) -> Any:
    """Приводит типы данных Pandas/Numpy к JSON-совместимым форматам."""
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
    """Обновляет кэш базовых цен открытия/закрытия один раз в день."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    if symbol in BASE_DATA_CACHE and BASE_DATA_CACHE[symbol]['cache_date'] == today_str:
        return BASE_DATA_CACHE[symbol]

    try:
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
        logger.error(f"Cache error for {symbol}: {e}")
        return None

def get_combined_quote(symbol: str):
    """Собирает полный объект данных (кэш + живые данные из fast_info)."""
    t = yf.Ticker(symbol)
    base = get_ticker_base_data(symbol, t)
    if not base: return None

    f = t.fast_info
    curr = getattr(f, 'last_price', None)
    
    # Расчет процента
    op = base['open']
    base_price = op if op and not np.isnan(op) else base['prev_close']
    pct = ((curr - base_price) / base_price * 100) if curr and base_price else 0

    return {
        "symbol": symbol,
        "current_price": curr,
        "change_percent": round(pct, 2),
        "open": base['open'],
        "high": max(getattr(f, 'day_high', 0), base['high']),
        "low": min(getattr(f, 'day_low', 9999999), base['low']),
        "volume": max(getattr(f, 'last_volume', 0), base['volume']),
        "previous_close": base['prev_close'],
        "date": datetime.now(timezone.utc).isoformat(),
        "currency": getattr(f, 'currency', 'USD'),
        "exchange": EXCHANGE_MAP.get(getattr(f, 'exchange', ''), getattr(f, 'exchange', ''))
    }

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ (Решает проблему пустого экрана в клиенте при запуске websocket) ---
def clean_for_json(obj: Any) -> Any:
    """
    Подготавливает любые данные к отправке через JSON.
    FastAPI не умеет сам сериализовать numpy-числа, NaN, Timestamp и т.д.
    Если отправить такой мусор - сокет молча упадет.
    """
    # 1. Обработка пустоты и NaN
    if obj is None:
        return None
    
    # 2. Обработка чисел (Float / Numpy Float)
    if isinstance(obj, (float, np.float16, np.float32, np.float64)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    
    # 3. Обработка целых чисел (Numpy Int)
    if isinstance(obj, (np.int8, np.int16, np.int32, np.int64, np.integer)):
        return int(obj)
        
    # 4. Обработка словарей (рекурсия)
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    
    # 5. Обработка списков (рекурсия)
    if isinstance(obj, (list, tuple, np.ndarray)):
        return [clean_for_json(v) for v in obj]
        
    # 6. Обработка Времени (Pandas Timestamp / Datetime / Date)
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
        
    return obj



# --- ENDPOINTS ---

# --- РЫНОЧНЫЕ ДАННЫЕ ---

@app.get("/tickers/quote", tags=["Market Data"])
def get_multiple_quotes(
    symbols: str = Query(..., description="Список тикеров через запятую", example="AAPL,TSLA,GOOGL,ESLT.TA")
):
    """
    **Получение текущих котировок (Live Quotes).**
    
    Возвращает актуальную цену, процент изменения, дневные High/Low и объемы.
    Данные кэшируются на уровне базовых показателей дня для оптимизации скорости.
    
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



# --- ОБНОВЛЕННЫЙ ВЕБСОКЕТ С РАЗДЕЛЕНИЕМ ПОТОКОВ ---

@app.websocket("/ws/price/{tickers}")
async def websocket_price(websocket: WebSocket, tickers: str):
    await websocket.accept()
    ticker_symbols = [s.strip().upper() for s in tickers.split(",")]
    
    # Shared state
    state: Dict[str, Dict[str, Any]] = {}
    stop_event = asyncio.Event()

    async def update_ticker_state(symbol: str, force_rest=False):
        """Обновляет состояние конкретного тикера через REST API."""
        try:
            t = yf.Ticker(symbol)
            # В праздники fast_info может быть пуст, берем из базового info если нужно
            f = t.fast_info
            
            # Собираем данные максимально безопасно
            price = clean_val(safe_get(f, 'last_price'))
            prev_close = clean_val(safe_get(f, 'previous_close'))
            
            # Расчет изменения
            change_pct = 0.0
            if price and prev_close:
                change_pct = round(((price - prev_close) / prev_close) * 100, 2)

            m_state = safe_get(f, 'market_state', 'UNKNOWN').upper()
            
            new_data = {
                "symbol": symbol,
                "current_price": price,
                "change_percent": change_pct,
                "open": clean_val(safe_get(f, 'open')),
                "high": clean_val(safe_get(f, 'day_high')),
                "low": clean_val(safe_get(f, 'day_low')),
                "volume": clean_val(safe_get(f, 'last_volume')),
                "previous_close": prev_close,
                "currency": safe_get(f, 'currency', 'USD'),
                "exchange": safe_get(f, 'exchange', 'N/A'),
                "market_stage": MARKET_STAGE_MAP.get(m_state, m_state),
                "trade_date": datetime.now(timezone.utc).isoformat(),
                "source": "api_refresh" if force_rest else "init"
            }
            
            if symbol in state:
                state[symbol].update(new_data)
            else:
                state[symbol] = new_data
                
        except Exception as e:
            logger.error(f"Error updating {symbol}: {e}")

    # 1. Поток Yahoo WebSocket (Live Stream)
    async def yahoo_streamer():
        while not stop_event.is_set():
            try:
                aws = AsyncWebSocket()
                # Подписка может вернуть None, если Yahoo сбросил соединение
                success = await aws.subscribe(ticker_symbols)
                iterator = await aws.listen()
                
                if iterator is None:
                    await asyncio.sleep(5)
                    continue

                async for msg in iterator:
                    if stop_event.is_set(): break
                    if not msg or 'id' not in msg: continue
                    
                    sym = msg['id']
                    if sym in state:
                        s = state[sym]
                        s.update({
                            "current_price": clean_val(msg.get('price', s["current_price"])),
                            "change_percent": round(msg.get('changePercent', s["change_percent"]), 2),
                            "volume": clean_val(msg.get('dayVolume', s["volume"])),
                            "source": "live_stream"
                        })
                        if 'time' in msg:
                            s["trade_date"] = datetime.fromtimestamp(int(msg['time'])/1000, tz=timezone.utc).isoformat()
            except Exception as e:
                logger.error(f"WS Streamer error: {e}")
                await asyncio.sleep(5)

    # 2. Фоновое обновление (REST API) - для High/Low и стадий рынка
    async def metadata_updater():
        while not stop_event.is_set():
            try:
                # Обновляем по одному, чтобы не блокировать цикл
                for sym in ticker_symbols:
                    if stop_event.is_set(): break
                    await update_ticker_state(sym, force_rest=True)
                    await asyncio.sleep(1) # Небольшая пауза между тикерами
                
                await asyncio.sleep(10) # Общая пауза цикла
            except Exception as e:
                logger.error(f"Metadata Task error: {e}")
                await asyncio.sleep(5)

    # 3. Отправка данных клиенту
    async def broadcaster():
        while not stop_event.is_set():
            try:
                if state:
                    # Важно: шлем копию, чтобы избежать RuntimeError при изменении словаря
                    payload = json.dumps(clean_val(state))
                    await websocket.send_text(payload)
                await asyncio.sleep(1)
            except (WebSocketDisconnect, ConnectionClosedError):
                stop_event.set()
            except Exception as e:
                stop_event.set()

    # --- СТАРТ ---
    # 1. Первая инициализация (ждем её)
    await asyncio.gather(*[update_ticker_state(s) for s in ticker_symbols])
    
    # 2. Запуск фоновых процессов
    tasks = [
        asyncio.create_task(yahoo_streamer()),
        asyncio.create_task(metadata_updater()),
        asyncio.create_task(broadcaster())
    ]

    try:
        await stop_event.wait()
    finally:
        stop_event.set()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Connection cleaned up for {tickers}")
            


# --- ИСТОРИЧЕСКИЕ ДАННЫЕ ---
# Важно: /history/tickerlist должен находится в коде перед /history/{ticker}

@app.get("/history/tickerlist", tags=["Historical Data"])
def get_multiple_histories(
    symbols: str = Query(
        ..., 
        description="Ticker symbols separated by commas (1-20 alphanumeric, may include . or -)", 
        example="AAPL,TSLA,TEVA.TA"
    ), 
    period: Optional[str] = Query("1mo", description="Период данных (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"), 
    interval: str = Query("1d", description="Интервал свечи (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)"),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD). If set, 'period' will be ignored.", example="2023-01-01"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD).", example="2023-12-31")
):
    """
    **Пакетное получение исторических данных (OHLC + Volume + Dividends + Splits) для нескольких тикеров сразу.**
    
    - **symbols**: Список тикеров через запятую.
    - **start/end**: Позволяют получить данные за конкретный промежуток времени. Если указан **start** или **end**, параметр **period** игнорируется автоматически.
    - **interval**: Интервал агрегации (размер свечи, от 1 минуты до 3 месяцев).
    - Возвращает словарь, где ключи — тикеры, а значения — массивы исторических записей ( { "AAPL": [свечи], "TSLA": [свечи] } ).
    
    URL: /history/tickerlist?symbols=AAPL,MSFT&start=2023-01-01
    
    URL: /history/tickerlist?symbols=AAPL,MSFT&start=2023-01-01&end=2023-12-31&interval=1d

    URL: /history/tickerlist?symbols=AAPL,MSFT&period=1mo&interval=1d
    """
    ticker_list = [s.strip().upper() for s in symbols.split(",")]
    result = {}

    # Логика обхода ошибки "nonsense": если есть start или end, убираем period
    effective_period = period
    if start or end:
        effective_period = None

    for symbol in ticker_list:
        try:
            t = yf.Ticker(symbol)
            # Передаем только разрешенную комбинацию параметров
            hist = t.history(
                period=effective_period, 
                interval=interval, 
                start=start, 
                end=end
            )
            
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
    interval: str = Query("1d", description="Data aggregation interval (candle size, e.g., 1h, 1d, 1wk). Available intervals: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo."),
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)", example="2023-01-01"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)", example="2023-12-31")
):
    """
    **Исторические данные OHLC + Volume + Dividends + Splits для одного тикера.**
    
    - **ticker**: Биржевой символ компании.
    - **start/end**: Используются для точного выбора временного диапазона.
    
    URL: /history/AAPL?period=1mo&interval=1d

    URL: /history/AAPL?start=2023-01-01
    """
    try:
        effective_period = None if (start or end) else period
        t = yf.Ticker(ticker.upper())
        # Приоритет дат над периодом встроен в саму библиотеку yfinance
        hist = t.history(period=effective_period, interval=interval, start=start, end=end)
        return normalize_value(hist)
    except Exception as e:
        logger.error(f"History error for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- УТИЛИТЫ ---
@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название компании или тикер", example="Apple")):
    """
    **Умный поиск активов.**
    
    Возвращает до 15 наиболее релевантных результатов. Приоритет отдается тикерам, начинающимся на поисковый запрос.
    
    URL: /search?query=aap
    """
    try:
        q = query.strip().upper()
        s = yf.Search(q, max_results=15)        
        quotes = s.quotes
        if not quotes:
            return {"results": []}

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
    **Полный профиль компании.** Фундаментальные данные, описание, сектор, сотрудники и т.д.
    
    URL: /info/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.info)


# --- ФИНАНСОВЫЕ ДАННЫЕ ---
@app.get("/financials/{ticker}", tags=["Financial Data"])
def get_financials(ticker: str):
    """**Финансовая отчетность.** Income Statement, Balance Sheet и Cash Flow."""
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
    **История выплаты дивидендов.**
    
    URL: /dividends/aapl
    """
    t = yf.Ticker(ticker.upper())
    divs = t.dividends
    return normalize_value(divs) if not divs.empty else {"message": "No dividends found"}

@app.get("/splits/{ticker}", tags=["Corporate Actions"])
def get_splits(ticker: str):
    """
    **История сплитов акций.**
    
    URL: /splits/aapl
    """
    t = yf.Ticker(ticker.upper())
    splits = t.splits
    return normalize_value(splits) if not splits.empty else {"message": "No splits found"}

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
    **Календарь корпоративных событий.** Даты отчетов и ближайших дивидендов.
    
    URL: /calendar/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.calendar)

@app.get("/news/{ticker}", tags=["Information"])
def get_news(ticker: str):
    """
    **Лента последних новостей по активу.**

    URL: /news/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.news)

@app.get("/holders/{ticker}", tags=["Information"])
def get_holders(ticker: str):
    """
    **Крупнейшие держатели актива.**
    
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
    **Рекомендации аналитиков по активу (byu/sell/hold/etc.).**
    
    URL: /recomendations/aapl
    """
    t = yf.Ticker(ticker.upper())
    return normalize_value(t.recommendations)


@app.get("/health", tags=["Utility"])
def health():
    """**Проверка здоровья API.** Возвращает статус системы и текущий размер кэша."""
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
