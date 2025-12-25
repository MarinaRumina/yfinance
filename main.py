import asyncio
import logging
from typing import Optional, Any, List, Dict
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="yfinance API",
    description="Full-featured API for Yahoo Finance with WebSockets and Search",
    version="1.1.0"
)

# Настройка CORS для доступа из браузера
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Вспомогательная функция для очистки данных перед отправкой в JSON ---
def normalize_value(v: Any) -> Any:
    """Преобразует типы данных Pandas/NumPy в стандартные типы Python"""
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat()
    if isinstance(v, (np.integer, int)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v)
    if isinstance(v, pd.DataFrame):
        return v.reset_index().to_dict(orient="records")
    if isinstance(v, pd.Series):
        return v.to_dict()
    if isinstance(v, dict):
        return {str(k): normalize_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [normalize_value(i) for i in v]
    return v

# --- НОВЫЙ: Эндпоинт поиска ---
@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название компании или символ")):
    """Поиск тикеров по названию (напр. Apple или Газпром)"""
    try:
        s = yf.Search(query, max_results=10)
        return {"results": normalize_value(s.quotes)}
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- НОВЫЙ: WebSocket для живых котировок ---
@app.websocket("/ws/price/{ticker}")
async def websocket_price(websocket: WebSocket, ticker: str):
    await websocket.accept()
    ticker_sym = ticker.upper()
    try:
        t = yf.Ticker(ticker_sym)
        # Получаем базовые данные один раз для расчета
        prev_close = t.fast_info.get('previous_close')
        exchange_name = t.info.get('exchDisp', t.fast_info.get('exchange')) # Берем красивое имя

        while True:
            fast = t.fast_info
            current_price = fast.get('last_price')
            
            # Рассчитываем процент изменения
            change_percent = 0.0
            if current_price and prev_close:
                change_percent = ((current_price - prev_close) / prev_close) * 100

            data = {
                "symbol": ticker_sym,
                "price": normalize_value(current_price),
                "change_percent": normalize_value(round(change_percent, 2)),
                "high": normalize_value(fast.get('day_high')),
                "low": normalize_value(fast.get('day_low')),
                "volume": normalize_value(fast.get('last_volume')),
                "exchange": exchange_name,
                "time": datetime.now().isoformat()
            }
            
            await websocket.send_json(data)
            await asyncio.sleep(2) # Пауза, чтобы не нагружать Yahoo
            
    except WebSocketDisconnect:
        logger.info(f"Client disconnected from {ticker_sym}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.close()


# --- ВАШИ ОРИГИНАЛЬНЫЕ ЭНДПОИНТЫ (УЛУЧШЕННЫЕ) ---

@app.get("/info/{ticker}")
def get_info(ticker: str):
    t = yf.Ticker(ticker)
    info = t.info
    if not info:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return normalize_value(info)

@app.get("/history/{ticker}")
def get_history(ticker: str, period: str = "1mo", interval: str = "1d"):
    t = yf.Ticker(ticker)
    hist = t.history(period=period, interval=interval)
    if hist.empty:
        raise HTTPException(status_code=404, detail="No history found")
    return normalize_value(hist)

@app.get("/financials/{ticker}", tags=["Data"])
def get_financials(ticker: str):
    try:
        t = yf.Ticker(ticker)
        
        # Получаем данные и сразу конвертируем в словари, если они не пустые
        income = t.income_stmt
        balance = t.balance_sheet
        cash = t.cashflow
        
        data = {
            "income_statement": income.to_dict() if not income.empty else {},
            "balance_sheet": balance.to_dict() if not balance.empty else {},
            "cashflow": cash.to_dict() if not cash.empty else {}
        }
        
        # Прогоняем через вашу функцию очистки для безопасности
        return normalize_value(data)
    except Exception as e:
        logger.error(f"Financials error for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching financials: {str(e)}")
        
# --- 1. Данные по нескольким тикерам сразу ---
@app.get("/tickers/quote", tags=["Bulk Data"])
def get_multiple_quotes(symbols: str = Query(..., description="AAPL,MSFT,BTC-USD")):
    try:
        ticker_list = [s.strip().upper() for s in symbols.split(",")]
        tickers = yf.Tickers(" ".join(ticker_list))
        result = {}

        for symbol in ticker_list:
            t = tickers.tickers[symbol]
            fast = t.fast_info
            
            # Получаем расширенную информацию (может быть чуть медленнее из-за t.info)
            try:
                exchange_full = t.info.get('exchDisp', fast.get('exchange'))
            except:
                exchange_full = fast.get('exchange')

            current_price = fast.get('last_price')
            prev_close = fast.get('previous_close')
            
            # Расчет процента
            change_percent = 0.0
            if current_price and prev_close:
                change_percent = ((current_price - prev_close) / prev_close) * 100

            result[symbol] = {
                "current_price": normalize_value(current_price),
                "previous_close": normalize_value(prev_close),
                "change_percent": normalize_value(round(change_percent, 2)),
                "open": normalize_value(fast.get('open')),
                "high": normalize_value(fast.get('day_high')),
                "low": normalize_value(fast.get('day_low')),
                "volume": normalize_value(fast.get('last_volume')),
                "exchange": exchange_full,
                "exchange_code": fast.get('exchange')
            }
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- 1. Дивиденды (с проверкой на наличие) ---
@app.get("/dividends/{ticker}", tags=["Corporate Actions"])
def get_dividends(ticker: str):
    """История выплат дивидендов с уведомлением об отсутствии данных"""
    try:
        t = yf.Ticker(ticker.upper())
        divs = t.dividends
        
        if divs is None or divs.empty:
            return {
                "symbol": ticker.upper(),
                "message": f"No dividend data found for {ticker.upper()}",
                "data": []
            }
            
        return normalize_value(divs)
    except Exception as e:
        logger.error(f"Dividends error for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

# --- 2. Сплиты (с проверкой на наличие) ---
@app.get("/splits/{ticker}", tags=["Corporate Actions"])
def get_splits(ticker: str):
    """История сплитов акций с уведомлением об отсутствии данных"""
    try:
        t = yf.Ticker(ticker.upper())
        splits = t.splits
        
        if splits is None or splits.empty:
            return {
                "symbol": ticker.upper(),
                "message": f"No split data found for {ticker.upper()}",
                "data": []
            }
            
        return normalize_value(splits)
    except Exception as e:
        logger.error(f"Splits error for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

# --- 3. Все действия (дивиденды + сплиты одним списком) ---
@app.get("/actions/{ticker}", tags=["Corporate Actions"])
def get_actions(ticker: str):
    """Все корпоративные действия (дивиденды и сплиты вместе)"""
    try:
        t = yf.Ticker(ticker.upper())
        actions = t.actions
        
        if actions is None or actions.empty:
            return {
                "symbol": ticker.upper(),
                "message": f"No corporate actions (dividends/splits) found for {ticker.upper()}",
                "data": []
            }
            
        return normalize_value(actions)
    except Exception as e:
        logger.error(f"Actions error for {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


# ---------------------------
@app.get("/holders/{ticker}")
def get_holders(ticker: str):
    t = yf.Ticker(ticker)
    return {
        "major_holders": normalize_value(t.major_holders),
        "institutional_holders": normalize_value(t.institutional_holders)
    }

@app.get("/news/{ticker}")
def get_news(ticker: str):
    t = yf.Ticker(ticker)
    return normalize_value(t.news)

@app.get("/recommendations/{ticker}")
def get_recommendations(ticker: str):
    t = yf.Ticker(ticker)
    return normalize_value(t.recommendations)

@app.get("/calendar/{ticker}")
def get_calendar(ticker: str):
    t = yf.Ticker(ticker)
    return normalize_value(t.calendar)

@app.get("/health")
def health():
    return {"status": "online", "timestamp": datetime.now().isoformat()}
    

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
