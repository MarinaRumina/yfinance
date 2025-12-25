import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
import yfinance as yf
import pandas as pd
from typing import Optional

app = FastAPI(title="yfinance wrapper API")


def normalize_value(v: Any):
    # Convert numpy/pandas scalars to python builtins, handle NaN/NaT
    try:
        if v is None:
            return None
        if isinstance(v, (np.generic,)):
            return v.item()
        if isinstance(v, pd.Timestamp):
            # ensure ISO format with timezone if present
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        if pd.isna(v):
            return None
        # If it's a numpy array or pandas object, try to convert
        if isinstance(v, (np.ndarray,)):
            return v.tolist()
        return v
    except Exception:
        return str(v)


def df_to_records(df: pd.DataFrame):
    # default: list of row-dicts with Date included if index is datetime-like
    if df is None:
        return []
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if not isinstance(df, pd.DataFrame):
        return df
    out = []
    df2 = df.copy()
    if df2.index.name is None and not df2.index.equals(pd.RangeIndex(len(df2))):
        # include index as 'Date' (or 'index') field
        idx_name = "Date"
        df2 = df2.reset_index().rename(columns={df2.columns[0]: df2.columns[0]})
        # ensure index column name
        df2 = df.reset_index()
    else:
        df2 = df.reset_index()
    for _, row in df2.iterrows():
        d = {}
        for k, v in row.items():
            d[str(k)] = normalize_value(v)
        out.append(d)
    return out


def df_to_col_dict_with_iso_index(df: pd.DataFrame):
    # returns {col: {index_iso: value}} preserving original actions format
    if df is None:
        return {}
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if not isinstance(df, pd.DataFrame):
        return df
    result = {}
    for col in df.columns:
        inner = {}
        for idx, val in zip(df.index, df[col]):
            # convert index (Timestamp) to iso string if possible
            if isinstance(idx, pd.Timestamp):
                key = idx.isoformat()
            else:
                key = str(idx)
            inner[key] = normalize_value(val)
        result[str(col)] = inner
    return result


def df_to_date_keyed_dict(df: pd.DataFrame):
    # convert df (rows=index metrics, cols=periods/dates) into {date_str: {metric: value}}
    if df is None:
        return {}
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if not isinstance(df, pd.DataFrame):
        return df
    # If columns are datelike -> use columns as dates
    cols = df.columns
    def col_to_str(c):
        if isinstance(c, pd.Timestamp):
            return c.isoformat()
        try:
            # try convert to Timestamp
            ts = pd.to_datetime(c, errors="coerce")
            if not pd.isna(ts):
                return ts.isoformat()
        except Exception:
            pass
        return str(c)

    out = {}
    for col in cols:
        key = col_to_str(col)
        # build dict of metric->value from column
        col_dict = {}
        for metric, val in zip(df.index, df[col]):
            col_dict[str(metric)] = normalize_value(val)
        out[key] = col_dict
    return out


def safe_to_dict_for_endpoint(obj: Any, endpoint: str = ""):
    """
    smart conversion depending on endpoint name:
      - actions -> column-oriented dict {col: {date: value}}
      - financials, balance_sheet, cashflow, earnings -> date-keyed dict {date: {metric: value}}
      - default DataFrame -> list of records
    """
    # early exits
    if obj is None:
        return {}
    if isinstance(obj, dict):
        # already a dict
        # normalize nested values
        def normalize_dict(d):
            out = {}
            for k, v in d.items():
                if isinstance(v, (pd.DataFrame, pd.Series)):
                    out[k] = safe_to_dict_for_endpoint(v, endpoint)
                else:
                    out[k] = normalize_value(v)
            return out
        return normalize_dict(obj)

    if isinstance(obj, (pd.Series, pd.DataFrame)):
        df = obj if isinstance(obj, pd.DataFrame) else obj.to_frame()
        name = endpoint.lower()
        if "action" in name:
            return df_to_col_dict_with_iso_index(df)
        if name in ("financials", "balance_sheet", "cashflow", "earnings", "quarterly_financials", "quarterly_balance_sheet"):
            return df_to_date_keyed_dict(df)
        # default for history/download/etc.
        return df_to_records(df)

    # For other iterables (like list)
    if isinstance(obj, list):
        return [safe_to_dict_for_endpoint(x, endpoint) for x in obj]

    # fallback for scalars
    return normalize_value(obj)


# --- endpoints ---
@app.get("/quote")
def quote(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        data = t.history(period="1d")
        return safe_to_dict_for_endpoint(data, "history")
    except Exception as e:
        return {"error": str(e)}


#@app.get("/info")
#def info(ticker: str = Query(...)):
#    try:
#        t = yf.Ticker(ticker)
#        return safe_to_dict_for_endpoint(t.info, "info")
#    except Exception as e:
#        return {"error": str(e)}

@app.get("/info/{ticker}", tags=["Data"])
def get_info(ticker: str):
    t = yf.Ticker(ticker)
    info = t.info
    if not info or len(info) < 2: # Yahoo иногда возвращает пустой словарь
        raise HTTPException(status_code=404, detail="Ticker not found or no info available")
    return info

#@app.get("/dividends")
#def dividends(ticker: str = Query(...)):
#    try:
#        t = yf.Ticker(ticker)
#        return safe_to_dict_for_endpoint(t.dividends, "dividends")
#    except Exception as e:
#        return {"error": str(e)}

@app.get("/dividends/{ticker}", tags=["Data"])
def get_dividends(ticker: str):
    t = yf.Ticker(ticker)
    divs = t.dividends
    return divs.to_dict()

@app.get("/actions")
def actions(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        # prefer column-oriented dict { "Dividends": {date: val}, "Stock Splits": {...} }
        return safe_to_dict_for_endpoint(t.actions, "actions")
    except Exception as e:
        return {"error": str(e)}


@app.get("/splits")
def splits(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict_for_endpoint(t.splits, "splits")
    except Exception as e:
        return {"error": str(e)}


@app.get("/financials")
def financials(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict_for_endpoint(t.financials, "financials")
    except Exception as e:
        return {"error": str(e)}


@app.get("/balance_sheet")
def balance_sheet(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict_for_endpoint(t.balance_sheet, "balance_sheet")
    except Exception as e:
        return {"error": str(e)}


@app.get("/cashflow")
def cashflow(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict_for_endpoint(t.cashflow, "cashflow")
    except Exception as e:
        return {"error": str(e)}


@app.get("/calendar")
def calendar(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict_for_endpoint(t.calendar, "calendar")
    except Exception as e:
        return {"error": str(e)}


#@app.get("/history")
#def history(
#    ticker: str = Query(...),
#    period: str = Query("1mo"),
#    interval: str = Query("1d")
#):
#    try:
#        data = yf.download(ticker, period=period, interval=interval, group_by="ticker")
#        # if group_by returns multi-column for multiple tickers, safe conversion will handle it
#        return safe_to_dict_for_endpoint(data, "history")
#    except Exception as e:
#        return {"error": str(e)}
        
@app.get("/history/{ticker}", tags=["Data"])
def get_history(ticker: str, period: str = "1mo", interval: str = "1d"):
    t = yf.Ticker(ticker)
    df = t.history(period=period, interval=interval)
    if df.empty:
        raise HTTPException(status_code=404, detail="No history found")
    # Преобразование индекса даты в строку для JSON
    return df.reset_index().to_dict(orient="records")


@app.get("/tickers")
def tickers(symbols: str = Query(...)):
    try:
        tickers_obj = yf.Tickers(symbols)
        result = {}
        # tickers_obj.tickers: mapping symbol -> Ticker
        for sym, t in tickers_obj.tickers.items():
            try:
                result[str(sym)] = safe_to_dict_for_endpoint(t.info, "info")
            except Exception as e:
                result[str(sym)] = {"error": str(e)}
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/ticker")
def ticker(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return {
            "info": safe_to_dict_for_endpoint(t.info, "info"),
            "history": safe_to_dict_for_endpoint(t.history(period="1y"), "history"),
            "actions": safe_to_dict_for_endpoint(t.actions, "actions"),
            "dividends": safe_to_dict_for_endpoint(t.dividends, "dividends"),
            "splits": safe_to_dict_for_endpoint(t.splits, "splits"),
            "financials": safe_to_dict_for_endpoint(t.financials, "financials"),
            "balance_sheet": safe_to_dict_for_endpoint(t.balance_sheet, "balance_sheet"),
            "cashflow": safe_to_dict_for_endpoint(t.cashflow, "cashflow"),
            "calendar": safe_to_dict_for_endpoint(t.calendar, "calendar"),
            "earnings": safe_to_dict_for_endpoint(getattr(t, "earnings", {}), "earnings"),
            "sustainability": safe_to_dict_for_endpoint(getattr(t, "sustainability", {}), "sustainability"),
            "isin": getattr(t, "isin", None),
            "major_holders": safe_to_dict_for_endpoint(getattr(t, "major_holders", {}), "major_holders"),
            "institutional_holders": safe_to_dict_for_endpoint(getattr(t, "institutional_holders", {}), "institutional_holders")
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/download")
def download(
    tickers: str = Query(...),
    period: str = Query("1mo"),
    interval: str = Query("1d")
):
    try:
        data = yf.download(tickers, period=period, interval=interval, group_by=None)
        return safe_to_dict_for_endpoint(data, "history")
    except Exception as e:
        return {"error": str(e)}


@app.get("/market")
def market(region: str = Query("US")):
    # market API in yfinance is unstable/non-standard; return informative error
    return {"error": "Market endpoint is not supported in this yfinance build. Use search/info endpoints instead."}


# --- 1. Поиск (НОВОЕ) ---
@app.get("/search", tags=["Utility"])
def search_ticker(query: str = Query(..., description="Название компании или тикер (напр. Apple)")):
    """Поиск тикеров и информации о компаниях по ключевому слову"""
    try:
        s = yf.Search(query, max_results=10)
        return {"results": s.quotes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/news")
def news(ticker: str = Query(...)):
    try:
        t = yf.Ticker(ticker)
        return safe_to_dict_for_endpoint(getattr(t, "news", {}), "news")
    except Exception as e:
        return {"error": str(e)}

# --- 2. Живые цены через WebSocket (НОВОЕ) ---
@app.websocket("/ws/price/{ticker}")
async def websocket_endpoint(websocket: WebSocket, ticker: str):
    await websocket.accept()
    try:
        # Пытаемся использовать новый модуль веб-сокетов yfinance
        from yfinance import yf_websocket
        async with yf_websocket.YfWebsocket() as yfw:
            yfw.subscribe([ticker.upper()])
            async for quote in yfw.messages():
                await websocket.send_json({
                    "symbol": ticker.upper(),
                    "price": getattr(quote, 'price', 'N/A'),
                    "time": str(getattr(quote, 'time', 'now'))
                })
    except (ImportError, Exception) as e:
        # Если веб-сокеты не поддерживаются или ошибка, используем имитацию (Polling)
        try:
            while True:
                t = yf.Ticker(ticker)
                price = t.fast_info.get('last_price') or t.basic_info.get('last_price')
                await websocket.send_json({"symbol": ticker.upper(), "price": price, "note": "Real-time via polling"})
                await asyncio.sleep(2) # Пауза 2 секунды, чтобы не забанили
        except WebSocketDisconnect:
            pass

# --- Вспомогательный эндпоинт для проверки работы ---
@app.get("/", tags=["Utility"])
def health_check():
    return {"status": "active", "provider": "yfinance"}
