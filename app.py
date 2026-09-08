# -*- coding: utf-8 -*-
"""
BOT STRATEGIA J225 5m  —  "LONG w konsolidacji"   (v1.4, 08.09.2026)
=====================================================================
Webhook TradingView  ->  Capital.com (REST API)  ->  jedna pozycja LONG na J225.

Reguły strategii (zwalidowane walk-forward):
  * sygnał: alert TradingView (CHOP(14)>60  AND  Strażnik 02:00-09:00 PL  AND  C_5r15MinPMCandleBuy)
  * wejście: natychmiast po alercie, po cenie rynkowej (BUY)
  * take profit  +1,2 %  od ceny wypełnienia   (zlecenie u brokera)
  * stop loss    -0,3 %  od ceny wypełnienia   (zlecenie u brokera)
  * time-stop: zamknięcie po 4 h LUB o 09:00 czasu polskiego - co wcześniej (wątek nadzorcy)
  * jedna pozycja naraz; wielkość = RISK_FRACTION (10 %) salda rachunku jako depozyt
  * bot ignoruje sygnały poza oknem 02:00-08:54 PL (drugie zabezpieczenie obok Strażnika)

Bezpieczniki:
  * ARMED=false  -> tryb suchy: loguje decyzje, nie składa zleceń (dwustopniowe uzbrojenie)
  * TRADING_ENABLED=false -> wstrzymanie nowych wejść (otwarta pozycja nadal nadzorowana)
  * kill switch: saldo < START_EQUITY*(1-MAX_DD_PCT/100)  -> blokada wejść + log CRITICAL
  * MAX_DAILY_LOSS_USD -> po przekroczeniu dziennej straty koniec handlu na dziś
  * sekret webhooka, deduplikacja sygnałów, limit jednej pozycji, odbudowa stanu po restarcie
"""
import os, json, time, threading, logging, hmac
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
import requests
from flask import Flask, request, jsonify

# ----------------------------------------------------------------------------- konfiguracja
def env(name, default=None, cast=str):
    v = os.getenv(name)
    if v is None or v == "":
        return default
    if cast is bool:
        return v.strip().lower() in ("1", "true", "yes", "on", "tak")
    return cast(v)

CFG = dict(
    CAPITAL_ENV        = env("CAPITAL_ENV", "demo").lower(),              # demo | live
    CAPITAL_API_KEY    = env("CAPITAL_API_KEY", ""),
    CAPITAL_IDENTIFIER = env("CAPITAL_IDENTIFIER", ""),                   # e-mail logowania
    CAPITAL_PASSWORD   = env("CAPITAL_PASSWORD", ""),                     # hasło klucza API
    CAPITAL_ACCOUNT_ID = env("CAPITAL_ACCOUNT_ID", ""),                   # opcjonalnie: numer rachunku
    WEBHOOK_SECRET     = env("WEBHOOK_SECRET", ""),
    EPIC               = env("EPIC", "J225"),
    FX_EPIC            = env("FX_EPIC", "USDJPY"),                        # do przeliczenia nominału
    FX_FALLBACK        = env("FX_FALLBACK", 148.0, float),
    RISK_FRACTION      = env("RISK_FRACTION", 0.10, float),               # 10 % salda jako depozyt
    TP_PCT             = env("TP_PCT", 1.2, float),
    SL_PCT             = env("SL_PCT", 0.3, float),
    TIME_STOP_MIN      = env("TIME_STOP_MIN", 240, int),
    CLOSE_BY           = env("CLOSE_BY", "09:00"),                        # czas lokalny TZ
    GUARD_START        = env("GUARD_START", "02:00"),
    GUARD_END          = env("GUARD_END", "08:55"),                       # ostatni akceptowany sygnał < 08:55
    TZ                 = env("TZ", "Europe/Warsaw"),
    ARMED              = env("ARMED", False, bool),
    TRADING_ENABLED    = env("TRADING_ENABLED", True, bool),
    START_EQUITY       = env("START_EQUITY", 218.59, float),
    MAX_DD_PCT         = env("MAX_DD_PCT", 10.0, float),
    MAX_DAILY_LOSS_USD = env("MAX_DAILY_LOSS_USD", 6.0, float),
    STATE_FILE         = env("STATE_FILE", "state.json"),
    HTTP_TIMEOUT       = env("HTTP_TIMEOUT", 10, int),
    KEEPALIVE_URL      = env("KEEPALIVE_URL", ""),          # publiczny adres usługi -> samo-ping (plan Free)
    SUPERVISOR_SEC     = env("SUPERVISOR_SEC", 30, int),
    LOG_LEVEL          = env("LOG_LEVEL", "INFO"),
)
BASE_URL = {"live": "https://api-capital.backend-capital.com",
            "demo": "https://demo-api-capital.backend-capital.com"}[CFG["CAPITAL_ENV"]]
TZ = ZoneInfo(CFG["TZ"])

logging.basicConfig(level=getattr(logging, CFG["LOG_LEVEL"].upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("j225bot")

def now_local():
    return datetime.now(TZ)

def hm(s):
    h, m = s.split(":"); return int(h), int(m)

# ----------------------------------------------------------------------------- klient Capital.com
class CapitalClient:
    """Minimalny klient REST Capital.com z automatycznym odnawianiem sesji."""
    def __init__(self):
        self.s = requests.Session()
        self.cst = None; self.xst = None; self.logged_at = 0
        self.lock = threading.Lock()

    def _hdr(self, auth=True):
        h = {"X-CAP-API-KEY": CFG["CAPITAL_API_KEY"], "Content-Type": "application/json"}
        if auth and self.cst:
            h["CST"] = self.cst; h["X-SECURITY-TOKEN"] = self.xst
        return h

    def login(self):
        with self.lock:
            r = self.s.post(BASE_URL + "/api/v1/session", headers=self._hdr(auth=False),
                            json={"identifier": CFG["CAPITAL_IDENTIFIER"], "password": CFG["CAPITAL_PASSWORD"],
                                  "encryptedPassword": False}, timeout=CFG["HTTP_TIMEOUT"])
            if r.status_code != 200:
                raise RuntimeError(f"logowanie nieudane {r.status_code}: {r.text[:200]}")
            self.cst = r.headers.get("CST"); self.xst = r.headers.get("X-SECURITY-TOKEN"); self.logged_at = time.time()
            body = r.json()
            acc = CFG["CAPITAL_ACCOUNT_ID"]
            if acc and body.get("currentAccountId") != acc:
                r2 = self.s.put(BASE_URL + "/api/v1/session", headers=self._hdr(), json={"accountId": acc}, timeout=CFG["HTTP_TIMEOUT"])
                if r2.status_code != 200:
                    raise RuntimeError(f"nie można przełączyć rachunku na {acc}: {r2.text[:200]}")
                log.info("przełączono aktywny rachunek na %s", acc)
            eff = CFG["CAPITAL_ACCOUNT_ID"] or body.get("currentAccountId")
            log.info("zalogowano do Capital.com (%s), AKTYWNY RACHUNEK: %s", CFG["CAPITAL_ENV"], eff)
            for a in body.get("accounts", []):
                b = a.get("balance", {})
                log.info("RACHUNEK %s | %s | %s | saldo %s | dostępne %s | domyślny=%s", a.get("accountId"), a.get("accountName"),
                         a.get("currency"), b.get("balance"), b.get("available"), a.get("preferred"))
            self.accounts_cache = body.get("accounts", [])

    def call(self, method, path, retry=True, **kw):
        if not self.cst or time.time() - self.logged_at > 540:   # sesja wygasa po 10 min bezczynności
            self.login()
        r = self.s.request(method, BASE_URL + path, headers=self._hdr(), timeout=CFG["HTTP_TIMEOUT"], **kw)
        if r.status_code == 401 and retry:
            self.login(); return self.call(method, path, retry=False, **kw)
        self.logged_at = time.time()
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:300]}")
        return r.json() if r.text else {}

    # --- dane
    def ping(self):            return self.call("GET", "/api/v1/ping")
    def accounts(self):        return self.call("GET", "/api/v1/accounts")["accounts"]
    def market(self, epic):    return self.call("GET", f"/api/v1/markets/{epic}")
    def positions(self):       return self.call("GET", "/api/v1/positions").get("positions", [])
    def balance(self):
        acc = CFG["CAPITAL_ACCOUNT_ID"]
        for a in self.accounts():
            if (acc and a["accountId"] == acc) or (not acc and a.get("preferred")):
                return float(a["balance"]["balance"]), float(a["balance"]["available"])
        a = self.accounts()[0]; return float(a["balance"]["balance"]), float(a["balance"]["available"])
    # --- zlecenia (limit brokera: max 1 żądanie / 0,1 s na pozycjach)
    def open_buy(self, epic, size, stop_dist, profit_dist):
        time.sleep(0.15)
        return self.call("POST", "/api/v1/positions", json={"epic": epic, "direction": "BUY", "size": size,
                         "guaranteedStop": False, "stopDistance": stop_dist, "profitDistance": profit_dist})
    def confirm(self, deal_ref):
        for _ in range(8):
            time.sleep(0.3)
            try: return self.call("GET", f"/api/v1/confirms/{deal_ref}")
            except RuntimeError as e:
                if "404" not in str(e): raise
        raise RuntimeError("brak potwierdzenia transakcji " + deal_ref)
    def update_levels(self, deal_id, stop_level, profit_level):
        time.sleep(0.15)
        return self.call("PUT", f"/api/v1/positions/{deal_id}", json={"stopLevel": stop_level, "profitLevel": profit_level})
    def close(self, deal_id):
        time.sleep(0.15)
        return self.call("DELETE", f"/api/v1/positions/{deal_id}")

# ----------------------------------------------------------------------------- stan bota
class State:
    def __init__(self, path):
        self.path = path; self.lock = threading.Lock()
        self.d = {"position": None, "last_signal": None, "seen": [], "day": None, "day_pnl": 0.0,
                  "trades": [], "halted": False, "halt_reason": "", "own_deals": [], "foreign_warned": []}
        try:
            with open(path) as f: self.d.update(json.load(f))
        except Exception: pass
    def save(self):
        try:
            with open(self.path, "w") as f: json.dump(self.d, f, ensure_ascii=False, indent=1, default=str)
        except Exception as e: log.warning("nie zapisano stanu: %s", e)

api = CapitalClient(); st = State(CFG["STATE_FILE"]); app = Flask(__name__)

# ----------------------------------------------------------------------------- logika
def in_guard(t):
    """Czy czas lokalny t mieści się w oknie wejść [GUARD_START, GUARD_END)."""
    gs, ge = hm(CFG["GUARD_START"]), hm(CFG["GUARD_END"])
    cur = (t.hour, t.minute); return gs <= cur < ge

def deadline_for(open_time):
    """Termin zamknięcia: open + TIME_STOP_MIN lub dzisiejsze CLOSE_BY - co wcześniej."""
    cb_h, cb_m = hm(CFG["CLOSE_BY"])
    close_by = open_time.replace(hour=cb_h, minute=cb_m, second=0, microsecond=0)
    if close_by <= open_time: close_by += timedelta(days=1)
    return min(open_time + timedelta(minutes=CFG["TIME_STOP_MIN"]), close_by)

def compute_size(balance_usd, mkt, fx):
    """Wielkość pozycji: depozyt = RISK_FRACTION*saldo; nominał = depozyt / marginFactor; size = nominał_JPY / cena."""
    ins = mkt["instrument"]; rules = mkt["dealingRules"]; snap = mkt["snapshot"]
    mf = float(ins.get("marginFactor", 5.0)); unit = ins.get("marginFactorUnit", "PERCENTAGE")
    margin_frac = mf / 100.0 if unit == "PERCENTAGE" else mf
    price = float(snap["offer"]); currency = ins.get("currency", "JPY")
    margin_usd = CFG["RISK_FRACTION"] * balance_usd
    notional_usd = margin_usd / margin_frac
    notional_ccy = notional_usd * (fx if currency == "JPY" else 1.0)
    raw = notional_ccy / price
    step = float(rules.get("minDealSize", {}).get("value", 0.1)) or 0.1
    size = int(raw / step) * step
    return round(size, 2), dict(price=price, margin_usd=round(margin_usd, 2), notional_usd=round(notional_usd, 2),
                                margin_frac=margin_frac, step=step, currency=currency, raw=round(raw, 3))

def fx_rate():
    try:
        m = api.market(CFG["FX_EPIC"])["snapshot"]; return (float(m["bid"]) + float(m["offer"])) / 2
    except Exception as e:
        log.warning("brak kursu %s (%s) - używam FX_FALLBACK", CFG["FX_EPIC"], e); return CFG["FX_FALLBACK"]

def risk_checks(balance):
    """Zwraca (ok, powód)."""
    if st.d["halted"]: return False, "bot zatrzymany: " + st.d["halt_reason"]
    if not CFG["TRADING_ENABLED"]: return False, "TRADING_ENABLED=false"
    floor = CFG["START_EQUITY"] * (1 - CFG["MAX_DD_PCT"] / 100)
    if balance < floor:
        st.d["halted"] = True; st.d["halt_reason"] = f"saldo {balance:.2f} < próg kill switch {floor:.2f}"; st.save()
        log.critical(st.d["halt_reason"]); return False, st.d["halt_reason"]
    today = str(now_local().date())
    if st.d["day"] != today: st.d["day"] = today; st.d["day_pnl"] = 0.0
    if st.d["day_pnl"] <= -abs(CFG["MAX_DAILY_LOSS_USD"]): return False, f"limit dziennej straty ({st.d['day_pnl']:.2f} USD)"
    return True, ""

def handle_signal(payload):
    """Główna ścieżka: sygnał -> kontrole -> zlecenie BUY z TP/SL."""
    with st.lock:
        t = now_local(); sig_id = str(payload.get("signal_time") or t.strftime("%Y-%m-%d %H:%M"))
        if sig_id in st.d["seen"]: return {"ok": False, "reason": "duplikat sygnału " + sig_id}
        st.d["seen"] = (st.d["seen"] + [sig_id])[-50:]; st.d["last_signal"] = {"t": str(t), "payload": payload}; st.save()
        if str(payload.get("action", "buy")).lower() != "buy": return {"ok": False, "reason": "bot obsługuje tylko action=buy"}
        if str(payload.get("symbol", CFG["EPIC"])).upper() != CFG["EPIC"].upper(): return {"ok": False, "reason": "inny symbol"}
        if not in_guard(t): return {"ok": False, "reason": f"poza oknem wejść {CFG['GUARD_START']}-{CFG['GUARD_END']} ({t:%H:%M})"}
        balance, available = api.balance()
        ok, why = risk_checks(balance)
        if not ok: return {"ok": False, "reason": why}
        if st.d["position"] or _any_epic_long(): return {"ok": False, "reason": "pozycja na J225 już otwarta (własna lub obca) - sygnał pominięty"}
        mkt = api.market(CFG["EPIC"]); fx = fx_rate()
        size, info = compute_size(balance, mkt, fx)
        if size < info["step"]: return {"ok": False, "reason": f"za mały rachunek na min. rozmiar {info['step']} (obliczono {info['raw']})"}
        tp_pct = float(payload.get("tp_pct", CFG["TP_PCT"])); sl_pct = float(payload.get("sl_pct", CFG["SL_PCT"]))
        stop_dist = round(info["price"] * sl_pct / 100, 1); profit_dist = round(info["price"] * tp_pct / 100, 1)
        plan = dict(epic=CFG["EPIC"], size=size, price=info["price"], stop_dist=stop_dist, profit_dist=profit_dist,
                    margin_usd=info["margin_usd"], notional_usd=info["notional_usd"], balance=balance, fx=round(fx, 3),
                    deadline=str(deadline_for(t)))
        if not CFG["ARMED"]:
            log.info("TRYB SUCHY (ARMED=false) - zlecenie NIE wysłane: %s", plan); return {"ok": True, "dry_run": True, "plan": plan}
        ref = api.open_buy(CFG["EPIC"], size, stop_dist, profit_dist)["dealReference"]
        conf = api.confirm(ref)
        if conf.get("dealStatus") != "ACCEPTED":
            log.error("zlecenie odrzucone: %s", conf); return {"ok": False, "reason": "odrzucone", "confirm": conf}
        deal_id = conf["affectedDeals"][0]["dealId"]; level = float(conf.get("level") or info["price"])
        # doprecyzowanie TP/SL względem faktycznej ceny wypełnienia
        try: api.update_levels(deal_id, round(level * (1 - sl_pct / 100), 1), round(level * (1 + tp_pct / 100), 1))
        except Exception as e: log.warning("nie udało się doprecyzować TP/SL: %s", e)
        st.d["position"] = dict(deal_id=deal_id, level=level, size=size, open_time=str(t), deadline=str(deadline_for(t)),
                                sl=round(level * (1 - sl_pct / 100), 1), tp=round(level * (1 + tp_pct / 100), 1))
        st.d["own_deals"] = (st.d.get("own_deals", []) + [deal_id])[-50:]
        st.save(); log.info("OTWARTO LONG %s size=%s @ %s TP=%s SL=%s deadline=%s", CFG["EPIC"], size, level,
                            st.d["position"]["tp"], st.d["position"]["sl"], st.d["position"]["deadline"])
        return {"ok": True, "position": st.d["position"]}

def _opened_at(broker):
    try: return datetime.fromisoformat(broker["createdDateUTC"].replace("Z", "+00:00")).astimezone(TZ)
    except Exception: return None

def _is_ours(broker):
    """Pozycja jest bota, jeśli zapisał jej dealId, albo została otwarta DZIŚ w oknie wejść (odbudowa po restarcie)."""
    if broker["dealId"] in st.d.get("own_deals", []): return True
    opened = _opened_at(broker)
    return bool(opened and opened.date() == now_local().date() and in_guard(opened))

def bot_position_ours():
    """Otwarta pozycja bota (BUY na EPIC, spełniająca _is_ours) lub None; obce pozycje tylko loguje."""
    for p in api.positions():
        pos, mk = p["position"], p["market"]
        if mk.get("epic") != CFG["EPIC"] or pos.get("direction") != "BUY": continue
        if _is_ours(pos): return pos
        if pos["dealId"] not in st.d.get("foreign_warned", []):
            st.d["foreign_warned"] = (st.d.get("foreign_warned", []) + [pos["dealId"]])[-50:]; st.save()
            log.warning("OBCA pozycja BUY %s na %s (otwarta %s, size %s) - bot jej NIE zarządza i nie otworzy własnej, dopóki istnieje",
                        pos["dealId"], CFG["EPIC"], pos.get("createdDateUTC"), pos.get("size"))
    return None

def _any_epic_long():
    return any(p["market"].get("epic") == CFG["EPIC"] and p["position"].get("direction") == "BUY" for p in api.positions())

def supervisor():
    """Wątek nadzorcy: keep-alive sesji, time-stop / CLOSE_BY, wykrycie zamknięcia przez TP/SL, księgowanie wyniku."""
    last_ping = 0; closed_market_until = 0
    while True:
        try:
            if time.time() - last_ping > 300:
                api.ping(); last_ping = time.time()
            with st.lock:
                pos = st.d["position"]
                broker = bot_position_ours()
                if pos is None and broker is not None:          # odbudowa stanu po restarcie (tylko własna pozycja)
                    opened = _opened_at(broker) or now_local()
                    st.d["position"] = dict(deal_id=broker["dealId"], level=float(broker["level"]), size=float(broker["size"]),
                                            open_time=str(opened), deadline=str(deadline_for(opened)),
                                            sl=broker.get("stopLevel"), tp=broker.get("profitLevel"))
                    st.save(); log.warning("odbudowano stan pozycji z brokera: %s", st.d["position"]); pos = st.d["position"]
                if pos is not None:
                    if broker is None or broker["dealId"] != pos["deal_id"]:   # zamknięta przez TP/SL u brokera
                        _settle(pos, reason="TP/SL brokera")
                    elif now_local() >= datetime.fromisoformat(pos["deadline"]) and time.time() >= closed_market_until:
                        if CFG["ARMED"]:
                            try:
                                api.close(pos["deal_id"]); log.info("TIME-STOP: zamknięto %s", pos["deal_id"])
                                _settle(pos, reason="time-stop/CLOSE_BY", upl=broker.get("upl"))
                            except RuntimeError as e:
                                if "currently closed" in str(e) or "closed" in str(e).lower():
                                    closed_market_until = time.time() + 600
                                    log.warning("rynek zamknięty - ponowię zamknięcie %s za 10 min", pos["deal_id"])
                                else: raise
                        else:
                            log.info("TRYB SUCHY: minął termin pozycji %s (nie zamykam, ARMED=false)", pos["deal_id"])
                            _settle(pos, reason="time-stop (tryb suchy, bez zlecenia)", upl=broker.get("upl"))
        except Exception as e:
            log.error("nadzorca: %s", e)
        time.sleep(CFG["SUPERVISOR_SEC"])

def _settle(pos, reason, upl=None):
    """Księguje zamkniętą pozycję (przybliżony wynik z ostatniej wyceny upl, jeśli brak - 0) i czyści stan."""
    pnl = float(upl) if upl is not None else 0.0
    st.d["day_pnl"] = float(st.d.get("day_pnl", 0.0)) + pnl
    st.d["trades"] = (st.d["trades"] + [dict(pos, closed=str(now_local()), reason=reason, pnl_est=pnl)])[-200:]
    st.d["position"] = None; st.save()
    log.info("ZAMKNIĘTO (%s) deal=%s wynik≈%.2f (dzień: %.2f)", reason, pos["deal_id"], pnl, st.d["day_pnl"])
    return pnl

# ----------------------------------------------------------------------------- HTTP
def _auth(payload):
    sec = CFG["WEBHOOK_SECRET"]
    return bool(sec) and hmac.compare_digest(str(payload.get("secret", "")), sec)

@app.get("/")
@app.get("/health")
def health():
    return jsonify(ok=True, bot="J225 5m LONG", env=CFG["CAPITAL_ENV"], armed=CFG["ARMED"], time=str(now_local()))

@app.get("/status")
def status():
    try: balance, available = api.balance()
    except Exception as e: balance, available = None, f"broker niedostępny: {e}"
    return jsonify(env=CFG["CAPITAL_ENV"], armed=CFG["ARMED"], trading_enabled=CFG["TRADING_ENABLED"], halted=st.d["halted"],
                   halt_reason=st.d["halt_reason"], balance=balance, available=available, position=st.d["position"],
                   day_pnl=st.d["day_pnl"], last_signal=st.d["last_signal"], trades=st.d["trades"][-10:],
                   guard=f"{CFG['GUARD_START']}-{CFG['GUARD_END']} {CFG['TZ']}", tp_pct=CFG["TP_PCT"], sl_pct=CFG["SL_PCT"],
                   time_stop_min=CFG["TIME_STOP_MIN"], close_by=CFG["CLOSE_BY"], risk_fraction=CFG["RISK_FRACTION"])

@app.post("/webhook")
def webhook():
    log.info("PRZYSZEDŁ WEBHOOK z %s, %d bajtów", request.headers.get("X-Forwarded-For", request.remote_addr), len(request.data or b""))
    payload = request.get_json(silent=True) or {}
    if not payload:
        try: payload = json.loads(request.data.decode("utf-8"))
        except Exception:
            log.error("webhook: treść nie jest JSON-em: %s", (request.data or b"")[:200])
            return jsonify(ok=False, reason="brak JSON"), 400
    if not _auth(payload): log.warning("webhook: zły sekret"); return jsonify(ok=False, reason="unauthorized"), 401
    try: res = handle_signal(payload)
    except Exception as e:
        log.exception("webhook: błąd"); return jsonify(ok=False, reason=str(e)), 500
    log.info("webhook -> %s", res); return jsonify(res)

@app.get("/accounts")
def list_accounts():
    """Lista rachunków w bieżącym środowisku (demo/live). Wymaga nagłówka X-Secret = WEBHOOK_SECRET."""
    if not _auth({"secret": request.headers.get("X-Secret", "")}): return jsonify(ok=False, reason="unauthorized"), 401
    try:
        accs = api.accounts()
        return jsonify(env=CFG["CAPITAL_ENV"], accounts=[dict(accountId=a.get("accountId"), name=a.get("accountName"), currency=a.get("currency"),
                       balance=a.get("balance", {}).get("balance"), available=a.get("balance", {}).get("available"), preferred=a.get("preferred")) for a in accs])
    except Exception as e:
        return jsonify(ok=False, reason=str(e)), 500

@app.post("/close")
def manual_close():
    payload = request.get_json(silent=True) or {}
    if not _auth(payload): return jsonify(ok=False, reason="unauthorized"), 401
    with st.lock:
        pos = st.d["position"] or ({"deal_id": bot_position_ours()["dealId"]} if bot_position_ours() else None)
        if not pos: return jsonify(ok=False, reason="brak pozycji")
        if CFG["ARMED"]: api.close(pos["deal_id"])
        _settle(pos if "level" in pos else dict(pos, level=None, size=None, open_time=None, deadline=None, sl=None, tp=None), "ręczne zamknięcie")
    return jsonify(ok=True)

@app.post("/halt")
def halt():
    payload = request.get_json(silent=True) or {}
    if not _auth(payload): return jsonify(ok=False, reason="unauthorized"), 401
    st.d["halted"] = bool(payload.get("halted", True)); st.d["halt_reason"] = payload.get("reason", "ręczne"); st.save()
    return jsonify(ok=True, halted=st.d["halted"])

# ----------------------------------------------------------------------------- start
def _selftest():
    """python app.py --selftest : sprawdza logikę czasu i wielkości bez sieci."""
    t = datetime(2026, 9, 7, 3, 10, tzinfo=TZ)
    assert in_guard(t) and not in_guard(t.replace(hour=9)) and not in_guard(t.replace(hour=1, minute=59))
    assert deadline_for(t) == t + timedelta(minutes=240)
    assert deadline_for(t.replace(hour=6)) == t.replace(hour=9, minute=0)
    mkt = {"instrument": {"marginFactor": 5, "marginFactorUnit": "PERCENTAGE", "currency": "JPY"},
           "dealingRules": {"minDealSize": {"value": 0.1}}, "snapshot": {"offer": 66000.0, "bid": 65990.0}}
    size, info = compute_size(218.59, mkt, 148.0)
    print("selftest OK | saldo 218.59 USD -> depozyt", info["margin_usd"], "USD, nominał", info["notional_usd"],
          "USD, size", size, "(surowe", info["raw"], ")")

def startup_banner():
    log.info("=" * 70)
    log.info("BOT J225 5m LONG | env=%s | rachunek=%s | ARMED=%s | TRADING_ENABLED=%s",
             CFG["CAPITAL_ENV"], CFG["CAPITAL_ACCOUNT_ID"] or "(domyślny)", CFG["ARMED"], CFG["TRADING_ENABLED"])
    log.info("okno wejść %s-%s %s | TP %.2f%% | SL %.2f%% | time-stop %d min | zamknięcie %s | depozyt %.0f%% salda",
             CFG["GUARD_START"], CFG["GUARD_END"], CFG["TZ"], CFG["TP_PCT"], CFG["SL_PCT"],
             CFG["TIME_STOP_MIN"], CFG["CLOSE_BY"], CFG["RISK_FRACTION"] * 100)
    log.info("adres dla TradingView: <adres-usługi>/webhook   (sprawdzenie stanu: /status)")
    if not CFG["WEBHOOK_SECRET"]:
        log.critical("UWAGA: WEBHOOK_SECRET jest PUSTY - bot odrzuci KAŻDY sygnał (401). Ustaw zmienną w Render.")
    if not CFG["ARMED"]:
        log.warning("UWAGA: ARMED=false - sygnały będą tylko logowane (tryb suchy), bez składania zleceń.")
    if not CFG["TRADING_ENABLED"]:
        log.warning("UWAGA: TRADING_ENABLED=false - nowe wejścia wstrzymane.")
    if st.d.get("halted"):
        log.warning("UWAGA: bot zatrzymany (%s) - wznów przez /halt z halted:false.", st.d.get("halt_reason"))
    log.info("=" * 70)

def keepalive():
    """Samo-ping publicznego adresu co 10 min - zapobiega usypianiu usługi na planie Free Render."""
    url = CFG["KEEPALIVE_URL"].rstrip("/") + "/"
    while True:
        time.sleep(600)
        try: requests.get(url, timeout=8)
        except Exception as e: log.debug("keepalive: %s", e)

startup_banner()
threading.Thread(target=supervisor, daemon=True).start()
if CFG["KEEPALIVE_URL"]:
    threading.Thread(target=keepalive, daemon=True).start()
    log.info("keep-alive włączony: %s co 10 min", CFG["KEEPALIVE_URL"])
else:
    log.warning("keep-alive wyłączony. Na planie Free usługa zaśnie po ~15 min i webhook z TradingView PRZEPADNIE. "
                "Ustaw KEEPALIVE_URL=<adres usługi> lub monitor UptimeRobot co 5 min.")
if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv: _selftest()
    else: app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
