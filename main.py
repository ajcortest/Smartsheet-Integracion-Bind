import os
import asyncio
import json
import logging
import unicodedata
import datetime as _dt
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import httpx
import smartsheet
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from smartsheet import Smartsheet
from smartsheet.models import Row, Cell
from zoneinfo import ZoneInfo  # Para zona horaria de CDMX
MX_TZ = ZoneInfo("America/Mexico_City")
"""
Smartsheet ↔ Bind ERP service
───────────────────────────────────────────
• Descarga facturas de Bind.
• Inserta NUEVAS facturas y ACTUALIZA las existentes (por UUID ‑o‑ firma compuesta).
• Job programable por fila (usando las columnas **Intervalo (minutos)**, **Ultima ejecucion** y **Siguiente ejecucion**).
  ╰─ El job global se prende / apaga con **HABILITAR_JOB**="0|false|no" en .env.

2025‑07‑04 → 2025‑07‑05  – versión consolidada con control de próxima ejecución.
Autor: @chatgpt (o3)
"""

# ════════════════════════════════════════
# 1. ENV & LOG
# ════════════════════════════════════════
load_dotenv()

SMARTSHEET_TOKEN: str | None = os.getenv("SMARTSHEET_TOKEN")
CONFIG_SHEET_ID: str | None = os.getenv("SMARTSHEET_CONFIG_ID")
BIND_TIMEOUT: int = int(os.getenv("BIND_TIMEOUT", 30))
JOB_ENABLED: bool = os.getenv("HABILITAR_JOB", "1").lower() not in {"0", "false", "no"}

if not SMARTSHEET_TOKEN:
    raise RuntimeError("Falta SMARTSHEET_TOKEN en tu .env")
if not CONFIG_SHEET_ID:
    logging.warning("No definiste SMARTSHEET_CONFIG_ID; /bind/* fallará")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("svc")

# ════════════════════════════════════════
# 2. CLIENTES & CONSTANTES
# ════════════════════════════════════════
ss: Smartsheet = smartsheet.Smartsheet(SMARTSHEET_TOKEN)
app = FastAPI(title="Smartsheet ↔ Bind ERP API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_SMART_BASE = "https://api.smartsheet.com/2.0"

# ════════════════════════════════════════
# 3. UTILS – SHEETS & FORMATO
# ════════════════════════════════════════

def sheet_to_json(sheet) -> Dict[str, Any]:
    headers = [c.title for c in sheet.columns]
    data = [
        {h: (c.display_value if c.display_value is not None else c.value) for h, c in zip(headers, r.cells)}
        for r in sheet.rows
    ]
    return {"header": headers, "data": data}


def rows_as_dict(sheet_id: int) -> List[Dict[str, Any]]:
    return sheet_to_json(ss.Sheets.get_sheet(sheet_id))["data"]


def _slug(text: str) -> str:
    return unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode().replace(" ", "").lower()


def _iso(d: str | None) -> str | None:
    if not d:
        return None
    try:
        return _dt.date.fromisoformat(d[:10]).isoformat()
    except Exception:
        return d


def _now_iso() -> str:
    """ISO-8601 en hora de CDMX, sin microsegundos."""
    return _dt.datetime.now(MX_TZ).replace(microsecond=0).isoformat()


def _parse_iso(s: str | None) -> Optional[_dt.datetime]:
    if not s:
        return None
    try:
        dt = _dt.datetime.fromisoformat(s.replace("Z", ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MX_TZ)
        else:
            dt = dt.astimezone(MX_TZ)
        return dt
    except Exception:
        return None

# ════════════════════════════════════════
# 4. BIND HELPERS
# ════════════════════════════════════════

def build_url(base: str, filtro: Optional[str]) -> str:
    if not filtro:
        return base
    clean = filtro.strip()
    if clean.startswith("?"):
        clean = clean[1:]
    if not clean.lower().startswith("$filter="):
        clean = "$filter=" + clean
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{clean}"


async def bind_get_all(base_url: str, token: str, filtro: Optional[str]) -> list[dict]:
    url = build_url(base_url, filtro)
    headers = {"Authorization": f"Bearer {token}"}
    facturas: list[dict] = []
    page = 1
    async with httpx.AsyncClient(timeout=BIND_TIMEOUT) as client:
        while url:
            log.debug(f"Bind pág {page} → {url}")
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            js = r.json()
            facturas.extend(js.get("value", []))
            url = js.get("nextLink")
            page += 1
    return facturas

# ════════════════════════════════════════
# 5. FASTAPI ENDPOINTS
# ════════════════════════════════════════

@app.get("/ping")
def ping():
    return {"status": "ok"}


@app.get("/sheet/{sheet_id}")
async def get_sheet(sheet_id: int):
    try:
        return sheet_to_json(ss.Sheets.get_sheet(sheet_id))
    except smartsheet.exceptions.ApiError as e:
        raise HTTPException(404, str(e))


@app.get("/bind/config")
def bind_config():
    try:
        data = rows_as_dict(int(CONFIG_SHEET_ID))
    except smartsheet.exceptions.ApiError as e:
        raise HTTPException(500, f"Smartsheet error: {e}")
    return {"header": list(data[0].keys()) if data else [], "data": data}


@app.get("/bind/invoices")
async def bind_invoices(id: str | None = None):
    return await _traer_facturas_por_empresas(id_fila=id)


@app.get("/bind/invoices/{empresa_id}")
async def bind_invoices_id(empresa_id: str):
    return await _traer_facturas_por_empresas(id_fila=empresa_id)


@app.post("/bind/push")
async def bind_push(background_tasks: BackgroundTasks, id: str | None = None):
    background_tasks.add_task(_traer_facturas_por_empresas, id_fila=id, push_mode=True)
    return {"status": "agendado", "id": id}

# ════════════════════════════════════════
# 6. CORE: DESCARGA Y (OPCIONAL) PUSH
# ════════════════════════════════════════

def _coerce_total(val: Any) -> float:
    try:
        return round(float(val), 2)
    except Exception:
        return 0.0


def _make_signature(date: str | None, rfc: str | None, total: Any, cfdi: Any) -> Tuple[str, str, float, str]:
    return (
        _iso(date or "") or "",
        (rfc or "").strip().upper(),
        _coerce_total(total),
        str(cfdi or "").strip(),
    )


async def _traer_facturas_por_empresas(*, id_fila: str | None = None, push_mode: bool = False):
    cfg = bind_config()["data"]
    tasks: list[asyncio.Task] = []
    etiquetas: list[str] = []
    metas: list[dict] = []

    for row in cfg:
        if id_fila and row.get("ID") != id_fila:
            continue

        token = row.get("API Token")
        base_url = row.get("API URL") or "https://api.bind.com.mx/api/Invoices"
        filtro = (row.get("Filtro") or "").strip()
        cliente = row.get("Cliente") or f"ID-{row.get('ID')}"

        if not token:
            log.warning(f"{cliente}: sin token — omitido")
            continue

        if base_url.lower().endswith("/invoices"):
            base_url = base_url[:-8] + "Invoices"
        if "/v1/" in base_url and "/api/" not in base_url:
            base_url = base_url.replace("/v1/", "/api/")

        etiquetas.append(cliente)
        metas.append({
            "dest_sheet": row.get("Hoja destino ID"),
            "rules": row.get("Reglas JSON"),
        })
        tasks.append(asyncio.create_task(bind_get_all(base_url, token, filtro)))

    resultados = await asyncio.gather(*tasks, return_exceptions=True)
    fact_total = sum(len(r) for r in resultados if isinstance(r, list))
    log.info(f"✔ Empresas procesadas: {len(etiquetas)} — Facturas totales: {fact_total}")

    if push_mode:
        for i, data in enumerate(resultados):
            if isinstance(data, Exception):
                continue
            dest_id = metas[i]["dest_sheet"]
            rules = metas[i]["rules"]
            if dest_id:
                await _push_to_sheet(dest_id, data, rules, etiquetas[i])

    return {etiquetas[i]: (res if isinstance(res, Exception) else {"count": len(res)}) for i, res in enumerate(resultados)}

# ════════════════════════════════════════
# 7. PUSH (Insert 🡢 POST / Update 🡢 PUT)
# ════════════════════════════════════════

async def _push_to_sheet(dest_sheet_id: str, facturas: list[dict], rules_json: str | None, cliente: str):
    try:
        dest_sheet = ss.Sheets.get_sheet(int(dest_sheet_id))
    except Exception as e:
        log.error(f"{cliente}: no pude abrir hoja {dest_sheet_id}: {e}")
        return

    col_map = {_slug(c.title): c.id for c in dest_sheet.columns}

    # —— reglas de mapeo ——
    default_rules = {
        "UUID": "UUID",
        "Date": "Fecha emisión",
        "RFC": "RFC Receptor",
        "Total": "Total",
        "CFDIUse": "Tipo CFDI",
    }
    try:
        rules = (json.loads(rules_json).get("map") if rules_json else {}) or default_rules
    except Exception as e:
        log.warning(f"{cliente}: reglas JSON malas ({e}); uso default")
        rules = default_rules

    slug_rules = {src: _slug(dst) for src, dst in rules.items()}

    uuid_col_id = col_map.get(slug_rules.get("UUID", ""))
    sig_cols = [
        col_map.get(slug_rules.get("Date", "")),
        col_map.get(slug_rules.get("RFC", "")),
        col_map.get(slug_rules.get("Total", "")),
        col_map.get(slug_rules.get("CFDIUse", "")),
    ]

    uuid_to_row: dict[str, int] = {}
    sig_to_row: dict[Tuple[str, str, float, str], int] = {}

    # —— indexa hoja destino ——
    for r in dest_sheet.rows:
        sig_vals: list[Any] = [None, None, None, None]
        row_uuid: Optional[str] = None
        for c in r.cells:
            if uuid_col_id and c.column_id == uuid_col_id and c.value:
                row_uuid = str(c.value).strip()
            for ix, cid in enumerate(sig_cols):
                if cid and c.column_id == cid:
                    sig_vals[ix] = c.display_value or c.value
        if row_uuid:
            uuid_to_row[row_uuid] = r.id
        sig_to_row[_make_signature(sig_vals[0], sig_vals[1], sig_vals[2], sig_vals[3])] = r.id

    inserts: list[dict] = []
    updates: list[dict] = []

    for fac in facturas:
        uuid_val = str(fac.get("UUID") or "").strip()
        cells: list[dict] = []

        fac_date = fac.get("Date")
        fac_rfc = fac.get("RFC")
        fac_total = fac.get("Total")
        fac_cfdi = fac.get("CFDIUse")
        sig_key = _make_signature(fac_date, fac_rfc, fac_total, fac_cfdi)

        for src_key, dest_col in rules.items():
            cid = col_map.get(_slug(dest_col))
            if not cid:
                continue
            val = fac.get(src_key)
            if val is None:
                continue
            if src_key.lower().startswith("date"):
                val = _iso(str(val))
            cells.append({"columnId": cid, "value": val})

        if not cells:
            continue

        row_id: Optional[int] = None
        if uuid_val:
            row_id = uuid_to_row.get(uuid_val)
        if not row_id:
            row_id = sig_to_row.get(sig_key)

        if row_id:
            updates.append({"id": row_id, "cells": cells})
        else:
            inserts.append({"toBottom": True, "cells": cells})

    if inserts:
        await _smartsheet_call("POST", f"{_SMART_BASE}/sheets/{dest_sheet_id}/rows", inserts, cliente, tag="insertadas")
    if updates:
        await _smartsheet_call(
            "PUT",
            f"{_SMART_BASE}/sheets/{dest_sheet_id}/rows",
            updates,
            cliente,
            tag="actualizadas",
        )

# ════════════════════════════════════════
# 8. Smartsheet REST Wrapper (logs & trunc)
# ════════════════════════════════════════

def _short_json(obj: Any, max_len: int = 800) -> str:
    txt = json.dumps(obj, ensure_ascii=False)
    return txt if len(txt) <= max_len else txt[:max_len] + " …"


async def _smartsheet_call(method: str, url: str, payload: list[dict], cliente: str, *, tag: str):
    headers = {
        "Authorization": f"Bearer {SMARTSHEET_TOKEN}",
        "Content-Type": "application/json",
    }
    body = json.dumps(payload, ensure_ascii=False)

    log.info(f"{cliente}: {method} {url}")
    log.debug(f"{cliente}: body {tag} len={len(payload)} → {_short_json(payload, 1200)}")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.request(method, url, content=body.encode(), headers=headers)

    if resp.status_code // 100 == 2:
        js = resp.json()
        cant = len(js.get("result", []))
        log.info(f"{cliente}: filas {tag}: {cant}")
        log.debug(f"{cliente}: respuesta {tag} → {_short_json(js, 800)}")
    else:
        log.error(f"{cliente}: ERROR {resp.status_code}: {resp.text[:800]}")

# ════════════════════════════════════════
# 9. JOB SCHEDULER (HABILITAR_JOB)
# ════════════════════════════════════════

async def _actualizar_timestamps(sheet, row, col_last, col_next, intervalo_minutos: int):
    """Actualiza 'Ultima ejecucion' y 'Siguiente ejecucion' en la fila."""
    ahora_dt = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
    ahora = ahora_dt.isoformat().replace("+00:00", "Z")
    prox_dt = ahora_dt + _dt.timedelta(minutes=intervalo_minutos)
    prox = prox_dt.isoformat().replace("+00:00", "Z")

    fila = Row()
    fila.id = row.id
    fila.cells = [
        {"columnId": col_last, "value": ahora},
        {"columnId": col_next, "value": prox}
    ]

    ss.Sheets.update_rows(sheet.id, [fila])
    log.info(
        f"📝 Actualizada última / siguiente ejecución fila {row.id} → "
        f"ahora:{ahora} prox:{prox}"
    )

async def _job_runner():
    while True:
        log.info("⏳ Revisión de ejecución programada para empresas")
        sheet = ss.Sheets.get_sheet(int(CONFIG_SHEET_ID))
        col_ids = {_slug(c.title): c.id for c in sheet.columns}
        col_int  = col_ids.get(_slug("Intervalo (minutos)"))
        col_last = col_ids.get(_slug("Ultima ejecucion"))
        col_next = col_ids.get(_slug("Siguiente ejecucion"))

        for row in sheet.rows:
            fila_id = row.cells[0].display_value if row.cells else None  # asume col 0 = ID
            cliente = next((c.display_value or c.value for c in row.cells if _slug("cliente") == _slug(sheet.columns[row.cells.index(c)].title)), fila_id)
            intervalo = 0
            ult_ejec  = None

            # extrae datos de la fila
            for c in row.cells:
                if c.column_id == col_int and c.value is not None:
                    try:
                        intervalo = int(c.value)
                    except Exception:
                        intervalo = 0
                if c.column_id == col_last:
                    ult_ejec = _parse_iso(c.value or c.display_value)

            if intervalo <= 0:
                continue

            now_aware = _dt.datetime.now(MX_TZ)
            mins_passed = ((now_aware - ult_ejec).total_seconds() / 60) if ult_ejec else None
            if mins_passed is None or mins_passed >= intervalo:
                log.info(f"⏰ Job: ejecutando para {cliente} (intervalo: {intervalo} min, última: {ult_ejec}, pasaron: {mins_passed if mins_passed is not None else '∞'} min)")
                try:
                    await _traer_facturas_por_empresas(id_fila=fila_id, push_mode=True)
                    await _actualizar_timestamps(sheet, row, col_last, col_next, intervalo)
                except Exception as exc:
                    log.error(f"Job fila {fila_id}: error {exc}")
            else:
                log.debug(f"⌛ {cliente}: aún faltan {intervalo - mins_passed:.1f} min para próxima ejecución")

        await asyncio.sleep(60)



@app.on_event("startup")
async def _startup_event():
    if JOB_ENABLED:
        log.info("🟢 Job automático ACTIVO (HABILITAR_JOB=1)")
        asyncio.create_task(_job_runner())
    else:
        log.info("🟡 Job automático DESACTIVADO (HABILITAR_JOB=0)")
