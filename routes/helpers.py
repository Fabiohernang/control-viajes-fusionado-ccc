"""
Helpers compartidos entre blueprints.
"""
import re
from functools import wraps
from datetime import datetime, date, timedelta
from decimal import Decimal

from flask import session, redirect, url_for
from sqlalchemy import func
from pypdf import PdfReader

from extensions import db
from models import (
    AppConfig, Tarifario, Factura, PagoAplicacion,
    LiquidacionFletero, LiquidacionItem, LiquidacionDescuento, LiquidacionPago,
    Viaje, FleteroMaster
)
from utils import to_decimal, quantize_money

# =========================
# UTIL EXCEL
# =========================

def _read_excel(file_storage):
    import pandas as pd
    name = file_storage.filename.lower()
    if name.endswith(".xls"):
        return pd.read_excel(file_storage, header=None, engine="xlrd")
    return pd.read_excel(file_storage, header=None)


def _row_values(row):
    return [x for x in row.tolist() if str(x).strip() and str(x) != "nan"]


def _row_text(row):
    return " ".join([str(x) for x in _row_values(row)])


def _parse_date_excel(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value)
    try:
        return datetime.fromisoformat(s[:10]).date()
    except:
        return None

# =========================
# PARSER LIQUIDACION EXCEL
# =========================

def parse_liquidacion_archivo(file_storage):
    name = file_storage.filename.lower()

    if name.endswith(".pdf"):
        from services.ccc_service import parse_liquidacion_pdf
        return parse_liquidacion_pdf(file_storage)

    df = _read_excel(file_storage)

    data = {
        "numero": None,
        "fecha": None,
        "fletero": None,
        "total_bruto": 0,
        "items": []
    }

    # cabecera
    for _, row in df.iterrows():
        text = _row_text(row)

        if not data["numero"]:
            m = re.search(r"\d{4}-\d{8}", text)
            if m:
                data["numero"] = m.group(0)

        if "Fecha" in text and not data["fecha"]:
            for v in row:
                d = _parse_date_excel(v)
                if d:
                    data["fecha"] = d
                    break

        if "Nombre" in text and not data["fletero"]:
            vals = _row_values(row)
            if len(vals) > 1:
                data["fletero"] = vals[-1]

        if "Subtotal" in text:
            nums = [x for x in _row_values(row) if isinstance(x, (int, float))]
            if nums:
                data["total_bruto"] = float(nums[-1])

    # items
    for i in range(1, len(df) - 1):
        row = df.iloc[i]
        vals = _row_values(row)

        fecha = _parse_date_excel(vals[0]) if vals else None
        if not fecha:
            continue

        nro_viaje = None
        ctg = None

        for v in vals:
            if isinstance(v, (int, float)):
                if not nro_viaje:
                    nro_viaje = int(v)
                elif not ctg and len(str(int(v))) >= 8:
                    ctg = str(int(v))

        if not ctg:
            continue

        prev = _row_values(df.iloc[i - 1])
        next_row = _row_values(df.iloc[i + 1])

        textos = [x for x in prev if isinstance(x, str)]
        nums = [x for x in prev if isinstance(x, (int, float))]

        origen = textos[0] if len(textos) > 0 else None
        destino = textos[1] if len(textos) > 1 else None

        kg = nums[0] if len(nums) > 0 else 0
        tarifa = nums[1] if len(nums) > 1 else 0
        kms = nums[2] if len(nums) > 2 else 0
        importe = nums[3] if len(nums) > 3 else 0

        cliente = None
        producto = None

        txt = " ".join([str(x) for x in next_row])

        if "Cliente:" in txt:
            cliente = txt.split("Cliente:")[1].split("Chofer")[0].strip()

        if "Mercadería:" in txt:
            producto = txt.split("Mercadería:")[1].strip()

        data["items"].append({
            "ctg": ctg,
            "kg": kg,
            "importe": importe,
            "producto": producto,
            "origen": origen,
            "destino": destino,
            "cliente": cliente
        })

    return data

# =========================
# PARSER FACTURA (PDF + EXCEL)
# =========================

def parse_factura_pdf(file_storage):
    name = file_storage.filename.lower()

    if name.endswith(".xls") or name.endswith(".xlsx"):
        return _parse_factura_excel(file_storage)

    return _parse_factura_pdf_original(file_storage)


def _parse_factura_excel(file_storage):
    df = _read_excel(file_storage)

    data = {
        "numero_factura": None,
        "fecha": None,
        "fecha_vencimiento": None,
        "cliente": None,
        "subtotal": "0",
        "iva": "0",
        "percepciones": "0",
        "total": "0",
        "items": []
    }

    for i, row in df.iterrows():
        text = _row_text(row)

        if not data["numero_factura"]:
            m = re.search(r"\d{4}-\d{8}", text)
            if m:
                data["numero_factura"] = m.group(0)

        if "FECHA" in text and not data["fecha"]:
            for v in row:
                d = _parse_date_excel(v)
                if d:
                    data["fecha"] = d.isoformat()

        if "Vencimiento" in text and not data["fecha_vencimiento"]:
            for v in row:
                d = _parse_date_excel(v)
                if d:
                    data["fecha_vencimiento"] = d.isoformat()

        if "SEÑOR" in text and not data["cliente"]:
            vals = _row_values(row)
            if len(vals) > 1:
                data["cliente"] = vals[-1]

        if "Subtotal" in text:
            nums = [x for x in _row_values(row) if isinstance(x, (int, float))]
            if nums:
                data["subtotal"] = str(nums[-1])

        if "I.V.A" in text:
            nums = [x for x in _row_values(row) if isinstance(x, (int, float))]
            if nums:
                data["iva"] = str(nums[-1])

        if "TOTAL" in text:
            nums = [x for x in _row_values(row) if isinstance(x, (int, float))]
            if nums:
                data["total"] = str(nums[-1])

    for i in range(len(df) - 1):
        row = df.iloc[i]
        text = _row_text(row)

        if "Socio" not in text:
            continue

        next_row = _row_text(df.iloc[i + 1])

        m = re.search(r"Socio (.*?), desde (.*?) hasta (.*?) \((\d+)km", text)
        if not m:
            continue

        fletero, origen, destino, km = m.groups()

        ctg = None
        if "CTG" in next_row:
            m2 = re.search(r"CTG[: ]+(\d+)", next_row)
            if m2:
                ctg = m2.group(1)

        nums = [x for x in _row_values(row) if isinstance(x, (int, float))]
        importe = nums[-1] if nums else 0

        data["items"].append({
            "fletero": fletero,
            "origen": origen,
            "destino": destino,
            "kilometros": int(km),
            "kg": "0",
            "kg_bruto": "0",
            "producto": None,
            "tarifa": "0",
            "ctg": ctg,
            "importe_total": str(importe),
        })

    data["cantidad_items"] = len(data["items"])

    return data


# =========================
# ORIGINAL PDF (SIN CAMBIOS)
# =========================

def _parse_factura_pdf_original(file_storage):
    reader = PdfReader(file_storage)
    layout_pages = [page.extract_text(extraction_mode="layout") or "" for page in reader.pages]
    raw_pages = [page.extract_text() or "" for page in reader.pages]

    layout_text = "\n".join(layout_pages)
    compact_text = " ".join(raw_pages)

    numero_factura = re.search(r"\d{4}-\d{8}", layout_text)

    return {
        "numero_factura": numero_factura.group(0) if numero_factura else "",
        "cliente": "",
        "fecha": "",
        "fecha_vencimiento": "",
        "subtotal": "0",
        "iva": "0",
        "percepciones": "0",
        "total": "0",
        "items": [],
        "cantidad_items": 0
    }
