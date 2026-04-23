import re
import numbers
from datetime import datetime, date
from decimal import Decimal

from pypdf import PdfReader
from utils import to_decimal, quantize_money


def _is_empty(value):
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() in ("nan", "nat", "none")


def _values(row):
    return [x for x in row.tolist() if not _is_empty(x)]


def _text(row):
    return " ".join(str(x) for x in _values(row))


def _parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except Exception:
            pass
    return None


def _is_number(value):
    return isinstance(value, numbers.Number) and not isinstance(value, bool)


def _num_to_decimal(value):
    return to_decimal(str(value), "0")


def _normalize(value):
    return " ".join((value or "").replace("\xa0", " ").split())


def _parse_local_decimal(value):
    value = _normalize(str(value))
    if not value:
        return Decimal("0")
    if "," in value:
        return to_decimal(value.replace(".", "").replace(",", "."), "0")
    if value.count(".") > 1:
        parts = value.split(".")
        value = "".join(parts[:-1]) + "." + parts[-1]
    return to_decimal(value, "0")


def _read_excel(file_storage):
    import pandas as pd
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".xls"):
        return pd.read_excel(file_storage, header=None, engine="xlrd")
    return pd.read_excel(file_storage, header=None)


def parse_liquidacion_archivo(file_storage):
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".pdf"):
        return parse_liquidacion_pdf_robusto(file_storage)
    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        return parse_liquidacion_excel(file_storage)
    raise ValueError("Formato no soportado. Usá Excel 8 (.xls), .xlsx o PDF.")


def _extraer_cabecera_desde_texto(text):
    data = {"numero": "", "fecha": "", "fletero": "", "total_bruto": Decimal("0")}
    m = re.search(r"\b\d{4}-\d{8}\b", text)
    if m:
        data["numero"] = m.group(0)
    fechas = re.findall(r"\b\d{1,2}/\d{1,2}/\d{4}\b", text)
    if fechas:
        data["fecha"] = fechas[0]
    m_nombre = re.search(r"Nombre\s+\d+\s+([A-Za-zÁÉÍÓÚáéíóúÑñ ]+?)(?:\s+Domicilio|\s+Localidad|\s+Tipo|\n|$)", text, re.I)
    if m_nombre:
        data["fletero"] = _normalize(m_nombre.group(1)).title()
    monies = re.findall(r"\d{1,3}(?:\.\d{3})*,\d{2}", text)
    if monies:
        try:
            data["total_bruto"] = quantize_money(max(_parse_local_decimal(x) for x in monies))
        except Exception:
            pass
    return data


def _parse_info_line(info_text):
    info_text = _normalize(info_text)
    cliente = ""
    chofer = ""
    producto = ""
    if "Cliente:" in info_text:
        cliente = info_text.split("Cliente:", 1)[1].split("Chofer:", 1)[0].strip()
    if "Chofer:" in info_text:
        chofer = info_text.split("Chofer:", 1)[1].split("Mercadería:", 1)[0].split("Mercaderia:", 1)[0].strip()
    m_prod = re.search(r"Mercad[eé]r[ií]a:\s*([A-Za-zÁÉÍÓÚÑáéíóúñ ]+)", info_text, re.I)
    if m_prod:
        producto = _normalize(m_prod.group(1)).upper()
    return cliente, chofer, producto


def _fallback_items_from_text(lines, cab):
    """Último recurso: crea viajes con fecha y CTG aunque falten campos.
    Esto permite cruzar por CTG, que es lo crítico del control.
    """
    items = []
    seen = set()
    for idx, line in enumerate(lines):
        context = " ".join(lines[max(0, idx-1):min(len(lines), idx+3)])
        fechas = re.findall(r"\b\d{1,2}/\d{1,2}/\d{4}\b", context)
        ctgs = re.findall(r"\b\d{8,}\b", context)
        if not ctgs:
            continue
        for ctg in ctgs:
            if ctg in seen:
                continue
            # Evitar confundir número de factura 00070000... demasiado corto? se acepta solo si hay fecha cerca.
            if not fechas:
                continue
            seen.add(ctg)
            cliente, chofer, producto = _parse_info_line(context)
            origen = "CAMPO" if "CAMPO" in context.upper() else ("PLANTA" if "PLANTA" in context.upper() else "")
            destino = ""
            items.append({
                "fecha": fechas[-1],
                "nro_viaje": "",
                "ctg": ctg,
                "kg": "0",
                "tarifa": "0",
                "kilometros": "0",
                "importe": "0",
                "importe_total": "0",
                "producto": producto,
                "mercaderia": producto,
                "origen": origen,
                "destino": destino,
                "cliente": cliente,
                "fletero": cab.get("fletero") or chofer,
                "chofer": chofer,
            })
    return items


def parse_liquidacion_pdf_robusto(file_storage):
    reader = PdfReader(file_storage)
    raw_text = "\n".join(page.extract_text() or "" for page in reader.pages)
    try:
        layout_text = "\n".join(page.extract_text(extraction_mode="layout") or "" for page in reader.pages)
    except Exception:
        layout_text = raw_text
    text = layout_text if len(layout_text) > len(raw_text) * 0.8 else raw_text
    lines = [_normalize(x) for x in text.splitlines() if _normalize(x)]
    full_text = "\n".join(lines)
    cab = _extraer_cabecera_desde_texto(full_text)
    items = []

    for idx, line in enumerate(lines):
        if not re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", line):
            continue
        if not re.search(r"\b\d{8,}\b", line):
            continue
        fecha_match = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", line)
        fecha = fecha_match.group(0) if fecha_match else ""
        nums = re.findall(r"\b\d[\d\.,]*\b", line)
        ctg_candidates = [n for n in nums if len(re.sub(r"\D", "", n)) >= 8]
        if not ctg_candidates:
            continue
        ctg = re.sub(r"\D", "", ctg_candidates[-1])
        cliente, chofer, producto = _parse_info_line(lines[idx + 1] if idx + 1 < len(lines) else "")
        items.append({"fecha": fecha, "nro_viaje": "", "ctg": ctg, "kg": "0", "tarifa": "0", "kilometros": "0", "importe": "0", "importe_total": "0", "producto": producto, "mercaderia": producto, "origen": "", "destino": "", "cliente": cliente, "fletero": cab.get("fletero") or chofer, "chofer": chofer})

    if not items:
        for i in range(len(lines) - 1):
            m_dest = re.match(r"^(.*?)\s+(\d{1,2}/\d{1,2}/\d{4})\s+00:00:00$", lines[i])
            if not m_dest:
                continue
            fecha = m_dest.group(2)
            detail = lines[i + 1]
            ctgs = re.findall(r"\b\d{8,}\b", detail)
            if not ctgs:
                continue
            ctg = ctgs[-1]
            cliente, chofer, producto = _parse_info_line(detail)
            items.append({"fecha": fecha, "nro_viaje": "", "ctg": ctg, "kg": "0", "tarifa": "0", "kilometros": "0", "importe": "0", "importe_total": "0", "producto": producto, "mercaderia": producto, "origen": "CAMPO" if "CAMPO" in detail.upper() else "", "destino": _normalize(m_dest.group(1)), "cliente": cliente, "fletero": cab.get("fletero") or chofer, "chofer": chofer})

    if not items:
        items = _fallback_items_from_text(lines, cab)

    print("=== LIQUIDACION PDF ROBUSTO === items:", len(items))
    if not items:
        print("\n".join(lines[:100]))
    return {"numero": cab.get("numero", ""), "fecha": cab.get("fecha", ""), "fletero": cab.get("fletero", ""), "total_bruto": cab.get("total_bruto", Decimal("0")), "items": items}


def parse_liquidacion_excel(file_storage):
    df = _read_excel(file_storage)
    data = {"numero": "", "fecha": "", "fletero": "", "total_bruto": Decimal("0"), "items": []}
    all_lines = []
    for _, row in df.iterrows():
        row_text = _text(row)
        if row_text:
            all_lines.append(row_text)
        vals = _values(row)
        if not data["numero"]:
            match = re.search(r"\d{4}-\d{8}", row_text)
            if match:
                data["numero"] = match.group(0)
        if not data["fecha"] and "Fecha" in row_text:
            for value in vals:
                parsed = _parse_date(value)
                if parsed:
                    data["fecha"] = parsed.isoformat()
                    break
        if not data["fletero"] and "Nombre" in row_text and len(vals) >= 2:
            data["fletero"] = str(vals[-1]).strip()

    cab = {"fletero": data["fletero"]}
    data["items"] = _fallback_items_from_text(all_lines, cab)
    return data
