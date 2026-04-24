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


def _normalize(value):
    return " ".join((value or "").replace("\xa0", " ").split())


def _dec(value):
    value = _normalize(str(value))
    if not value:
        return Decimal("0")
    if "," in value:
        value = value.replace(".", "").replace(",", ".")
    elif value.count(".") > 1:
        parts = value.split(".")
        value = "".join(parts[:-1]) + "." + parts[-1]
    return to_decimal(value, "0")


def _read_excel(file_storage):
    import pandas as pd
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".xls"):
        return pd.read_excel(file_storage, header=None, engine="xlrd")
    return pd.read_excel(file_storage, header=None)


def _looks_like_ctg(value):
    digits = re.sub(r"\D", "", str(value))
    if len(digits) < 8 or len(digits) > 14:
        return False
    # Evita agarrar importes chicos o números de factura. Los CTG reales suelen empezar 10, 22, etc.
    if digits.startswith("0007"):
        return False
    return True


def _clean_ctg(value):
    return re.sub(r"\D", "", str(value))


def parse_liquidacion_archivo(file_storage):
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".pdf"):
        return parse_liquidacion_pdf_robusto(file_storage)
    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        return parse_liquidacion_excel(file_storage)
    raise ValueError("Formato no soportado. Usá Excel 8 (.xls), .xlsx o PDF.")


def _extraer_cabecera(text):
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
            data["total_bruto"] = quantize_money(max(_dec(x) for x in monies))
        except Exception:
            pass
    return data


def _parse_info(info_text):
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


def _make_item(fecha="", nro_viaje="", ctg="", kg="0", tarifa="0", kms="0", importe="0", producto="", origen="", destino="", cliente="", chofer="", fletero=""):
    return {
        "fecha": fecha,
        "nro_viaje": nro_viaje,
        "ctg": ctg,
        "kg": str(kg),
        "tarifa": str(tarifa),
        "kilometros": str(kms),
        "importe": str(importe),
        "importe_total": str(importe),
        "producto": producto,
        "mercaderia": producto,
        "origen": origen,
        "destino": destino,
        "cliente": cliente,
        "fletero": fletero or chofer,
        "chofer": chofer,
    }


def parse_liquidacion_excel(file_storage):
    df = _read_excel(file_storage)
    data = {"numero": "", "fecha": "", "fletero": "", "total_bruto": Decimal("0"), "items": []}

    for _, row in df.iterrows():
        vals = _values(row)
        row_text = _text(row)
        if not data["numero"]:
            m = re.search(r"\d{4}-\d{8}", row_text)
            if m:
                data["numero"] = m.group(0)
        if not data["fecha"] and "Fecha" in row_text:
            for v in vals:
                d = _parse_date(v)
                if d:
                    data["fecha"] = d.isoformat()
                    break
        if not data["fletero"] and "Nombre" in row_text and len(vals) >= 2:
            data["fletero"] = str(vals[-1]).strip()
        if "Subtotal" in row_text:
            nums = [x for x in vals if _is_number(x)]
            if nums:
                data["total_bruto"] = quantize_money(_dec(nums[-1]))

    seen = set()
    for i in range(len(df)):
        vals = _values(df.iloc[i])
        if not vals:
            continue
        fecha = None
        for v in vals:
            fecha = _parse_date(v)
            if fecha:
                break
        if not fecha:
            continue

        # CTG real en la misma fila.
        ctg = ""
        nro_viaje = ""
        for v in vals:
            if _is_number(v):
                n = int(v)
                s = str(n)
                if _looks_like_ctg(s):
                    ctg = s
                    break
                if not nro_viaje and 1000 <= n <= 999999:
                    nro_viaje = s
            else:
                s = _clean_ctg(v)
                if _looks_like_ctg(s):
                    ctg = s
                    break
        if not ctg or ctg in seen:
            continue
        seen.add(ctg)

        strings = [str(x).strip() for x in vals if isinstance(x, str) and str(x).strip()]
        nums = [x for x in vals if _is_number(x)]
        # Números después del CTG: normalmente kg, tarifa, kms, importe.
        ctg_pos = next((idx for idx, x in enumerate(vals) if _clean_ctg(x) == ctg), -1)
        after = vals[ctg_pos + 1:] if ctg_pos >= 0 else vals
        after_nums = [x for x in after if _is_number(x)]
        kg = _dec(after_nums[0]) if len(after_nums) > 0 else Decimal("0")
        tarifa = _dec(after_nums[1]) if len(after_nums) > 1 else Decimal("0")
        kms = _dec(after_nums[2]) if len(after_nums) > 2 else Decimal("0")
        importe = _dec(after_nums[3]) if len(after_nums) > 3 else Decimal("0")

        origen = ""
        destino = ""
        if len(strings) >= 1:
            origen = strings[0]
        if len(strings) >= 2:
            destino = " ".join(strings[1:])

        info_line = _text(df.iloc[i + 1]) if i + 1 < len(df) else ""
        cliente, chofer, producto = _parse_info(info_line)
        data["items"].append(_make_item(fecha.isoformat(), nro_viaje, ctg, kg, tarifa, kms, importe, producto, origen, destino, cliente, chofer, data["fletero"]))

    print("=== LIQUIDACION EXCEL === items:", len(data["items"]))
    return data


def parse_liquidacion_pdf_robusto(file_storage):
    reader = PdfReader(file_storage)
    texts = []
    for page in reader.pages:
        try:
            texts.append(page.extract_text(extraction_mode="layout") or "")
        except Exception:
            texts.append(page.extract_text() or "")
    full = "\n".join(texts)
    lines = [_normalize(x) for x in full.splitlines() if _normalize(x)]
    cab = _extraer_cabecera("\n".join(lines))
    items = []
    seen = set()

    # Ventana de 3 líneas: muchas veces la fila de tabla queda partida.
    for i in range(len(lines)):
        window = " ".join(lines[i:min(i + 3, len(lines))])
        fechas = re.findall(r"\b\d{1,2}/\d{1,2}/\d{4}\b", window)
        if not fechas:
            continue
        ctgs = [_clean_ctg(x) for x in re.findall(r"\b\d[\d\.]{7,}\b", window)]
        ctgs = [x for x in ctgs if _looks_like_ctg(x)]
        if not ctgs:
            continue
        for ctg in ctgs:
            if ctg in seen:
                continue
            seen.add(ctg)
            cliente, chofer, producto = _parse_info(window)
            origen = "CAMPO" if "CAMPO" in window.upper() else ("PLANTA" if "PLANTA" in window.upper() else "")
            # intenta tomar números cercanos antes/después, si no quedan cero
            nums = re.findall(r"\d[\d\.,]*", window)
            importe = Decimal("0")
            if nums:
                money_like = [n for n in nums if "," in n]
                if money_like:
                    try:
                        importe = _dec(money_like[-1])
                    except Exception:
                        importe = Decimal("0")
            items.append(_make_item(fechas[-1], "", ctg, "0", "0", "0", importe, producto, origen, "", cliente, chofer, cab.get("fletero", "")))

    print("=== LIQUIDACION PDF === items:", len(items))
    if not items:
        print("Primeras lineas PDF:")
        print("\n".join(lines[:120]))

    return {"numero": cab.get("numero", ""), "fecha": cab.get("fecha", ""), "fletero": cab.get("fletero", ""), "total_bruto": cab.get("total_bruto", Decimal("0")), "items": items}
