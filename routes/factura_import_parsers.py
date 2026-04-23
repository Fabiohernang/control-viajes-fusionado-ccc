import re
import numbers
from datetime import datetime, date
from decimal import Decimal

from pypdf import PdfReader
from utils import to_decimal, quantize_money


def _empty(v):
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in ("nan", "nat", "none")


def _vals(row):
    return [x for x in row.tolist() if not _empty(x)]


def _txt(row):
    return " ".join(str(x) for x in _vals(row))


def _is_num(v):
    return isinstance(v, numbers.Number) and not isinstance(v, bool)


def _dec(v):
    return to_decimal(str(v), "0")


def _date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    return None


def _read_excel(file_storage):
    import pandas as pd
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".xls"):
        return pd.read_excel(file_storage, header=None, engine="xlrd")
    return pd.read_excel(file_storage, header=None)


def parse_factura_archivo(file_storage):
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        return parse_factura_excel(file_storage)
    return parse_factura_pdf_basico(file_storage)


def parse_factura_excel(file_storage):
    df = _read_excel(file_storage)

    data = {
        "numero_factura": "",
        "fecha": "",
        "fecha_vencimiento": "",
        "cliente": "",
        "cliente_numero": "",
        "cuit_cliente": "",
        "condicion_pago": "",
        "subtotal": "0",
        "iva": "0",
        "percepciones": "0",
        "impuesto": "0",
        "total": "0",
        "items": [],
        "cantidad_items": 0,
    }

    for _, row in df.iterrows():
        text = _txt(row)
        upper = text.upper()
        vals = _vals(row)

        if not data["numero_factura"]:
            m = re.search(r"\d{4}-\d{8}", text)
            if m:
                data["numero_factura"] = m.group(0)

        if not data["fecha"] and "FECHA" in upper:
            for v in vals:
                d = _date(v)
                if d:
                    data["fecha"] = d.isoformat()
                    break

        if not data["fecha_vencimiento"] and "VENCIMIENTO" in upper:
            for v in vals:
                d = _date(v)
                if d:
                    data["fecha_vencimiento"] = d.isoformat()
                    break

        if not data["cliente"] and "SEÑOR" in upper and vals:
            data["cliente"] = str(vals[-1]).strip()

        if not data["cliente_numero"] and "CLIENTE" in upper and "N" in upper and vals:
            nums = [x for x in vals if _is_num(x)]
            if nums:
                data["cliente_numero"] = str(nums[-1])

        if not data["cuit_cliente"]:
            m = re.search(r"\d{2}-\d{8}-\d", text)
            if m:
                data["cuit_cliente"] = m.group(0)

        if not data["condicion_pago"] and "PAGO" in upper and vals:
            data["condicion_pago"] = str(vals[-1]).strip()

        nums = [x for x in vals if _is_num(x)]
        if nums:
            last = str(quantize_money(_dec(nums[-1])))
            if "SUBTOTAL" in upper:
                data["subtotal"] = last
            elif "I.V.A" in upper or "IVA" in upper:
                data["iva"] = last
            elif "PERC" in upper or "IIBB" in upper:
                data["percepciones"] = last
            elif re.search(r"\bTOTAL\b", upper):
                data["total"] = last

    # Items: una fila contiene la descripción del viaje; el CTG puede estar en las filas siguientes.
    item_regex = re.compile(
        r"Socio\s+(.*?),\s*desde\s+(.*?)\s+hasta\s+(.*?)\s+\((\d+)km\.?\)\s+([\d\.,]+)\s+kg de\s+([A-ZÁÉÍÓÚÑ ]+)\.\s*Tarifa\s*\$?\s*([\d\.,]+)",
        re.I,
    )

    for i in range(len(df)):
        row = df.iloc[i]
        text = _txt(row)
        if "Socio" not in text:
            continue

        m = item_regex.search(text)
        if not m:
            continue

        fletero, origen, destino, km, kg_raw, producto, tarifa_raw = m.groups()
        vals = _vals(row)
        nums = [x for x in vals if _is_num(x)]
        importe = _dec(nums[-1]) if nums else Decimal("0")

        # Buscar CTG en la misma fila y hasta 5 filas siguientes.
        ctg = ""
        window = []
        for j in range(i, min(i + 6, len(df))):
            window.append(_txt(df.iloc[j]))
        window_text = " ".join(window)

        m_ctg = re.search(r"CTG\s*[:\-]?\s*(\d{8,})", window_text, re.I)
        if not m_ctg:
            # fallback: primer número largo que no sea CUIT/factura, usualmente CTG
            largos = re.findall(r"\b\d{8,}\b", window_text)
            if largos:
                ctg = largos[-1]
        else:
            ctg = m_ctg.group(1)

        kg_dec = _dec(kg_raw.replace(".", "").replace(",", "."))
        tarifa_dec = _dec(tarifa_raw.replace(".", "").replace(",", "."))

        data["items"].append({
            "fletero": fletero.strip(),
            "socio": True,
            "origen": origen.strip(),
            "destino": destino.strip(),
            "kilometros": int(km),
            "kg": str(quantize_money(kg_dec / Decimal("1000"))),
            "kg_bruto": str(kg_dec),
            "producto": producto.strip(),
            "tarifa": str(tarifa_dec),
            "ctg": ctg,
            "importe_total": str(quantize_money(importe)),
        })

    data["cantidad_items"] = len(data["items"])

    # Si no detectó total, lo calcula con subtotal + IVA + percepciones.
    if _dec(data["total"]) == 0:
        total_calc = _dec(data["subtotal"]) + _dec(data["iva"]) + _dec(data["percepciones"])
        if total_calc == 0 and data["items"]:
            # último fallback: suma de importes netos detectados
            total_calc = sum((_dec(x.get("importe_total", "0")) for x in data["items"]), Decimal("0"))
        data["total"] = str(quantize_money(total_calc))

    if not data["fecha_vencimiento"] and data["fecha"]:
        try:
            base = datetime.strptime(data["fecha"], "%Y-%m-%d").date()
            data["fecha_vencimiento"] = base.replace(day=base.day).isoformat()
        except Exception:
            pass

    return data


def parse_factura_pdf_basico(file_storage):
    # Fallback PDF simple: deja que el sistema no se rompa si suben PDF.
    reader = PdfReader(file_storage)
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    numero = re.search(r"\d{4}-\d{8}", text)
    return {
        "numero_factura": numero.group(0) if numero else "",
        "fecha": "",
        "fecha_vencimiento": "",
        "cliente": "",
        "cliente_numero": "",
        "cuit_cliente": "",
        "condicion_pago": "",
        "subtotal": "0",
        "iva": "0",
        "percepciones": "0",
        "impuesto": "0",
        "total": "0",
        "items": [],
        "cantidad_items": 0,
    }
