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
    # Caso raro: 28.440.01 debería ser 28440.01
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

    # Fletero: intenta Nombre <codigo> <nombre> o después de /CUIT.
    m_nombre = re.search(r"Nombre\s+\d+\s+([A-Za-zÁÉÍÓÚáéíóúÑñ ]+?)(?:\s+Domicilio|\s+Localidad|\s+Tipo|\n|$)", text, re.I)
    if m_nombre:
        data["fletero"] = _normalize(m_nombre.group(1)).title()
    else:
        m_cuit = re.search(r"/CUIT\s*([A-Za-zÁÉÍÓÚáéíóúÑñ ]+?)\s+\d{2}-", text, re.I)
        if m_cuit:
            data["fletero"] = _normalize(m_cuit.group(1)).title()

    # Total bruto/subtotal: busca Subtotal o el mayor importe monetario cerca del final.
    m_sub = re.search(r"Subtotal\s+([\d\.]+,\d{2})", text, re.I)
    if m_sub:
        data["total_bruto"] = quantize_money(_parse_local_decimal(m_sub.group(1)))
    else:
        monies = re.findall(r"\d{1,3}(?:\.\d{3})*,\d{2}", text)
        if monies:
            valores = [_parse_local_decimal(x) for x in monies]
            data["total_bruto"] = quantize_money(max(valores))

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

    # Formato A: tabla en una línea con fecha + nro viaje + CTG + origen + destino + números.
    for idx, line in enumerate(lines):
        if not re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", line):
            continue
        if not re.search(r"\b\d{8,}\b", line):
            continue

        nums = re.findall(r"\b\d[\d\.,]*\b", line)
        largos = [n for n in nums if len(re.sub(r"\D", "", n)) >= 8]
        if not largos:
            continue

        fecha_match = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", line)
        fecha = fecha_match.group(0) if fecha_match else ""
        ctg = re.sub(r"\D", "", largos[0])

        # Después del CTG suele venir: origen destino kilos tarifa kms importe
        after_ctg = line.split(largos[0], 1)[1] if largos[0] in line else line
        money_nums = re.findall(r"\d[\d\.,]*", after_ctg)

        kilos = tarifa = kms = importe = Decimal("0")
        if len(money_nums) >= 4:
            kilos = _parse_local_decimal(money_nums[-4])
            tarifa = _parse_local_decimal(money_nums[-3])
            kms = _parse_local_decimal(money_nums[-2])
            importe = _parse_local_decimal(money_nums[-1])

        before_nums = re.split(r"\s+\d[\d\.,]*", after_ctg, maxsplit=1)[0]
        words = before_nums.split()
        origen = words[0] if words else ""
        destino = " ".join(words[1:]) if len(words) > 1 else ""

        info_line = lines[idx + 1] if idx + 1 < len(lines) else ""
        cliente, chofer, producto = _parse_info_line(info_line)

        items.append({
            "fecha": fecha,
            "nro_viaje": "",
            "ctg": ctg,
            "kg": str(kilos),
            "tarifa": str(tarifa),
            "kilometros": str(kms),
            "importe": str(importe),
            "importe_total": str(importe),
            "producto": producto,
            "mercaderia": producto,
            "origen": origen,
            "destino": destino,
            "cliente": cliente,
            "fletero": cab.get("fletero") or chofer,
            "chofer": chofer,
        })

    # Formato B: destino + fecha en una línea; detalle completo en la siguiente.
    if not items:
        for i in range(len(lines) - 1):
            m_dest = re.match(r"^(.*?)\s+(\d{1,2}/\d{1,2}/\d{4})\s+00:00:00$", lines[i])
            if not m_dest:
                continue
            destino = _normalize(m_dest.group(1))
            fecha = m_dest.group(2)
            detail = lines[i + 1]
            m_ctg = re.search(r"(\d{8,})\s*$", detail)
            if not m_ctg:
                continue
            ctg = m_ctg.group(1)
            tokens = re.findall(r"\d[\d\.,]*", detail)
            if len(tokens) < 6:
                continue
            kilos = _parse_local_decimal(tokens[-6])
            tarifa = _parse_local_decimal(tokens[-5])
            kms = _parse_local_decimal(tokens[-4])
            importe = _parse_local_decimal(tokens[-3])
            nro_viaje = tokens[-2]
            cliente, chofer, producto = _parse_info_line(detail)

            origen = ""
            upper = detail.upper()
            if "CAMPO" in upper:
                origen = "CAMPO"
            elif "PLANTA" in upper:
                origen = "PLANTA"
            elif "ACOPIO" in upper:
                origen = "ACOPIO"

            items.append({
                "fecha": fecha,
                "nro_viaje": nro_viaje,
                "ctg": ctg,
                "kg": str(kilos),
                "tarifa": str(tarifa),
                "kilometros": str(kms),
                "importe": str(importe),
                "importe_total": str(importe),
                "producto": producto,
                "mercaderia": producto,
                "origen": origen,
                "destino": destino,
                "cliente": cliente,
                "fletero": cab.get("fletero") or chofer,
                "chofer": chofer,
            })

    parsed = {
        "numero": cab.get("numero", ""),
        "fecha": cab.get("fecha", ""),
        "fletero": cab.get("fletero", ""),
        "total_bruto": cab.get("total_bruto", Decimal("0")),
        "items": items,
    }

    print("=== LIQUIDACION PDF ROBUSTO ===")
    print("items:", len(items))
    if not items:
        print("Primeras lineas:")
        print("\n".join(lines[:80]))

    return parsed


def parse_liquidacion_excel(file_storage):
    df = _read_excel(file_storage)

    data = {
        "numero": "",
        "fecha": "",
        "fletero": "",
        "total_bruto": Decimal("0"),
        "items": [],
    }

    for _, row in df.iterrows():
        row_text = _text(row)
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

        if "Subtotal" in row_text:
            nums = [x for x in vals if _is_number(x)]
            if nums:
                data["total_bruto"] = quantize_money(_num_to_decimal(nums[-1]))

    for i in range(1, len(df) - 1):
        vals = _values(df.iloc[i])
        if not vals:
            continue

        row_text = " ".join(str(x) for x in vals)
        if not re.search(r"\d{8,}", row_text):
            continue

        fecha = None
        for value in vals:
            fecha = _parse_date(value)
            if fecha:
                break
        if not fecha:
            continue

        numeric_vals = [x for x in vals if _is_number(x)]
        if len(numeric_vals) < 2:
            continue

        nro_viaje = ""
        ctg = ""
        for number in numeric_vals:
            number_int = int(number)
            text_num = str(number_int)
            if len(text_num) >= 8:
                ctg = text_num
                break
            if not nro_viaje and 1000 <= number_int <= 999999:
                nro_viaje = str(number_int)

        if not ctg:
            continue

        prev_vals = _values(df.iloc[i - 1])
        next_vals = _values(df.iloc[i + 1])
        prev_texts = [str(x).strip() for x in prev_vals if isinstance(x, str) and str(x).strip()]
        prev_nums = [x for x in prev_vals if _is_number(x)]

        origen = prev_texts[0] if len(prev_texts) >= 1 else ""
        destino = prev_texts[1] if len(prev_texts) >= 2 else ""
        kg = _num_to_decimal(prev_nums[0]) if len(prev_nums) >= 1 else Decimal("0")
        tarifa = _num_to_decimal(prev_nums[1]) if len(prev_nums) >= 2 else Decimal("0")
        kms = _num_to_decimal(prev_nums[2]) if len(prev_nums) >= 3 else Decimal("0")
        importe = _num_to_decimal(prev_nums[3]) if len(prev_nums) >= 4 else Decimal("0")

        cliente, chofer, producto = _parse_info_line(" ".join(str(x) for x in next_vals))

        data["items"].append({
            "fecha": fecha.isoformat(),
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
            "fletero": data["fletero"] or chofer,
            "chofer": chofer,
        })

    if not data["items"]:
        print("DEBUG parser liquidacion Excel: no se detectaron viajes")
        print("Shape:", df.shape)
        print(df.head(40).to_string())

    return data
