import re
from datetime import datetime, date
from decimal import Decimal

from utils import to_decimal, quantize_money


def _is_empty(value):
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() == "nan"


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
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _number_to_str(value):
    if _is_number(value):
        return str(int(value))
    return str(value).strip()


def _read_excel(file_storage):
    import pandas as pd
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".xls"):
        return pd.read_excel(file_storage, header=None, engine="xlrd")
    return pd.read_excel(file_storage, header=None)


def parse_liquidacion_archivo(file_storage):
    filename = (getattr(file_storage, "filename", "") or "").lower()

    if filename.endswith(".pdf"):
        from services.ccc_service import parse_liquidacion_pdf
        return parse_liquidacion_pdf(file_storage)

    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        return parse_liquidacion_excel(file_storage)

    raise ValueError("Formato no soportado. Usá Excel 8 (.xls), .xlsx o PDF.")


def parse_liquidacion_excel(file_storage):
    df = _read_excel(file_storage)

    data = {
        "numero": "",
        "fecha": "",
        "fletero": "",
        "total_bruto": Decimal("0"),
        "items": [],
    }

    # Cabecera y totales
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
            # En el Excel real aparece algo como: Nombre | 5326 | Lauman Luis Alberto
            data["fletero"] = str(vals[-1]).strip()

        if "Subtotal" in row_text:
            nums = [x for x in vals if _is_number(x)]
            if nums:
                data["total_bruto"] = quantize_money(to_decimal(nums[-1]))

    # Viajes: el formato real usa bloques:
    # fila técnica: origen, destino, kilos, tarifa, kms, importe
    # fila viaje: fecha, nro viaje, CTG
    # fila info: Cliente / Chofer / Mercadería
    for i in range(1, len(df) - 1):
        row = df.iloc[i]
        vals = _values(row)
        if not vals:
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

        nro_viaje = None
        ctg = None
        for number in numeric_vals:
            text_num = str(int(number))
            if len(text_num) >= 8:
                ctg = text_num
                break
            if nro_viaje is None and 1000 <= int(number) <= 999999:
                nro_viaje = int(number)

        if not ctg:
            continue

        prev_vals = _values(df.iloc[i - 1])
        next_vals = _values(df.iloc[i + 1])

        prev_texts = [str(x).strip() for x in prev_vals if isinstance(x, str) and str(x).strip()]
        prev_nums = [x for x in prev_vals if _is_number(x)]

        origen = ""
        destino = ""
        if len(prev_texts) >= 1:
            origen = prev_texts[0]
        if len(prev_texts) >= 2:
            destino = prev_texts[1]

        kg = Decimal("0")
        tarifa = Decimal("0")
        kms = Decimal("0")
        importe = Decimal("0")

        if len(prev_nums) >= 1:
            kg = to_decimal(prev_nums[0])
        if len(prev_nums) >= 2:
            tarifa = to_decimal(prev_nums[1])
        if len(prev_nums) >= 3:
            kms = to_decimal(prev_nums[2])
        if len(prev_nums) >= 4:
            importe = to_decimal(prev_nums[3])

        info_text = " ".join(str(x) for x in next_vals)
        cliente = ""
        chofer = ""
        producto = ""

        if "Cliente:" in info_text:
            cliente = info_text.split("Cliente:", 1)[1].split("Chofer:", 1)[0].strip()
        if "Chofer:" in info_text:
            chofer = info_text.split("Chofer:", 1)[1].split("Mercadería:", 1)[0].strip()
        if "Mercadería:" in info_text:
            producto = info_text.split("Mercadería:", 1)[1].strip()

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
            "origen": origen,
            "destino": destino,
            "cliente": cliente,
            "fletero": data["fletero"] or chofer,
            "chofer": chofer,
        })

    if not data["items"]:
        print("DEBUG parser liquidacion Excel: no se detectaron viajes")
        print("Shape:", df.shape)
        print(df.head(30).to_string())

    return data
