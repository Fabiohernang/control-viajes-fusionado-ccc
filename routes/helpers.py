"""
Helpers compartidos entre blueprints.
Se importan desde aquí para evitar importar app.py directamente.
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
    Viaje, FleteroMaster, Productor
)
from utils import to_decimal, quantize_money


# -------------------------
# AUTENTICACIÓN
# -------------------------

def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("auth.login"))
        return view_func(*args, **kwargs)
    return wrapper


# -------------------------
# CONFIGURACIÓN
# -------------------------

def get_config_decimal(key, default):
    item = db.session.get(AppConfig, key)
    return to_decimal(item.value if item else default)


# -------------------------
# MAESTROS
# -------------------------

def upsert_maestro(model, nombre):
    nombre = (nombre or "").strip()
    if not nombre:
        return None
    existente = model.query.filter(func.lower(model.nombre) == nombre.lower()).first()
    if not existente:
        existente = model(nombre=nombre)
        db.session.add(existente)
    return existente


def buscar_tarifa_por_km(km_value):
    km_decimal = to_decimal(km_value, "0")
    if km_decimal <= 0:
        return None
    km_int = int(km_decimal)
    exacta = Tarifario.query.filter(Tarifario.km == km_int).first()
    if exacta:
        return exacta
    menor = Tarifario.query.filter(Tarifario.km <= km_int).order_by(Tarifario.km.desc()).first()
    mayor = Tarifario.query.filter(Tarifario.km >= km_int).order_by(Tarifario.km.asc()).first()
    if not menor:
        return mayor
    if not mayor:
        return menor
    diff_menor = km_int - menor.km
    diff_mayor = mayor.km - km_int
    return menor if diff_menor <= diff_mayor else mayor


# -------------------------
# FACTURAS
# -------------------------

def actualizar_estado_factura(factura):
    saldo = to_decimal(factura.importe_total) - factura.total_aplicado
    if saldo < 0:
        saldo = Decimal("0")
    if saldo == 0:
        factura.estado_pago = "pagada"
    elif factura.total_aplicado > 0:
        factura.estado_pago = "parcial"
    else:
        factura.estado_pago = "pendiente"


def sincronizar_factura_por_numero(numero_factura):
    if not numero_factura:
        return
    factura = Factura.query.filter_by(numero_factura=numero_factura).first()
    if factura:
        actualizar_estado_factura(factura)
        db.session.flush()


# -------------------------
# LIQUIDACIONES
# -------------------------

def recalcular_liquidacion(liquidacion):
    items = LiquidacionItem.query.filter_by(liquidacion_id=liquidacion.id).all()
    descuentos = LiquidacionDescuento.query.filter_by(liquidacion_id=liquidacion.id).all()

    subtotal = sum((to_decimal(i.importe) for i in items), Decimal("0"))
    total_descuentos = sum((to_decimal(d.importe) for d in descuentos), Decimal("0"))
    total = subtotal - total_descuentos

    pagos = LiquidacionPago.query.filter_by(liquidacion_id=liquidacion.id).all()
    total_pagado = sum((to_decimal(p.importe) for p in pagos), Decimal("0"))

    if hasattr(liquidacion, "subtotal"):
        liquidacion.subtotal = quantize_money(subtotal)
    if hasattr(liquidacion, "total"):
        liquidacion.total = quantize_money(total)
    if hasattr(liquidacion, "saldo"):
        liquidacion.saldo = quantize_money(total - total_pagado)

    # Campos reales del modelo actual
    liquidacion.total_bruto = quantize_money(subtotal)
    liquidacion.total_descuentos = quantize_money(total_descuentos)
    liquidacion.neto_pagar = quantize_money(total)

    saldo_pendiente = total - total_pagado
    if saldo_pendiente <= 0:
        liquidacion.estado = "pagada"
    elif total_pagado > 0:
        liquidacion.estado = "parcial"
    else:
        liquidacion.estado = "pendiente"


# -------------------------
# VIAJES
# -------------------------

def hydrate_viaje(viaje, form):
    fecha_raw = form.get("fecha", "")
    viaje.fecha = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()
    viaje.cliente = form.get("cliente", "").strip()
    viaje.factura = form.get("factura", "").strip() or None
    viaje.fletero = form.get("fletero", "").strip()
    viaje.socio = form.get("socio") == "si"
    viaje.ctg = form.get("ctg", "").strip() or None
    viaje.origen = form.get("origen", "").strip() or None
    viaje.destino = form.get("destino", "").strip() or None
    viaje.producto = form.get("producto", "").strip() or None
    viaje.kilometros = to_decimal(form.get("kilometros", "0"))

    tarifa_manual = form.get("tarifa", "").strip()
    usar_tarifario = form.get("usar_tarifario") == "si"

    if usar_tarifario and viaje.kilometros and to_decimal(viaje.kilometros) > 0:
        match = buscar_tarifa_por_km(viaje.kilometros)
        if match:
            viaje.tarifa = to_decimal(match.tarifa_tn)
        else:
            viaje.tarifa = to_decimal(tarifa_manual, "0")
    else:
        viaje.tarifa = to_decimal(tarifa_manual, "0")

    viaje.descuento = to_decimal(form.get("descuento", "0"))
    viaje.kg = to_decimal(form.get("kg", "0"))
    viaje.liquidado = form.get("liquidado") == "si"
    viaje.observaciones = form.get("observaciones", "").strip() or None


# -------------------------
# PARSERS / TEXTO
# -------------------------
def parse_date_safe(value):
    value = (value or "").strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def normalize_spaces(value):
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def normalize_title_keep_upper(value):
    value = normalize_spaces(value)
    if not value:
        return value
    return " ".join(word if word.isupper() else word.capitalize() for word in value.split())


def parse_local_decimal(value, default="0"):
    value = normalize_spaces(value)
    if not value:
        return Decimal(default)
    cleaned = value.replace(".", "").replace(",", ".")
    return to_decimal(cleaned, default)


def extract_first_match(pattern, text, flags=0, group=1, default=None):
    match = re.search(pattern, text, flags)
    if not match:
        return default
    return match.group(group).strip()


def parse_amount_from_lines(lines, keyword):
    keyword_upper = keyword.upper()

    for line in lines:
        clean = " ".join(line.strip().split())
        upper = clean.upper()

        if keyword_upper in upper:
            matches = re.findall(r"(\d{1,3}(?:\.\d{3})*,\d{2})", clean)
            if matches:
                return parse_local_decimal(matches[-1])

    return Decimal("0")


# -------------------------
# EXCEL HELPERS
# -------------------------
def _read_excel(file_storage):
    import pandas as pd
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".xls"):
        return pd.read_excel(file_storage, header=None, engine="xlrd")
    return pd.read_excel(file_storage, header=None)


def _is_nan_like(value):
    if value is None:
        return True
    sval = str(value).strip()
    return sval == "" or sval.lower() == "nan"


def _row_values(row):
    return [x for x in row.tolist() if not _is_nan_like(x)]


def _row_text(row):
    return " ".join(str(x) for x in _row_values(row))


def _parse_date_excel(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    sval = str(value).strip()
    try:
        return datetime.fromisoformat(sval[:10]).date()
    except Exception:
        pass
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(sval[:10], fmt).date()
        except Exception:
            pass
    return None


# -------------------------
# LIQUIDACIÓN: PDF O EXCEL
# -------------------------
def parse_liquidacion_archivo(file_storage):
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".pdf"):
        from services.ccc_service import parse_liquidacion_pdf
        return parse_liquidacion_pdf(file_storage)
    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        return parse_liquidacion_excel(file_storage)
    raise ValueError("Formato no soportado para liquidación. Usá PDF o Excel.")


def parse_liquidacion_excel(file_storage):
    df = _read_excel(file_storage)

    data = {
        "numero": "",
        "fecha": "",
        "fletero": "",
        "total_bruto": Decimal("0"),
        "items": []
    }

    for _, row in df.iterrows():
        text = _row_text(row)
        if not data["numero"]:
            m = re.search(r"\d{4}-\d{8}", text)
            if m:
                data["numero"] = m.group(0)

        if not data["fecha"] and "Fecha" in text:
            for v in row:
                d = _parse_date_excel(v)
                if d:
                    data["fecha"] = d.isoformat()
                    break

        if not data["fletero"] and "Nombre" in text:
            vals = _row_values(row)
            if vals:
                data["fletero"] = str(vals[-1]).strip()

        if "Subtotal" in text:
            nums = [x for x in _row_values(row) if isinstance(x, (int, float))]
            if nums:
                data["total_bruto"] = quantize_money(to_decimal(nums[-1]))

    for i in range(1, len(df) - 1):
        current_vals = _row_values(df.iloc[i])
        if not current_vals:
            continue

        fecha = _parse_date_excel(current_vals[0])
        if not fecha:
            continue

        nro_viaje = None
        ctg = None
        for v in current_vals:
            if isinstance(v, (int, float)):
                if nro_viaje is None:
                    nro_viaje = int(v)
                elif len(str(int(v))) >= 8:
                    ctg = str(int(v))
                    break

        if not ctg:
            continue

        prev_vals = _row_values(df.iloc[i - 1])
        next_vals = _row_values(df.iloc[i + 1])

        prev_texts = [x for x in prev_vals if isinstance(x, str)]
        prev_nums = [x for x in prev_vals if isinstance(x, (int, float))]

        origen = str(prev_texts[0]).strip() if len(prev_texts) > 0 else ""
        destino = str(prev_texts[1]).strip() if len(prev_texts) > 1 else ""
        kg = prev_nums[0] if len(prev_nums) > 0 else 0
        tarifa = prev_nums[1] if len(prev_nums) > 1 else 0
        kms = prev_nums[2] if len(prev_nums) > 2 else 0
        importe = prev_nums[3] if len(prev_nums) > 3 else 0

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
            "producto": producto,
            "origen": origen,
            "destino": destino,
            "cliente": cliente,
            "fletero": data["fletero"] or chofer,
            "chofer": chofer,
            "importe_total": str(importe),
        })

    return data


def parse_factura_pdf(file_storage):
    filename = (getattr(file_storage, "filename", "") or "").lower()
    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        return parse_factura_excel(file_storage)

    reader = PdfReader(file_storage)
    layout_pages = [page.extract_text(extraction_mode="layout") or "" for page in reader.pages]
    raw_pages = [page.extract_text() or "" for page in reader.pages]

    layout_text = "\n".join(layout_pages)
    compact_text = normalize_spaces(" ".join(raw_pages))
    layout_lines = [line.strip() for line in layout_text.splitlines() if line.strip()]

    numero_factura = extract_first_match(r"N[º°]\s*([0-9]{4}-[0-9]{8})", layout_text)
    fecha_factura = parse_date_safe(
        extract_first_match(r"FECHA:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", layout_text, default="")
    )
    fecha_vencimiento = parse_date_safe(
        extract_first_match(r"Fecha de Vencimiento\s*:?[ ]*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", layout_text, default="")
    )

    cliente = normalize_spaces(
        extract_first_match(r"SEÑOR/ES:\s*(.*?)\s*Cliente Nº:", layout_text, flags=re.S, default="")
    )
    cliente_numero = extract_first_match(r"Cliente Nº:\s*([0-9\.]+)", layout_text, default="")
    cuit_cliente = extract_first_match(r"([0-9]{2}-[0-9]{8}-[0-9])", layout_text, default="")
    condicion_pago = extract_first_match(
        r"Condición de Pago:\s*(.*?)\s*Fecha de Vencimiento", layout_text, flags=re.S, default=""
    )

    subtotal = parse_amount_from_lines(layout_lines, "Subtotal")
    iva = parse_amount_from_lines(layout_lines, "I.V.A. INSC %")
    percepciones = parse_amount_from_lines(layout_lines, "PERC. IIBB")
    impuesto = parse_amount_from_lines(layout_lines, "IMPUESTO")

    total = Decimal("0")
    for line in layout_lines:
        clean = " ".join(line.strip().split())
        upper = clean.upper()
        if upper.startswith("TOTAL") or "TOTAL $" in upper:
            matches = re.findall(r"(\d{1,3}(?:\.\d{3})*,\d{2})", clean)
            if matches:
                total = parse_local_decimal(matches[-1])
                break

    if total == 0:
        total = subtotal + iva + percepciones + impuesto

    item_pattern = re.compile(r"1,00\s+([\d\.,]+)\s+([\d\.,]+)\s*(Socio .*?)CTG:\s*(\d+)", re.S)
    detail_pattern = re.compile(
        r"Socio\s+(.*?),\s*desde\s+(.*?)\s+hasta\s+(.*?)\s+\((\d+)km\.\)\s+([\d\.]+)\s+kg de\s+([A-ZÁÉÍÓÚÑ ]+)\.\s*Tarifa\s*\$([\d\.]+)",
        re.I
    )

    items = []
    for match in item_pattern.finditer(compact_text):
        importe_total = parse_local_decimal(match.group(2))
        body = normalize_spaces(match.group(3))
        ctg = match.group(4)
        detail = detail_pattern.search(body)
        if not detail:
            continue

        fletero_raw, origen, destino, km, kg_raw, producto, tarifa_raw = detail.groups()
        fletero = normalize_title_keep_upper(fletero_raw.replace("Socio ", "").strip())
        origen = normalize_title_keep_upper(origen)
        destino = normalize_title_keep_upper(destino)
        producto = normalize_title_keep_upper(producto)
        kg_decimal = to_decimal(kg_raw, "0")
        kg_tn = quantize_money(kg_decimal / Decimal("1000"))
        tarifa = to_decimal(tarifa_raw, "0")

        items.append({
            "fletero": fletero,
            "socio": True,
            "origen": origen,
            "destino": destino,
            "kilometros": int(km),
            "kg": str(kg_tn),
            "kg_bruto": kg_raw,
            "producto": producto,
            "tarifa": str(tarifa),
            "ctg": ctg,
            "importe_total": str(importe_total),
        })

    parsed = {
        "numero_factura": numero_factura or "",
        "fecha": fecha_factura.isoformat() if fecha_factura else "",
        "fecha_vencimiento": fecha_vencimiento.isoformat() if fecha_vencimiento else "",
        "cliente": cliente,
        "cliente_numero": cliente_numero,
        "cuit_cliente": cuit_cliente,
        "condicion_pago": condicion_pago,
        "subtotal": str(quantize_money(subtotal)),
        "iva": str(quantize_money(iva)),
        "percepciones": str(quantize_money(percepciones + impuesto)),
        "impuesto": str(quantize_money(impuesto)),
        "total": str(quantize_money(total)),
        "items": items,
        "cantidad_items": len(items),
    }

    if not parsed["numero_factura"] or not parsed["cliente"] or not parsed["fecha"]:
        raise ValueError("No se pudieron detectar correctamente los datos principales de la factura.")

    return parsed


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
        text = _row_text(row)
        vals = _row_values(row)

        if not data["numero_factura"]:
            m = re.search(r"\d{4}-\d{8}", text)
            if m:
                data["numero_factura"] = m.group(0)

        if not data["fecha"] and "FECHA" in text.upper():
            for v in row:
                d = _parse_date_excel(v)
                if d:
                    data["fecha"] = d.isoformat()
                    break

        if not data["fecha_vencimiento"] and "Vencimiento" in text:
            for v in row:
                d = _parse_date_excel(v)
                if d:
                    data["fecha_vencimiento"] = d.isoformat()
                    break

        if not data["cliente"] and "SEÑOR/ES" in text.upper() and vals:
            data["cliente"] = str(vals[-1]).strip()

        if not data["cliente_numero"] and "Cliente Nº" in text and vals:
            data["cliente_numero"] = str(vals[-1]).strip()

        if not data["cuit_cliente"] and "C.U.I.T." in text and vals:
            last = str(vals[-1]).strip()
            if re.match(r"\d{2}-\d{8}-\d", last):
                data["cuit_cliente"] = last

        if not data["condicion_pago"] and "Condición de Pago" in text and vals:
            if len(vals) >= 2:
                data["condicion_pago"] = str(vals[1]).strip()

        if "Subtotal" in text:
            nums = [x for x in vals if isinstance(x, (int, float))]
            if nums:
                data["subtotal"] = str(nums[-1])

        if "I.V.A. INSC" in text.upper():
            nums = [x for x in vals if isinstance(x, (int, float))]
            if nums:
                data["iva"] = str(nums[-1])

        if "PERC. IIBB" in text.upper():
            nums = [x for x in vals if isinstance(x, (int, float))]
            if nums:
                data["percepciones"] = str(nums[-1])

        if re.search(r"\bTOTAL\b", text.upper()):
            nums = [x for x in vals if isinstance(x, (int, float))]
            if nums:
                data["total"] = str(nums[-1])

    for i in range(len(df) - 1):
        row = df.iloc[i]
        text = _row_text(row)
        if "Socio" not in text:
            continue

        next_text = _row_text(df.iloc[i + 1])
        m = re.search(r"Socio\s+(.*?),\s+desde\s+(.*?)\s+hasta\s+(.*?)\s+\((\d+)km\.\)\s+([\d\.]+)\s+kg de\s+([A-ZÁÉÍÓÚÑ ]+)\.\s+Tarifa\s*\$([\d\.]+)", text, re.I)
        if not m:
            continue

        fletero, origen, destino, km, kg_raw, producto, tarifa_raw = m.groups()
        vals = _row_values(row)
        nums = [x for x in vals if isinstance(x, (int, float))]
        importe = nums[-1] if nums else 0
        ctg = ""
        m_ctg = re.search(r"CTG[: ]+(\d+)", next_text)
        if m_ctg:
            ctg = m_ctg.group(1)

        data["items"].append({
            "fletero": fletero.strip(),
            "socio": True,
            "origen": origen.strip(),
            "destino": destino.strip(),
            "kilometros": int(km),
            "kg": str(quantize_money(to_decimal(kg_raw, "0") / Decimal("1000"))),
            "kg_bruto": kg_raw,
            "producto": producto.strip(),
            "tarifa": str(to_decimal(tarifa_raw, "0")),
            "ctg": ctg,
            "importe_total": str(importe),
        })

    data["cantidad_items"] = len(data["items"])
    return data


def crear_factura_y_viajes_desde_importacion(data, crear_viajes=True):
    numero_factura = (data.get("numero_factura") or "").strip()
    if Factura.query.filter_by(numero_factura=numero_factura).first():
        raise ValueError(f"La factura {numero_factura} ya existe.")

    fecha_factura = parse_date_safe(data.get("fecha")) or date.today()
    fecha_vencimiento = parse_date_safe(data.get("fecha_vencimiento")) or (fecha_factura + timedelta(days=20))
    cliente = (data.get("cliente") or "").strip()

    upsert_maestro(Productor, cliente)

    factura = Factura(
        numero_factura=numero_factura,
        fecha=fecha_factura,
        fecha_vencimiento=fecha_vencimiento,
        cliente=cliente,
        importe_neto=quantize_money(to_decimal(data.get("subtotal", "0"))),
        iva=quantize_money(to_decimal(data.get("iva", "0"))),
        percepciones=quantize_money(to_decimal(data.get("percepciones", "0"))),
        importe_total=quantize_money(to_decimal(data.get("total", "0"))),
        estado_pago="pendiente",
        observaciones=f"Importada desde archivo. Cliente N° {data.get('cliente_numero') or '-'}",
    )
    db.session.add(factura)

    if crear_viajes:
        iva_rate = get_config_decimal("iva", "0.21")
        socio_rate = get_config_decimal("comision_socio", "0.06")
        no_socio_rate = get_config_decimal("comision_no_socio", "0.10")
        lucas_rate = get_config_decimal("comision_lucas", "0.015")

        for item in data.get("items", []):
            fletero = (item.get("fletero") or "").strip()
            if fletero:
                upsert_maestro(FleteroMaster, fletero)

            viaje = Viaje(
                fecha=fecha_factura,
                cliente=cliente,
                factura=numero_factura,
                fletero=fletero,
                socio=bool(item.get("socio", True)),
                ctg=(item.get("ctg") or "").strip() or None,
                origen=(item.get("origen") or "").strip() or None,
                destino=(item.get("destino") or "").strip() or None,
                producto=(item.get("producto") or "").strip() or None,
                kilometros=to_decimal(item.get("kilometros", "0")),
                tarifa=to_decimal(item.get("tarifa", "0")),
                descuento=Decimal("0"),
                kg=to_decimal(item.get("kg", "0")),
                liquidado=False,
                observaciones=f"Importado desde archivo | Kg factura: {item.get('kg_bruto') or '-'}",
            )
            viaje.recalcular(
                iva=iva_rate,
                socio_rate=socio_rate,
                no_socio_rate=no_socio_rate,
                lucas_rate=lucas_rate,
            )
            db.session.add(viaje)

    db.session.commit()
    sincronizar_factura_por_numero(numero_factura)
    db.session.commit()

    return factura


def parse_tarifario_text(texto):
    registros = []
    errores = []

    lineas = texto.splitlines()
    for i, linea in enumerate(lineas, start=1):
        raw = linea.strip()
        if not raw:
            continue

        raw = raw.replace("\t", "=").replace(";", "=").replace(":", "=")
        if "=" not in raw:
            errores.append(f"Línea {i}: formato inválido. Usá km=tarifa")
            continue

        km_str, tarifa_str = raw.split("=", 1)
        km_str = km_str.strip()
        tarifa_str = tarifa_str.strip().replace(".", "").replace(",", ".")

        if not km_str.isdigit():
            errores.append(f"Línea {i}: km inválido")
            continue

        try:
            km = int(km_str)
            tarifa = Decimal(tarifa_str)
        except Exception:
            errores.append(f"Línea {i}: tarifa inválida")
            continue

        registros.append((km, tarifa))

    return registros, errores


# -------------------------
# ESTADÍSTICAS
# -------------------------
def get_monthly_stats(year: int, month: int):
    viajes_mes = Viaje.query.filter(
        func.extract("year", Viaje.fecha) == year,
        func.extract("month", Viaje.fecha) == month,
    )

    cantidad_viajes = viajes_mes.count()
    cantidad_liquidados = viajes_mes.filter(Viaje.liquidado.is_(True)).count()

    total_facturado = viajes_mes.with_entities(
        func.coalesce(func.sum(Viaje.total_importe), 0)
    ).scalar() or 0

    total_liquidado = viajes_mes.filter(Viaje.liquidado.is_(True)).with_entities(
        func.coalesce(func.sum(Viaje.total_importe), 0)
    ).scalar() or 0

    total_comisiones = viajes_mes.with_entities(
        func.coalesce(func.sum(Viaje.comision), 0)
    ).scalar() or 0

    total_comision_lucas = viajes_mes.with_entities(
        func.coalesce(func.sum(Viaje.comision_lucas), 0)
    ).scalar() or 0

    return {
        "cantidad_viajes": cantidad_viajes,
        "cantidad_liquidados": cantidad_liquidados,
        "total_facturado": total_facturado,
        "total_liquidado": total_liquidado,
        "total_comisiones": total_comisiones,
        "total_comision_lucas": total_comision_lucas,
    }
