"""
Helpers compartidos entre blueprints.
Se importan desde aquí para evitar importar app.py directamente.
"""
import re
from functools import wraps
from datetime import datetime, date
from decimal import Decimal

from flask import session, redirect, url_for
from sqlalchemy import func
from pypdf import PdfReader

from extensions import db
from models import (
    AppConfig, Tarifario, Factura, PagoAplicacion,
    LiquidacionFletero, LiquidacionItem, LiquidacionDescuento, LiquidacionPago,
    Viaje,
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

    liquidacion.subtotal = quantize_money(subtotal)
    liquidacion.total_descuentos = quantize_money(total_descuentos)
    liquidacion.total = quantize_money(total)
    liquidacion.total_pagado = quantize_money(total_pagado)
    liquidacion.saldo = quantize_money(total - total_pagado)


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

def parse_factura_pdf(file_storage):
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
        observaciones=f"Importada desde PDF. Cliente N° {data.get('cliente_numero') or '-'}",
    )
    db.session.add(factura)

    if crear_viajes:
        iva_rate = get_config_decimal("iva_rate", "0.21")
        socio_rate = get_config_decimal("socio_commission_rate", "0.06")
        no_socio_rate = get_config_decimal("no_socio_commission_rate", "0.10")
        lucas_rate = get_config_decimal("lucas_commission_rate", "0.015")

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
                observaciones=f"Importado desde PDF | Kg factura: {item.get('kg_bruto') or '-'}",
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


