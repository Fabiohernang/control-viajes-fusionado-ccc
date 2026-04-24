"""
Helpers compartidos entre blueprints.
Sistema final: carga manual + liquidaciones + reportes.
"""
from functools import wraps
from datetime import datetime, date
from decimal import Decimal

from flask import session, redirect, url_for
from sqlalchemy import func

from extensions import db
from models import (
    AppConfig, Tarifario, Factura,
    LiquidacionItem, LiquidacionDescuento, LiquidacionPago,
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


# -------------------------
# TARIFARIO
# -------------------------

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


def parse_tarifario_text(texto):
    registros = []
    errores = []

    for i, linea in enumerate(texto.splitlines(), start=1):
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
            registros.append((int(km_str), Decimal(tarifa_str)))
        except Exception:
            errores.append(f"Línea {i}: tarifa inválida")

    return registros, errores


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
    pagos = LiquidacionPago.query.filter_by(liquidacion_id=liquidacion.id).all()

    subtotal = sum((to_decimal(i.importe) for i in items), Decimal("0"))
    total_descuentos = sum((to_decimal(d.importe) for d in descuentos), Decimal("0"))
    total = subtotal - total_descuentos
    if total < 0:
        total = Decimal("0")

    total_pagado = sum((to_decimal(p.importe) for p in pagos), Decimal("0"))
    saldo_pendiente = total - total_pagado

    liquidacion.total_bruto = quantize_money(subtotal)
    liquidacion.total_descuentos = quantize_money(total_descuentos)
    liquidacion.neto_pagar = quantize_money(total)

    if hasattr(liquidacion, "subtotal"):
        liquidacion.subtotal = quantize_money(subtotal)
    if hasattr(liquidacion, "total"):
        liquidacion.total = quantize_money(total)
    if hasattr(liquidacion, "saldo"):
        liquidacion.saldo = quantize_money(saldo_pendiente)

    if saldo_pendiente <= 0 and total > 0:
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
        viaje.tarifa = to_decimal(match.tarifa_tn) if match else to_decimal(tarifa_manual, "0")
    else:
        viaje.tarifa = to_decimal(tarifa_manual, "0")

    viaje.descuento = to_decimal(form.get("descuento", "0"))
    viaje.kg = to_decimal(form.get("kg", "0"))
    viaje.liquidado = form.get("liquidado") == "si"
    viaje.observaciones = form.get("observaciones", "").strip() or None


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

    total_facturado = viajes_mes.with_entities(func.coalesce(func.sum(Viaje.total_importe), 0)).scalar() or 0
    total_liquidado = viajes_mes.filter(Viaje.liquidado.is_(True)).with_entities(func.coalesce(func.sum(Viaje.total_importe), 0)).scalar() or 0
    total_comisiones = viajes_mes.with_entities(func.coalesce(func.sum(Viaje.comision), 0)).scalar() or 0
    total_comision_matias = viajes_mes.with_entities(func.coalesce(func.sum(Viaje.comision_lucas), 0)).scalar() or 0

    return {
        "cantidad_viajes": cantidad_viajes,
        "cantidad_liquidados": cantidad_liquidados,
        "total_facturado": total_facturado,
        "total_liquidado": total_liquidado,
        "total_comisiones": total_comisiones,
        "total_comision_matias": total_comision_matias,
        "total_comision_lucas": total_comision_matias,
    }
