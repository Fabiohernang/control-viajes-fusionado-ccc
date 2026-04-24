"""
Helpers compartidos entre blueprints.
Sistema final: carga manual + liquidaciones + reportes.
"""
from functools import wraps
from datetime import datetime, date, timedelta
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


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("auth.login"))
        return view_func(*args, **kwargs)
    return wrapper


def get_config_decimal(key, default):
    item = db.session.get(AppConfig, key)
    return to_decimal(item.value if item else default)


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
    return menor if (km_int - menor.km) <= (mayor.km - km_int) else mayor


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


def actualizar_estado_factura(factura):
    saldo = to_decimal(factura.importe_total) - factura.total_aplicado
    if saldo < 0:
        saldo = Decimal("0")
    if saldo == 0 and to_decimal(factura.importe_total) > 0:
        factura.estado_pago = "pagada"
    elif factura.total_aplicado > 0:
        factura.estado_pago = "parcial"
    else:
        factura.estado_pago = "pendiente"


def sincronizar_factura_por_numero(numero_factura):
    numero_factura = (numero_factura or "").strip()
    if not numero_factura:
        return None

    viajes = Viaje.query.filter_by(factura=numero_factura).all()
    factura = Factura.query.filter_by(numero_factura=numero_factura).first()

    if not viajes:
        if factura:
            db.session.delete(factura)
            db.session.flush()
        return None

    primer = viajes[0]
    fecha = min((v.fecha for v in viajes if v.fecha), default=date.today())
    cliente = primer.cliente or "Sin cliente"

    importe_neto = quantize_money(sum((to_decimal(v.total_importe) for v in viajes), Decimal("0")))
    importe_total = quantize_money(sum((to_decimal(v.importe_con_iva) for v in viajes), Decimal("0")))
    iva = quantize_money(importe_total - importe_neto)

    if not factura:
        factura = Factura(
            numero_factura=numero_factura,
            fecha=fecha,
            fecha_vencimiento=fecha + timedelta(days=20),
            cliente=cliente,
            importe_neto=importe_neto,
            iva=iva,
            percepciones=Decimal("0"),
            importe_total=importe_total,
            estado_pago="pendiente",
            observaciones="Generada automáticamente desde viajes cargados manualmente.",
        )
        db.session.add(factura)
    else:
        factura.fecha = fecha
        factura.fecha_vencimiento = factura.fecha_vencimiento or (fecha + timedelta(days=20))
        factura.cliente = cliente
        factura.importe_neto = importe_neto
        factura.iva = iva
        factura.importe_total = importe_total

    actualizar_estado_factura(factura)
    db.session.flush()
    return factura


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
    if saldo_pendiente <= 0 and total > 0:
        liquidacion.estado = "pagada"
    elif total_pagado > 0:
        liquidacion.estado = "parcial"
    else:
        liquidacion.estado = "pendiente"


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


def get_monthly_stats(year: int, month: int):
    viajes_mes = Viaje.query.filter(func.extract("year", Viaje.fecha) == year, func.extract("month", Viaje.fecha) == month)
    cantidad_viajes = viajes_mes.count()
    cantidad_liquidados = viajes_mes.filter(Viaje.liquidado.is_(True)).count()
    total_facturado = viajes_mes.with_entities(func.coalesce(func.sum(Viaje.total_importe), 0)).scalar() or 0
    total_liquidado = viajes_mes.filter(Viaje.liquidado.is_(True)).with_entities(func.coalesce(func.sum(Viaje.total_importe), 0)).scalar() or 0
    total_comisiones = viajes_mes.with_entities(func.coalesce(func.sum(Viaje.comision), 0)).scalar() or 0
    total_comision_matias = viajes_mes.with_entities(func.coalesce(func.sum(Viaje.comision_lucas), 0)).scalar() or 0
    return {"cantidad_viajes": cantidad_viajes, "cantidad_liquidados": cantidad_liquidados, "total_facturado": total_facturado, "total_liquidado": total_liquidado, "total_comisiones": total_comisiones, "total_comision_matias": total_comision_matias, "total_comision_lucas": total_comision_matias}
