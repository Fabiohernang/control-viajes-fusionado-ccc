from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy import or_
from datetime import datetime, date
from decimal import Decimal

from extensions import db
from models import (
    FleteroMaster, LiquidacionDescuento, LiquidacionFletero,
    LiquidacionItem, LiquidacionPago, Viaje,
)
from routes.helpers import login_required, recalcular_liquidacion
from utils import to_decimal, quantize_money

liquidaciones_bp = Blueprint("liquidaciones", __name__)
NO_LIQUIDAR_TAG = "[NO_LIQUIDAR]"


def _es_liquidable(viaje):
    return NO_LIQUIDAR_TAG not in (viaje.observaciones or "")


def _viajes_disponibles(liquidacion=None):
    seleccionados = set()
    if liquidacion:
        seleccionados = {item.viaje_id for item in liquidacion.items}

    viajes = Viaje.query.order_by(Viaje.fecha.desc(), Viaje.id.desc()).all()
    disponibles = []
    for viaje in viajes:
        if not _es_liquidable(viaje):
            continue
        if viaje.liquidado and viaje.id not in seleccionados:
            continue
        disponibles.append(viaje)
    return disponibles


def _guardar_items_y_descuentos(liquidacion, form):
    prev_items = list(liquidacion.items)
    for item in prev_items:
        if item.viaje:
            item.viaje.liquidado = False

    liquidacion.items.clear()
    liquidacion.descuentos.clear()
    db.session.flush()

    viaje_ids = [int(x) for x in form.getlist("viaje_ids") if str(x).strip()]
    for viaje_id in viaje_ids:
        viaje = db.session.get(Viaje, viaje_id)
        if not viaje or not _es_liquidable(viaje):
            continue
        if viaje.fletero.strip().lower() != liquidacion.fletero.strip().lower():
            continue
        viaje.liquidado = True
        liquidacion.items.append(
            LiquidacionItem(
                viaje_id=viaje.id,
                importe=quantize_money(to_decimal(viaje.importe_con_iva)),
            )
        )

    conceptos_fijos = [
        ("Comisión", form.get("comision_manual", "0")),
        ("Combustible", form.get("combustible", "0")),
        ("Retención IIBB", form.get("retencion_iibb", "0")),
        ("Percepciones factura comisión", form.get("percepciones_comision", "0")),
        ("Otros descuentos", form.get("otros_descuentos", "0")),
    ]

    for concepto, importe_raw in conceptos_fijos:
        importe = to_decimal(importe_raw, "0")
        if importe > 0:
            liquidacion.descuentos.append(
                LiquidacionDescuento(concepto=concepto, importe=quantize_money(importe))
            )

    for concepto, importe_raw in zip(form.getlist("descuento_concepto[]"), form.getlist("descuento_importe[]")):
        concepto = (concepto or "").strip()
        importe = to_decimal(importe_raw, "0")
        if concepto and importe > 0:
            liquidacion.descuentos.append(
                LiquidacionDescuento(concepto=concepto, importe=quantize_money(importe))
            )

    recalcular_liquidacion(liquidacion)


@liquidaciones_bp.route("/liquidaciones")
@login_required
def liquidaciones():
    q = request.args.get("q", "").strip()
    query = LiquidacionFletero.query

    if q:
        like = f"%{q}%"
        query = query.filter(or_(LiquidacionFletero.fletero.ilike(like), LiquidacionFletero.factura_fletero.ilike(like)))

    items = query.order_by(LiquidacionFletero.fecha.desc(), LiquidacionFletero.id.desc()).all()
    stats = {
        "total_bruto": quantize_money(sum((to_decimal(x.total_bruto) for x in items), Decimal("0"))),
        "total_descuentos": quantize_money(sum((to_decimal(x.total_descuentos) for x in items), Decimal("0"))),
        "total_neto": quantize_money(sum((to_decimal(x.neto_pagar) for x in items), Decimal("0"))),
        "cantidad": len(items),
    }
    return render_template("liquidaciones.html", items=items, q=q, stats=stats)


@liquidaciones_bp.route("/liquidaciones/buscar-pagos")
@login_required
def buscar_pagos_fleteros():
    q = request.args.get("q", "").strip()
    medio = request.args.get("medio", "").strip()
    fecha_desde_raw = request.args.get("fecha_desde", "").strip()
    fecha_hasta_raw = request.args.get("fecha_hasta", "").strip()

    query = LiquidacionPago.query.join(LiquidacionFletero)

    if fecha_desde_raw:
        try:
            query = query.filter(LiquidacionPago.fecha >= datetime.strptime(fecha_desde_raw, "%Y-%m-%d").date())
        except Exception:
            flash("Fecha desde inválida", "warning")
    if fecha_hasta_raw:
        try:
            query = query.filter(LiquidacionPago.fecha <= datetime.strptime(fecha_hasta_raw, "%Y-%m-%d").date())
        except Exception:
            flash("Fecha hasta inválida", "warning")
    if medio:
        query = query.filter(LiquidacionPago.medio == medio)
    if q:
        like = f"%{q}%"
        query = query.filter(or_(LiquidacionPago.numero.ilike(like), LiquidacionFletero.fletero.ilike(like), LiquidacionFletero.factura_fletero.ilike(like), LiquidacionPago.observaciones.ilike(like)))

    items = query.order_by(LiquidacionPago.fecha.desc()).all()
    stats = {"cantidad": len(items), "total": quantize_money(sum((to_decimal(x.importe) for x in items), Decimal("0")))}
    return render_template("buscar_pagos_fleteros.html", items=items, stats=stats, q=q, medio=medio, fecha_desde=fecha_desde_raw, fecha_hasta=fecha_hasta_raw)


@liquidaciones_bp.route("/liquidaciones/nueva", methods=["GET", "POST"])
@login_required
def nueva_liquidacion():
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]
    viajes = _viajes_disponibles()

    if request.method == "POST":
        fecha_raw = request.form.get("fecha", "")
        fecha_liq = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()
        fletero = request.form.get("fletero", "").strip()
        factura_fletero = request.form.get("factura_fletero", "").strip() or None
        observaciones = request.form.get("observaciones", "").strip() or None

        if not fletero:
            flash("Tenés que indicar el fletero.", "warning")
            return redirect(url_for("liquidaciones.nueva_liquidacion"))

        liquidacion = LiquidacionFletero(
            fecha=fecha_liq,
            fletero=fletero,
            factura_fletero=factura_fletero,
            observaciones=observaciones,
        )
        db.session.add(liquidacion)
        db.session.flush()
        _guardar_items_y_descuentos(liquidacion, request.form)
        db.session.commit()

        flash("Liquidación creada correctamente.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))

    return render_template("liquidacion_form.html", fleteros=fleteros, viajes=viajes, liquidacion=None)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/editar", methods=["GET", "POST"])
@login_required
def editar_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]

    if request.method == "POST":
        liquidacion.fecha = datetime.strptime(request.form.get("fecha"), "%Y-%m-%d").date()
        liquidacion.fletero = request.form.get("fletero", "").strip()
        liquidacion.factura_fletero = request.form.get("factura_fletero", "").strip() or None
        liquidacion.observaciones = request.form.get("observaciones", "").strip() or None
        _guardar_items_y_descuentos(liquidacion, request.form)
        db.session.commit()

        flash("Liquidación actualizada.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))

    viajes = _viajes_disponibles(liquidacion)
    return render_template("liquidacion_form.html", fleteros=fleteros, viajes=viajes, liquidacion=liquidacion)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/eliminar", methods=["POST"])
@login_required
def eliminar_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    for item in liquidacion.items:
        if item.viaje:
            item.viaje.liquidado = False
    db.session.delete(liquidacion)
    db.session.commit()
    flash("Liquidación eliminada.", "success")
    return redirect(url_for("liquidaciones.liquidaciones"))


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>")
@login_required
def detalle_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    recalcular_liquidacion(liquidacion)
    db.session.commit()
    return render_template("liquidacion_detalle.html", liquidacion=liquidacion)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/pago", methods=["GET", "POST"])
@login_required
def pagar_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    if request.method == "POST":
        fecha_raw = request.form.get("fecha", "")
        fecha_pago = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()
        medio = request.form.get("medio", "").strip()
        numero = request.form.get("numero", "").strip() or None
        importe = to_decimal(request.form.get("importe", "0"))
        observaciones = request.form.get("observaciones", "").strip() or None
        if not medio or importe <= 0:
            flash("Completá medio e importe del pago.", "warning")
            return redirect(url_for("liquidaciones.pagar_liquidacion", liquidacion_id=liquidacion.id))
        db.session.add(LiquidacionPago(liquidacion_id=liquidacion.id, fecha=fecha_pago, medio=medio, numero=numero, importe=quantize_money(importe), observaciones=observaciones))
        db.session.flush()
        recalcular_liquidacion(liquidacion)
        db.session.commit()
        flash("Pago registrado.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))
    return render_template("liquidacion_pago_form.html", liquidacion=liquidacion, fecha_hoy=date.today().strftime("%Y-%m-%d"), pago=None, accion_url=url_for("liquidaciones.pagar_liquidacion", liquidacion_id=liquidacion.id), titulo="Registrar pago de liquidación", boton="Guardar pago")


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/pago/<int:pago_id>/editar", methods=["GET", "POST"])
@login_required
def editar_pago_liquidacion(liquidacion_id, pago_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    pago = LiquidacionPago.query.get_or_404(pago_id)
    if pago.liquidacion_id != liquidacion.id:
        flash("El pago no corresponde a esta liquidación.", "warning")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))
    if request.method == "POST":
        fecha_raw = request.form.get("fecha", "")
        pago.fecha = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()
        pago.medio = request.form.get("medio", "").strip()
        pago.numero = request.form.get("numero", "").strip() or None
        pago.importe = quantize_money(to_decimal(request.form.get("importe", "0")))
        pago.observaciones = request.form.get("observaciones", "").strip() or None
        if not pago.medio or to_decimal(pago.importe) <= 0:
            flash("Completá medio e importe del pago.", "warning")
            return redirect(url_for("liquidaciones.editar_pago_liquidacion", liquidacion_id=liquidacion.id, pago_id=pago.id))
        recalcular_liquidacion(liquidacion)
        db.session.commit()
        flash("Pago actualizado.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))
    return render_template("liquidacion_pago_form.html", liquidacion=liquidacion, fecha_hoy=pago.fecha.strftime("%Y-%m-%d"), pago=pago, accion_url=url_for("liquidaciones.editar_pago_liquidacion", liquidacion_id=liquidacion.id, pago_id=pago.id), titulo="Editar pago de liquidación", boton="Guardar cambios")


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/pago/<int:pago_id>/eliminar", methods=["POST"])
@login_required
def eliminar_pago_liquidacion(liquidacion_id, pago_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    pago = LiquidacionPago.query.get_or_404(pago_id)
    if pago.liquidacion_id != liquidacion.id:
        flash("El pago no corresponde a esta liquidación.", "warning")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))
    db.session.delete(pago)
    db.session.flush()
    recalcular_liquidacion(liquidacion)
    db.session.commit()
    flash("Pago eliminado correctamente.", "success")
    return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/recibo")
@login_required
def recibo_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    recalcular_liquidacion(liquidacion)
    db.session.commit()
    return render_template("liquidacion_recibo.html", liquidacion=liquidacion)
