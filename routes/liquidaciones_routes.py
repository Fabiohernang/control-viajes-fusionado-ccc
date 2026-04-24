from flask import Blueprint, render_template, request, redirect, url_for, flash
from sqlalchemy import or_
from extensions import db
from models import LiquidacionFletero, Viaje, LiquidacionItem, LiquidacionPago, LiquidacionDescuento
from routes.helpers import login_required, recalcular_liquidacion
from utils import to_decimal, quantize_money
from datetime import datetime, date
from decimal import Decimal

liquidaciones_bp = Blueprint("liquidaciones", __name__)
NO_LIQUIDAR_TAG = "[NO_LIQUIDAR]"


def _parse_fecha(value):
    if not value:
        return date.today()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _es_liquidable(viaje):
    return NO_LIQUIDAR_TAG not in (viaje.observaciones or "")


def _stats_liquidaciones(items):
    return {"total_bruto": quantize_money(sum((to_decimal(x.total_bruto) for x in items), Decimal("0"))), "total_descuentos": quantize_money(sum((to_decimal(x.total_descuentos) for x in items), Decimal("0"))), "total_neto": quantize_money(sum((to_decimal(x.neto_pagar) for x in items), Decimal("0"))), "cantidad": len(items)}


def _viajes_disponibles(liquidacion=None):
    seleccionados = set()
    if liquidacion:
        seleccionados = {item.viaje_id for item in liquidacion.items}
    viajes = Viaje.query.order_by(Viaje.fecha.desc(), Viaje.id.desc()).all()
    salida = []
    for v in viajes:
        if not _es_liquidable(v):
            continue
        if v.liquidado and v.id not in seleccionados:
            continue
        salida.append(v)
    return salida


def _guardar_items(liquidacion, form):
    for item in list(liquidacion.items):
        if item.viaje:
            item.viaje.liquidado = False
    liquidacion.items.clear()
    db.session.flush()
    for raw_id in form.getlist("viaje_ids"):
        if not raw_id:
            continue
        viaje = db.session.get(Viaje, int(raw_id))
        if not viaje or not _es_liquidable(viaje):
            continue
        if liquidacion.fletero and viaje.fletero.strip().lower() != liquidacion.fletero.strip().lower():
            continue
        viaje.liquidado = True
        liquidacion.items.append(LiquidacionItem(viaje_id=viaje.id, importe=quantize_money(to_decimal(viaje.importe_con_iva))))
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
    return render_template("liquidaciones.html", items=items, q=q, stats=_stats_liquidaciones(items))


@liquidaciones_bp.route("/liquidaciones/buscar-pagos")
@login_required
def buscar_pagos_fleteros():
    q = request.args.get("q", "").strip()
    query = LiquidacionPago.query.join(LiquidacionFletero)
    if q:
        like = f"%{q}%"
        query = query.filter(or_(LiquidacionPago.numero.ilike(like), LiquidacionPago.medio.ilike(like), LiquidacionFletero.fletero.ilike(like), LiquidacionFletero.factura_fletero.ilike(like)))
    items = query.order_by(LiquidacionPago.fecha.desc(), LiquidacionPago.id.desc()).all()
    stats = {"cantidad": len(items), "total": quantize_money(sum((to_decimal(p.importe) for p in items), Decimal("0")))}
    try:
        return render_template("buscar_pagos_fleteros.html", items=items, stats=stats, q=q, medio="", fecha_desde="", fecha_hasta="")
    except Exception:
        all_items = LiquidacionFletero.query.order_by(LiquidacionFletero.fecha.desc()).all()
        return render_template("liquidaciones.html", items=all_items, q="", stats=_stats_liquidaciones(all_items))


@liquidaciones_bp.route("/liquidaciones/nueva", methods=["GET", "POST"])
@login_required
def nueva_liquidacion():
    if request.method == "POST":
        fletero = (request.form.get("fletero") or "").strip()
        if not fletero:
            flash("Tenés que indicar el fletero.", "warning")
            return redirect(url_for("liquidaciones.nueva_liquidacion"))
        liq = LiquidacionFletero(fletero=fletero, fecha=_parse_fecha(request.form.get("fecha")), factura_fletero=(request.form.get("factura_fletero") or "").strip() or None, observaciones=(request.form.get("observaciones") or "").strip() or None)
        db.session.add(liq)
        db.session.flush()
        _guardar_items(liq, request.form)
        db.session.commit()
        flash("Liquidación creada correctamente.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))
    viajes = _viajes_disponibles()
    fleteros = sorted(set([v.fletero for v in Viaje.query.all() if v.fletero]))
    return render_template("liquidacion_form.html", viajes=viajes, fleteros=fleteros, liquidacion=None)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>")
@login_required
def detalle_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    recalcular_liquidacion(liquidacion)
    db.session.commit()
    return render_template("liquidacion_detalle.html", liquidacion=liquidacion)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/editar", methods=["GET", "POST"])
@login_required
def editar_liquidacion(liquidacion_id):
    liq = LiquidacionFletero.query.get_or_404(liquidacion_id)
    if request.method == "POST":
        liq.fecha = _parse_fecha(request.form.get("fecha"))
        liq.fletero = (request.form.get("fletero") or "").strip()
        liq.factura_fletero = (request.form.get("factura_fletero") or "").strip() or None
        liq.observaciones = (request.form.get("observaciones") or "").strip() or None
        _guardar_items(liq, request.form)
        db.session.commit()
        flash("Liquidación actualizada.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))
    viajes = _viajes_disponibles(liq)
    fleteros = sorted(set([v.fletero for v in Viaje.query.all() if v.fletero] + [liq.fletero]))
    return render_template("liquidacion_form.html", viajes=viajes, fleteros=fleteros, liquidacion=liq)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/eliminar", methods=["POST"])
@login_required
def eliminar_liquidacion(liquidacion_id):
    liq = LiquidacionFletero.query.get_or_404(liquidacion_id)
    for item in liq.items:
        if item.viaje:
            item.viaje.liquidado = False
    db.session.delete(liq)
    db.session.commit()
    flash("Liquidación eliminada.", "success")
    return redirect(url_for("liquidaciones.liquidaciones"))


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/orden-pago")
@login_required
def orden_pago_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    recalcular_liquidacion(liquidacion)
    db.session.commit()
    return render_template("liquidacion_orden_pago.html", liquidacion=liquidacion)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/recibo")
@login_required
def recibo_liquidacion(liquidacion_id):
    return redirect(url_for("liquidaciones.orden_pago_liquidacion", liquidacion_id=liquidacion_id))


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/descuento", methods=["GET", "POST"])
@login_required
def agregar_descuento_liquidacion(liquidacion_id):
    liq = LiquidacionFletero.query.get_or_404(liquidacion_id)
    categorias = ["Adelanto", "Combustible", "Percepciones", "Cuota", "Seguros", "Otro"]
    if request.method == "POST":
        categoria = (request.form.get("categoria") or "").strip()
        otro = (request.form.get("otro_concepto") or "").strip()
        concepto = otro if categoria == "Otro" and otro else categoria
        importe = to_decimal(request.form.get("importe", "0"))
        if not concepto or importe <= 0:
            flash("Completá concepto e importe del descuento.", "warning")
            return redirect(url_for("liquidaciones.agregar_descuento_liquidacion", liquidacion_id=liq.id))
        obs = (request.form.get("observaciones") or "").strip()
        if obs:
            concepto = f"{concepto} - {obs}"
        db.session.add(LiquidacionDescuento(liquidacion_id=liq.id, concepto=concepto, importe=quantize_money(importe)))
        db.session.flush()
        recalcular_liquidacion(liq)
        db.session.commit()
        flash("Descuento agregado.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))
    return render_template("liquidacion_descuento_form.html", liquidacion=liq, categorias=categorias)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/descuento/<int:descuento_id>/eliminar", methods=["POST"])
@login_required
def eliminar_descuento_liquidacion(liquidacion_id, descuento_id):
    liq = LiquidacionFletero.query.get_or_404(liquidacion_id)
    descuento = LiquidacionDescuento.query.get_or_404(descuento_id)
    if descuento.liquidacion_id != liq.id:
        flash("El descuento no corresponde a esta liquidación.", "warning")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))
    db.session.delete(descuento)
    db.session.flush()
    recalcular_liquidacion(liq)
    db.session.commit()
    flash("Descuento eliminado.", "success")
    return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/pago", methods=["GET", "POST"])
@login_required
def pagar_liquidacion(liquidacion_id):
    liq = LiquidacionFletero.query.get_or_404(liquidacion_id)
    if request.method == "POST":
        importe = to_decimal(request.form.get("importe", "0"))
        medio = (request.form.get("medio") or "").strip()
        if importe <= 0 or not medio:
            flash("Completá medio e importe.", "warning")
            return redirect(url_for("liquidaciones.pagar_liquidacion", liquidacion_id=liq.id))
        db.session.add(LiquidacionPago(liquidacion_id=liq.id, fecha=_parse_fecha(request.form.get("fecha")), medio=medio, numero=(request.form.get("numero") or "").strip() or None, importe=quantize_money(importe), observaciones=(request.form.get("observaciones") or "").strip() or None))
        db.session.flush()
        recalcular_liquidacion(liq)
        db.session.commit()
        flash("Pago registrado.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))
    return render_template("liquidacion_pago_form.html", liquidacion=liq, fecha_hoy=date.today().strftime("%Y-%m-%d"), pago=None, accion_url=url_for("liquidaciones.pagar_liquidacion", liquidacion_id=liq.id), titulo="Registrar pago de liquidación", boton="Guardar pago")


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/pago/<int:pago_id>/editar", methods=["GET", "POST"])
@login_required
def editar_pago_liquidacion(liquidacion_id, pago_id):
    liq = LiquidacionFletero.query.get_or_404(liquidacion_id)
    pago = LiquidacionPago.query.get_or_404(pago_id)
    if request.method == "POST":
        pago.fecha = _parse_fecha(request.form.get("fecha"))
        pago.medio = (request.form.get("medio") or "").strip()
        pago.numero = (request.form.get("numero") or "").strip() or None
        pago.importe = quantize_money(to_decimal(request.form.get("importe", "0")))
        pago.observaciones = (request.form.get("observaciones") or "").strip() or None
        recalcular_liquidacion(liq)
        db.session.commit()
        flash("Pago actualizado.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))
    return render_template("liquidacion_pago_form.html", liquidacion=liq, fecha_hoy=pago.fecha.strftime("%Y-%m-%d"), pago=pago, accion_url=url_for("liquidaciones.editar_pago_liquidacion", liquidacion_id=liq.id, pago_id=pago.id), titulo="Editar pago", boton="Guardar cambios")


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/pago/<int:pago_id>/eliminar", methods=["POST"])
@login_required
def eliminar_pago_liquidacion(liquidacion_id, pago_id):
    liq = LiquidacionFletero.query.get_or_404(liquidacion_id)
    pago = LiquidacionPago.query.get_or_404(pago_id)
    db.session.delete(pago)
    db.session.flush()
    recalcular_liquidacion(liq)
    db.session.commit()
    flash("Pago eliminado.", "success")
    return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liq.id))
