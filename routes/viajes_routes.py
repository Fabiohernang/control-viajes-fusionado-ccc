from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from sqlalchemy import func, or_, text
from datetime import datetime, date
from decimal import Decimal

from extensions import db
from models import (
    AppConfig, FleteroMaster, LiquidacionFletero, Productor,
    Tarifario, Viaje,
)
from routes.helpers import (
    login_required, get_config_decimal, upsert_maestro,
    buscar_tarifa_por_km, parse_tarifario_text,
    hydrate_viaje, sincronizar_factura_por_numero, recalcular_liquidacion,
)
from utils import to_decimal, quantize_money

viajes_bp = Blueprint("viajes", __name__)
NO_LIQUIDAR_TAG = "[NO_LIQUIDAR]"


def _rates_context():
    matias_rate = get_config_decimal("matias_commission_rate", get_config_decimal("lucas_commission_rate", "0.015"))
    return {
        "iva_rate": float(get_config_decimal("iva_rate", "0.21")),
        "socio_rate": float(get_config_decimal("socio_commission_rate", "0.06")),
        "no_socio_rate": float(get_config_decimal("no_socio_commission_rate", "0.10")),
        "matias_rate": float(matias_rate),
    }


@viajes_bp.route("/api/tarifa")
@login_required
def api_tarifa():
    km = request.args.get("km", "").strip()

    if not km:
        return jsonify({"tarifa": None})

    match = buscar_tarifa_por_km(km)
    if not match:
        return jsonify({"tarifa": None})

    return jsonify({"tarifa": float(match.tarifa_tn), "km_encontrado": match.km})


def _normalizar_ctg(value):
    return (value or "").strip()


def _validar_ctg_unico(ctg, viaje_id=None):
    ctg = _normalizar_ctg(ctg)
    if not ctg:
        return False, "El CTG es obligatorio."

    query = Viaje.query.filter(Viaje.ctg == ctg)
    if viaje_id:
        query = query.filter(Viaje.id != viaje_id)

    if query.first():
        return False, f"El CTG {ctg} ya está cargado en otro viaje."

    return True, ""


def _es_liquidable(viaje):
    return NO_LIQUIDAR_TAG not in (viaje.observaciones or "")


def _aplicar_marca_liquidable(viaje, form):
    liquidable = form.get("liquidable_fletero", "si") == "si"
    obs = (viaje.observaciones or "").replace(NO_LIQUIDAR_TAG, "").strip()

    if not liquidable:
        obs = f"{NO_LIQUIDAR_TAG} {obs}".strip()
        viaje.liquidado = True
    else:
        if viaje.liquidado and NO_LIQUIDAR_TAG in (viaje.observaciones or ""):
            viaje.liquidado = False

    viaje.observaciones = obs or None


def _recalcular_viaje(viaje):
    iva = get_config_decimal("iva_rate", "0.21")
    socio_rate = get_config_decimal("socio_commission_rate", "0.06")
    no_socio_rate = get_config_decimal("no_socio_commission_rate", "0.10")
    matias_rate = get_config_decimal("matias_commission_rate", get_config_decimal("lucas_commission_rate", "0.015"))

    viaje.recalcular(iva=iva, socio_rate=socio_rate, no_socio_rate=no_socio_rate, lucas_rate=matias_rate)

    if not (viaje.factura or "").strip():
        viaje.comision_lucas = Decimal("0")


def _form_context(viaje=None):
    productores = [p.nombre for p in Productor.query.order_by(Productor.nombre.asc()).all()]
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]
    ctx = {
        "viaje": viaje,
        "productores": productores,
        "fleteros": fleteros,
        "factura_prefijo": "0007-000",
        "no_liquidar_tag": NO_LIQUIDAR_TAG,
    }
    ctx.update(_rates_context())
    return ctx


@viajes_bp.route("/viajes/nuevo", methods=["GET", "POST"])
@login_required
def nuevo_viaje():
    if request.method == "POST":
        ctg = _normalizar_ctg(request.form.get("ctg"))
        ok, mensaje = _validar_ctg_unico(ctg)
        if not ok:
            flash(mensaje, "warning")
            return redirect(url_for("viajes.nuevo_viaje"))

        viaje = Viaje()
        hydrate_viaje(viaje, request.form)
        _aplicar_marca_liquidable(viaje, request.form)
        _recalcular_viaje(viaje)

        upsert_maestro(Productor, viaje.cliente)
        upsert_maestro(FleteroMaster, viaje.fletero)

        db.session.add(viaje)
        db.session.commit()

        if viaje.factura:
            sincronizar_factura_por_numero(viaje.factura)
            db.session.commit()

        flash("Viaje creado correctamente.", "success")
        return redirect(url_for("viajes.viajes"))

    return render_template("form.html", **_form_context(None))


@viajes_bp.route("/viajes/<int:viaje_id>/editar", methods=["GET", "POST"])
@login_required
def editar_viaje(viaje_id):
    viaje = Viaje.query.get_or_404(viaje_id)

    if request.method == "POST":
        ctg = _normalizar_ctg(request.form.get("ctg"))
        ok, mensaje = _validar_ctg_unico(ctg, viaje_id=viaje.id)
        if not ok:
            flash(mensaje, "warning")
            return redirect(url_for("viajes.editar_viaje", viaje_id=viaje.id))

        factura_anterior = (viaje.factura or "").strip()
        hydrate_viaje(viaje, request.form)
        _aplicar_marca_liquidable(viaje, request.form)
        _recalcular_viaje(viaje)

        upsert_maestro(Productor, viaje.cliente)
        upsert_maestro(FleteroMaster, viaje.fletero)
        db.session.commit()

        factura_nueva = (viaje.factura or "").strip()
        if factura_anterior:
            sincronizar_factura_por_numero(factura_anterior)
        if factura_nueva:
            sincronizar_factura_por_numero(factura_nueva)
        db.session.commit()

        flash("Viaje actualizado correctamente.", "success")
        return redirect(url_for("viajes.viajes"))

    return render_template("form.html", **_form_context(viaje))


@viajes_bp.route("/viajes/<int:viaje_id>/eliminar", methods=["POST"])
@login_required
def eliminar_viaje(viaje_id):
    viaje = Viaje.query.get_or_404(viaje_id)
    factura_numero = (viaje.factura or "").strip()
    db.session.delete(viaje)
    db.session.commit()
    if factura_numero:
        sincronizar_factura_por_numero(factura_numero)
        db.session.commit()
    flash("Viaje eliminado.", "success")
    return redirect(url_for("viajes.viajes"))


@viajes_bp.route("/viajes/<int:viaje_id>/toggle-liquidado", methods=["POST"])
@login_required
def toggle_liquidado(viaje_id):
    viaje = Viaje.query.get_or_404(viaje_id)
    if not _es_liquidable(viaje):
        flash("Este viaje está marcado como no liquidable al fletero.", "warning")
        return redirect(url_for("viajes.viajes"))
    viaje.liquidado = not viaje.liquidado
    db.session.commit()
    estado = "liquidado" if viaje.liquidado else "pendiente"
    flash(f"Viaje marcado como {estado}.", "success")
    return redirect(url_for("viajes.viajes"))


@viajes_bp.route("/viajes")
@login_required
def viajes():
    q = request.args.get("q", "").strip()
    query = Viaje.query
    if q:
        like = f"%{q}%"
        query = query.filter(or_(Viaje.cliente.ilike(like), Viaje.fletero.ilike(like), Viaje.ctg.ilike(like), Viaje.factura.ilike(like), Viaje.producto.ilike(like)))

    viajes = query.order_by(Viaje.fecha.desc(), Viaje.id.desc()).all()
    ctg_counts = {}
    for v in viajes:
        if v.ctg:
            ctg_counts[v.ctg] = ctg_counts.get(v.ctg, 0) + 1
    ctg_repetidos = {k for k, cantidad in ctg_counts.items() if cantidad > 1}

    total_viajes = len(viajes)
    total_importe = quantize_money(sum((to_decimal(v.total_importe) for v in viajes), Decimal("0")))
    pendientes_liquidar = sum(1 for v in viajes if not v.liquidado and _es_liquidable(v))
    sin_factura = sum(1 for v in viajes if not (v.factura or "").strip())
    no_liquidables = sum(1 for v in viajes if not _es_liquidable(v))

    stats = {"total_viajes": total_viajes, "total_importe": total_importe, "pendientes_liquidar": pendientes_liquidar, "ctg_repetidos": len(ctg_repetidos), "sin_factura": sin_factura, "no_liquidables": no_liquidables}
    return render_template("viajes.html", viajes=viajes, q=q, stats=stats, ctg_repetidos=ctg_repetidos, no_liquidar_tag=NO_LIQUIDAR_TAG)


@viajes_bp.route("/configuracion", methods=["GET", "POST"])
@login_required
def configuracion():
    if request.method == "POST":
        for key in ["iva_rate", "socio_commission_rate", "no_socio_commission_rate", "matias_commission_rate", "lucas_commission_rate"]:
            value = request.form.get(key, "").strip() or "0"
            if key == "lucas_commission_rate" and not request.form.get("lucas_commission_rate"):
                continue
            item = db.session.get(AppConfig, key)
            if item:
                item.value = value
            else:
                db.session.add(AppConfig(key=key, value=value))
        db.session.commit()
        flash("Configuración guardada.", "success")
        return redirect(url_for("viajes.configuracion"))

    matias_default = get_config_decimal("matias_commission_rate", get_config_decimal("lucas_commission_rate", "0.015"))
    config = {"iva_rate": str(get_config_decimal("iva_rate", "0.21")), "socio_commission_rate": str(get_config_decimal("socio_commission_rate", "0.06")), "no_socio_commission_rate": str(get_config_decimal("no_socio_commission_rate", "0.10")), "matias_commission_rate": str(matias_default)}
    return render_template("configuracion.html", config=config)


@viajes_bp.route("/configuracion/reset-datos", methods=["POST"])
@login_required
def resetear_datos_operativos():
    password = (request.form.get("password_reset") or "").strip()
    if password != "BORRAR2026":
        flash("Contraseña incorrecta para borrar datos.", "warning")
        return redirect(url_for("viajes.configuracion"))
    try:
        with db.session.begin():
            db.session.execute(text("TRUNCATE TABLE pago_aplicaciones, saldos_favor, liquidacion_items, liquidacion_descuentos, liquidacion_pagos, liquidaciones_fletero, facturas, pagos, viajes, caja_movimientos, cuotas_seguros RESTART IDENTITY CASCADE"))
        flash("Se borraron todos los datos operativos. El sistema quedó listo para arrancar de cero.", "success")
    except Exception as exc:
        db.session.rollback()
        flash(f"No se pudieron borrar los datos: {exc}", "warning")
    return redirect(url_for("viajes.configuracion"))


@viajes_bp.route("/recalcular", methods=["POST"])
@login_required
def recalcular_todo():
    iva = get_config_decimal("iva_rate", "0.21")
    socio_rate = get_config_decimal("socio_commission_rate", "0.06")
    no_socio_rate = get_config_decimal("no_socio_commission_rate", "0.10")
    matias_rate = get_config_decimal("matias_commission_rate", get_config_decimal("lucas_commission_rate", "0.015"))
    viajes = Viaje.query.all()
    facturas_afectadas = set()
    for viaje in viajes:
        viaje.recalcular(iva=iva, socio_rate=socio_rate, no_socio_rate=no_socio_rate, lucas_rate=matias_rate)
        if not (viaje.factura or "").strip():
            viaje.comision_lucas = Decimal("0")
        if viaje.factura:
            facturas_afectadas.add(viaje.factura.strip())
    db.session.commit()
    for numero in facturas_afectadas:
        sincronizar_factura_por_numero(numero)
    db.session.commit()
    for liq in LiquidacionFletero.query.all():
        recalcular_liquidacion(liq)
    db.session.commit()
    flash("Se recalcularon todos los viajes y liquidaciones con la configuración actual.", "success")
    return redirect(url_for("main.index"))


@viajes_bp.route("/tarifario", methods=["GET", "POST"])
@login_required
def tarifario():
    if request.method == "POST":
        accion = request.form.get("accion", "").strip()
        if accion == "pegar":
            texto = request.form.get("tarifario_texto", "").strip()
            registros, errores = parse_tarifario_text(texto)
            if errores:
                for error in errores[:10]:
                    flash(error, "warning")
                return redirect(url_for("viajes.tarifario"))
            cargados = 0
            actualizados = 0
            for km, tarifa in registros:
                existente = Tarifario.query.filter_by(km=km).first()
                if existente:
                    existente.tarifa_tn = quantize_money(tarifa)
                    actualizados += 1
                else:
                    db.session.add(Tarifario(km=km, tarifa_tn=quantize_money(tarifa)))
                    cargados += 1
            db.session.commit()
            flash(f"Tarifario procesado. Nuevos: {cargados}. Actualizados: {actualizados}.", "success")
            return redirect(url_for("viajes.tarifario"))
        elif accion == "vaciar":
            Tarifario.query.delete()
            db.session.commit()
            flash("Se eliminó todo el tarifario.", "success")
            return redirect(url_for("viajes.tarifario"))
    items = Tarifario.query.order_by(Tarifario.km.asc()).limit(1000).all()
    total_items = Tarifario.query.count()
    return render_template("tarifario.html", items=items, total_items=total_items)
