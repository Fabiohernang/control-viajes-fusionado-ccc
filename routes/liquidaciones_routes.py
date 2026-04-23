from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from sqlalchemy import func, or_
from datetime import datetime, date
from decimal import Decimal

from extensions import db
from models import (
    FleteroMaster, LiquidacionDescuento, LiquidacionFletero,
    LiquidacionItem, LiquidacionPago, Pago, Viaje,
)
from routes.helpers import (
    login_required, buscar_tarifa_por_km, hydrate_viaje, recalcular_liquidacion,
    parse_liquidacion_archivo,
)
from utils import to_decimal, quantize_money

liquidaciones_bp = Blueprint("liquidaciones", __name__)


@liquidaciones_bp.route("/liquidaciones")
@login_required
def liquidaciones():
    q = request.args.get("q", "").strip()
    query = LiquidacionFletero.query

    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                LiquidacionFletero.fletero.ilike(like),
                LiquidacionFletero.factura_fletero.ilike(like),
            )
        )

    items = query.order_by(LiquidacionFletero.fecha.desc(), LiquidacionFletero.id.desc()).all()

    total_bruto = quantize_money(sum((to_decimal(x.total_bruto) for x in items), Decimal("0")))
    total_descuentos = quantize_money(sum((to_decimal(x.total_descuentos) for x in items), Decimal("0")))
    total_neto = quantize_money(sum((to_decimal(x.neto_pagar) for x in items), Decimal("0")))

    stats = {
        "total_bruto": total_bruto,
        "total_descuentos": total_descuentos,
        "total_neto": total_neto,
        "cantidad": len(items),
    }

    return render_template("liquidaciones.html", items=items, q=q, stats=stats)


# =========================
# IMPORTAR ARCHIVO
# =========================

@liquidaciones_bp.route("/importar_liquidacion_pdf", methods=["GET", "POST"])
@login_required
def importar_liquidacion_pdf():

    if request.method == "POST":
        archivo = request.files.get("archivo")

        if not archivo or not archivo.filename:
            flash("Seleccioná un archivo de liquidación.", "warning")
            return redirect(url_for("liquidaciones.importar_liquidacion_pdf"))

        nombre = archivo.filename.lower()
        if not (nombre.endswith(".pdf") or nombre.endswith(".xls") or nombre.endswith(".xlsx")):
            flash("Formato no soportado. Subí PDF o Excel 8 (.xls/.xlsx).", "warning")
            return redirect(url_for("liquidaciones.importar_liquidacion_pdf"))

        try:
            data = parse_liquidacion_archivo(archivo)

            resultados = []
            for item in data.get("items", []):
                ctg = str(item.get("ctg") or "").strip()

                coincidencias = []
                if ctg:
                    coincidencias = Viaje.query.filter_by(ctg=ctg).all()

                resultados.append({
                    "item": item,
                    "coincidencias": coincidencias,
                    "cantidad": len(coincidencias),
                })

            tipo = "Excel" if nombre.endswith(".xls") or nombre.endswith(".xlsx") else "PDF"
            flash(f"{tipo} procesado correctamente", "success")

            return render_template(
                "liquidacion_preview.html",
                data=data,
                resultados=resultados
            )

        except Exception as e:
            print("ERROR importar_liquidacion_archivo:", e)
            flash(f"Error procesando archivo: {e}", "danger")
            return redirect(url_for("liquidaciones.importar_liquidacion_pdf"))

    return render_template("importar_liquidacion_pdf.html")


# =========================
# BUSCAR PAGOS
# =========================

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
            fecha_desde = datetime.strptime(fecha_desde_raw, "%Y-%m-%d").date()
            query = query.filter(LiquidacionPago.fecha >= fecha_desde)
        except Exception:
            flash("Fecha desde inválida", "warning")

    if fecha_hasta_raw:
        try:
            fecha_hasta = datetime.strptime(fecha_hasta_raw, "%Y-%m-%d").date()
            query = query.filter(LiquidacionPago.fecha <= fecha_hasta)
        except Exception:
            flash("Fecha hasta inválida", "warning")

    if medio:
        query = query.filter(LiquidacionPago.medio == medio)

    if q:
        like = f"%{q}%"

        filtros = [
            LiquidacionPago.numero.ilike(like),
            LiquidacionFletero.fletero.ilike(like),
            LiquidacionFletero.factura_fletero.ilike(like),
            LiquidacionPago.observaciones.ilike(like),
        ]

        query = query.filter(or_(*filtros))

    items = query.order_by(LiquidacionPago.fecha.desc()).all()

    total = quantize_money(sum((to_decimal(x.importe) for x in items), Decimal("0")))

    stats = {
        "cantidad": len(items),
        "total": total
    }

    return render_template(
        "buscar_pagos_fleteros.html",
        items=items,
        stats=stats,
        q=q,
        medio=medio,
        fecha_desde=fecha_desde_raw,
        fecha_hasta=fecha_hasta_raw
    )


# =========================
# NUEVA LIQUIDACION
# =========================

@liquidaciones_bp.route("/liquidaciones/nueva", methods=["GET", "POST"])
@login_required
def nueva_liquidacion():

    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]

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
            observaciones=observaciones
        )

        db.session.add(liquidacion)
        db.session.commit()

        flash("Liquidación creada correctamente", "success")
        return redirect(url_for("liquidaciones.liquidaciones"))

    return render_template("liquidacion_form.html", fleteros=fleteros)


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

        liquidacion.items.clear()
        liquidacion.descuentos.clear()

        viaje_ids = request.form.getlist("viaje_ids")
        viaje_ids = [int(x) for x in viaje_ids if str(x).strip()]
        for viaje_id in viaje_ids:
            viaje = db.session.get(Viaje, viaje_id)
            if viaje:
                liquidacion.items.append(LiquidacionItem(
                    viaje_id=viaje.id,
                    importe=quantize_money(to_decimal(viaje.total_importe))
                ))

        conceptos = request.form.getlist("descuento_concepto[]")
        importes = request.form.getlist("descuento_importe[]")
        for concepto, importe_desc in zip(conceptos, importes):
            concepto = (concepto or "").strip()
            importe_dec = to_decimal(importe_desc, "0")
            if concepto and importe_dec > 0:
                liquidacion.descuentos.append(LiquidacionDescuento(
                    concepto=concepto,
                    importe=quantize_money(importe_dec)
                ))

        recalcular_liquidacion(liquidacion)
        db.session.commit()

        flash("Liquidación actualizada.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))

    viajes = Viaje.query.order_by(Viaje.fecha.desc(), Viaje.id.desc()).all()
    return render_template("liquidacion_form.html", fleteros=fleteros, viajes=viajes, liquidacion=liquidacion)


@liquidaciones_bp.route("/liquidaciones/<int:liquidacion_id>/eliminar", methods=["POST"])
@login_required
def eliminar_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
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

        pago = LiquidacionPago(
            liquidacion_id=liquidacion.id,
            fecha=fecha_pago,
            medio=medio,
            numero=numero,
            importe=quantize_money(importe),
            observaciones=observaciones,
        )
        db.session.add(pago)
        db.session.flush()

        recalcular_liquidacion(liquidacion)
        db.session.commit()

        flash("Pago registrado.", "success")
        return redirect(url_for("liquidaciones.detalle_liquidacion", liquidacion_id=liquidacion.id))

    return render_template(
        "liquidacion_pago_form.html",
        liquidacion=liquidacion,
        fecha_hoy=date.today().strftime("%Y-%m-%d"),
        pago=None,
        accion_url=url_for("liquidaciones.pagar_liquidacion", liquidacion_id=liquidacion.id),
        titulo="Registrar pago de liquidación",
        boton="Guardar pago"
    )


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

    return render_template(
        "liquidacion_pago_form.html",
        liquidacion=liquidacion,
        fecha_hoy=pago.fecha.strftime("%Y-%m-%d"),
        pago=pago,
        accion_url=url_for("liquidaciones.editar_pago_liquidacion", liquidacion_id=liquidacion.id, pago_id=pago.id),
        titulo="Editar pago de liquidación",
        boton="Guardar cambios"
    )


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


# =========================
# HIDRATAR VIAJE
# =========================

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
            viaje.tarifa = to_decimal(tarifa_manual, "0"))
    else:
        viaje.tarifa = to_decimal(tarifa_manual, "0")

    viaje.descuento = to_decimal(form.get("descuento", "0"))
    viaje.kg = to_decimal(form.get("kg", "0"))
    viaje.liquidado = form.get("liquidado") == "si"
    viaje.observaciones = form.get("observaciones", "").strip() or None


# =========================
# CUENTAS CORRIENTES
# =========================
