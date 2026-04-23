from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from sqlalchemy import or_, func
from datetime import datetime, date, timedelta
from decimal import Decimal

from extensions import db
from models import (
    CajaMovimiento, CuotaSeguro, Factura, FleteroMaster,
    LiquidacionDescuento, LiquidacionFletero,
    Pago, PagoAplicacion, Productor, SaldoFavor,
)
from routes.helpers import (
    login_required, actualizar_estado_factura,
    recalcular_liquidacion, upsert_maestro,
)
from utils import to_decimal, quantize_money

pagos_bp = Blueprint("pagos", __name__)

@login_required
def pagos():
    q = request.args.get("q", "").strip()
    medio = request.args.get("medio", "").strip()
    fecha_desde_raw = request.args.get("fecha_desde", "").strip()
    fecha_hasta_raw = request.args.get("fecha_hasta", "").strip()

    query = Pago.query

    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(Pago.productor.ilike(like), Pago.numero_referencia.ilike(like), Pago.observaciones.ilike(like))
        )
    if medio:
        query = query.filter(Pago.medio_pago == medio)
    if fecha_desde_raw:
        try:
            fd = datetime.strptime(fecha_desde_raw, "%Y-%m-%d").date()
            query = query.filter(Pago.fecha_pago >= fd)
        except ValueError:
            pass
    if fecha_hasta_raw:
        try:
            fh = datetime.strptime(fecha_hasta_raw, "%Y-%m-%d").date()
            query = query.filter(Pago.fecha_pago <= fh)
        except ValueError:
            pass

    items = query.order_by(Pago.fecha_pago.desc(), Pago.id.desc()).all()
    stats_count = len(items)
    stats_total = quantize_money(sum((to_decimal(x.total_aplicable) for x in items), Decimal("0")))

    return render_template(
        "pagos.html",
        items=items,
        q=q, medio=medio, fecha_desde=fecha_desde_raw, fecha_hasta=fecha_hasta_raw,
        stats={"cantidad": stats_count, "total": stats_total},
    )

@pagos_bp.route("/caja", methods=["GET", "POST"])
@login_required
def caja():
    if request.method == "POST":
        fecha = request.form.get("fecha", "").strip()
        tipo = request.form.get("tipo", "").strip().lower()
        importe = to_decimal(request.form.get("importe", "0"))
        observaciones = request.form.get("observaciones", "").strip() or None

        if tipo not in {"ingreso", "egreso"}:
            flash("Tipo de movimiento inválido.", "warning")
            return redirect(url_for("pagos.caja"))

        if importe <= 0:
            flash("El importe debe ser mayor a 0.", "warning")
            return redirect(url_for("pagos.caja"))

        try:
            fecha_obj = datetime.strptime(fecha, "%Y-%m-%d").date()
        except ValueError:
            flash("Fecha inválida.", "warning")
            return redirect(url_for("pagos.caja"))

        concepto = request.form.get("concepto", "").strip() or None
        medio = request.form.get("medio", "").strip() or None

        mov = CajaMovimiento(
            fecha=fecha_obj,
            tipo=tipo,
            concepto=concepto,
            medio=medio,
            importe=quantize_money(importe),
            observaciones=observaciones,
        )
        db.session.add(mov)
        db.session.commit()
        flash("Movimiento de caja guardado.", "success")
        return redirect(url_for("pagos.caja"))

    items = CajaMovimiento.query.order_by(CajaMovimiento.fecha.desc(), CajaMovimiento.id.desc()).all()
    total_ingresos = quantize_money(
        sum((to_decimal(x.importe) for x in items if x.tipo == "ingreso"), Decimal("0"))
    )
    total_egresos = quantize_money(
        sum((to_decimal(x.importe) for x in items if x.tipo == "egreso"), Decimal("0"))
    )
    saldo = quantize_money(total_ingresos - total_egresos)

    return render_template(
        "caja.html",
        items=items,
        stats={
            "ingresos": total_ingresos,
            "egresos": total_egresos,
            "saldo": saldo,
            "cantidad": len(items),
        },
        today=date.today().strftime("%Y-%m-%d"),
    )


@pagos_bp.route("/pagos/nuevo", methods=["GET", "POST"])
@login_required
def nuevo_pago():
    facturas_pendientes = (
        Factura.query
        .filter(Factura.estado_pago != "pagada")
        .order_by(Factura.fecha_vencimiento.asc(), Factura.fecha.asc(), Factura.numero_factura.asc())
        .all()
    )
    productores = [p.nombre for p in Productor.query.order_by(Productor.nombre.asc()).all()]

    if request.method == "POST":
        fecha_raw = request.form.get("fecha_pago", "")
        fecha_pago = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()

        fecha_cobro_raw = request.form.get("fecha_cobro_real", "").strip()
        fecha_cobro_real = datetime.strptime(fecha_cobro_raw, "%Y-%m-%d").date() if fecha_cobro_raw else None

        productor = request.form.get("productor", "").strip()
        medio_pago = request.form.get("medio_pago", "").strip() or "Transferencia"
        numero_referencia = request.form.get("numero_referencia", "").strip() or None

        importe = to_decimal(request.form.get("importe", "0"))
        retenciones = to_decimal(request.form.get("retenciones", "0"))
        observaciones = request.form.get("observaciones", "").strip() or None

        if not productor:
            flash("Tenés que indicar el productor/cliente.", "warning")
            return redirect(url_for("pagos.nuevo_pago"))

        upsert_maestro(Productor, productor)

        total_aplicable = quantize_money(importe + retenciones)

        factura_ids = request.form.getlist("factura_ids")
        factura_ids = [int(x) for x in factura_ids if str(x).strip()]

        if not factura_ids:
            flash("Tenés que seleccionar al menos una factura.", "warning")
            return redirect(url_for("pagos.nuevo_pago"))

        facturas_sel = Factura.query.filter(Factura.id.in_(factura_ids)).all()
        facturas_sel = sorted(
            facturas_sel,
            key=lambda f: (f.fecha_vencimiento, f.fecha, f.numero_factura)
        )

        pago = Pago(
            fecha_pago=fecha_pago,
            fecha_cobro_real=fecha_cobro_real,
            productor=productor,
            medio_pago=medio_pago,
            numero_referencia=numero_referencia,
            importe=quantize_money(importe),
            retenciones=quantize_money(retenciones),
            total_aplicable=quantize_money(total_aplicable),
            observaciones=observaciones,
        )
        db.session.add(pago)
        db.session.flush()

        disponible = total_aplicable

        for factura in facturas_sel:
            if disponible <= 0:
                break

            saldo = factura.saldo_pendiente
            if saldo <= 0:
                continue

            aplicado_total = saldo if saldo <= disponible else disponible
            aplicado_total = quantize_money(aplicado_total)

            proporcion_pago = Decimal("0.00")
            proporcion_ret = Decimal("0.00")

            if total_aplicable > 0:
                if importe > 0:
                    proporcion_pago = quantize_money(aplicado_total * importe / total_aplicable)
                proporcion_ret = aplicado_total - proporcion_pago

            db.session.add(PagoAplicacion(
                pago_id=pago.id,
                factura_id=factura.id,
                importe_pago=quantize_money(proporcion_pago),
                importe_retenciones=quantize_money(proporcion_ret),
                total_aplicado=quantize_money(aplicado_total),
            ))

            disponible -= aplicado_total

        db.session.flush()

        for factura in facturas_sel:
            actualizar_estado_factura(factura)

        disponible = quantize_money(disponible if disponible > 0 else Decimal("0"))

        if disponible > 0:
            db.session.add(SaldoFavor(
                productor=productor,
                pago_origen_id=pago.id,
                importe=disponible,
                aplicado=False,
                observaciones=f"Saldo a favor generado por pago #{pago.id}",
            ))

        db.session.commit()
        flash("Pago registrado correctamente.", "success")
        return redirect(url_for("pagos.pagos"))

    return render_template(
        "pago_form.html",
        facturas_pendientes=facturas_pendientes,
        productores=productores,
        pago=None,
        modo_edicion=False
    )

@pagos_bp.route("/pagos/<int:pago_id>/editar", methods=["GET", "POST"])
@login_required
def editar_pago(pago_id):
    pago = Pago.query.get_or_404(pago_id)
    productores = [p.nombre for p in Productor.query.order_by(Productor.nombre.asc()).all()]

    if request.method == "POST":
        fecha_raw = request.form.get("fecha_pago", "")
        pago.fecha_pago = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()

        fecha_cobro_raw = request.form.get("fecha_cobro_real", "").strip()
        pago.fecha_cobro_real = datetime.strptime(fecha_cobro_raw, "%Y-%m-%d").date() if fecha_cobro_raw else None

        pago.productor = request.form.get("productor", "").strip()
        pago.medio_pago = request.form.get("medio_pago", "").strip() or "Transferencia"
        pago.numero_referencia = request.form.get("numero_referencia", "").strip() or None
        pago.importe = quantize_money(to_decimal(request.form.get("importe", "0")))
        pago.retenciones = quantize_money(to_decimal(request.form.get("retenciones", "0")))
        pago.total_aplicable = quantize_money(to_decimal(pago.importe) + to_decimal(pago.retenciones))
        pago.observaciones = request.form.get("observaciones", "").strip() or None

        db.session.commit()
        flash("Pago actualizado correctamente.", "success")
        return redirect(url_for("pagos.pagos"))

    return render_template(
        "pago_form.html",
        pago=pago,
        productores=productores,
        modo_edicion=True
    )

@pagos_bp.route("/cuotas-seguros", methods=["GET", "POST"])
@login_required
def cuotas_seguros():
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]
    periodo_q = request.args.get("periodo", "").strip()
    fletero_q = request.args.get("fletero", "").strip()

    if request.method == "POST":
        periodo_raw = request.form.get("periodo", "").strip()
        fletero = request.form.get("fletero", "").strip()
        cuota_social = to_decimal(request.form.get("cuota_social", "0"))
        seguro_carga = to_decimal(request.form.get("seguro_carga", "0"))
        seguro_accidentes = to_decimal(request.form.get("seguro_accidentes", "0"))
        seguro_particular = to_decimal(request.form.get("seguro_particular", "0"))
        otros_descuentos = to_decimal(request.form.get("otros_descuentos", "0"))
        observaciones = request.form.get("observaciones", "").strip() or None

        if not periodo_raw:
            flash("Tenés que indicar el período.", "warning")
            return redirect(url_for("pagos.cuotas_seguros"))
        if not fletero:
            flash("Tenés que indicar el fletero.", "warning")
            return redirect(url_for("pagos.cuotas_seguros"))

        try:
            year, month = periodo_raw.split("-")
            periodo = date(int(year), int(month), 1)
        except (TypeError, ValueError):
            flash("Período inválido.", "warning")
            return redirect(url_for("pagos.cuotas_seguros"))

        upsert_maestro(FleteroMaster, fletero)

        item = CuotaSeguro(
            periodo=periodo,
            fletero=fletero,
            cuota_social=quantize_money(cuota_social),
            seguro_carga=quantize_money(seguro_carga),
            seguro_accidentes=quantize_money(seguro_accidentes),
            seguro_particular=quantize_money(seguro_particular),
            otros_descuentos=quantize_money(otros_descuentos),
            observaciones=observaciones,
        )
        db.session.add(item)
        db.session.commit()
        flash("Registro de cuotas/seguros guardado.", "success")
        return redirect(url_for("pagos.cuotas_seguros"))

    # -----------------------------
    # Historial filtrado
    # -----------------------------
    query = CuotaSeguro.query

    if periodo_q:
        try:
            year, month = periodo_q.split("-")
            periodo = date(int(year), int(month), 1)
            query = query.filter(CuotaSeguro.periodo == periodo)
        except (TypeError, ValueError):
            flash("Período de filtro inválido, se ignoró.", "warning")

    if fletero_q:
        query = query.filter(CuotaSeguro.fletero.ilike(f"%{fletero_q}%"))

    items = query.order_by(CuotaSeguro.periodo.desc(), CuotaSeguro.fletero.asc()).all()

    total_mes = quantize_money(sum((x.total for x in items), Decimal("0")))

    # -----------------------------
    # Estado actual por fletero
    # -----------------------------
    resumen_query = CuotaSeguro.query
    if fletero_q:
        resumen_query = resumen_query.filter(CuotaSeguro.fletero.ilike(f"%{fletero_q}%"))

    resumen_items = resumen_query.order_by(CuotaSeguro.fletero.asc(), CuotaSeguro.periodo.asc()).all()

    resumen_dict = {}

    for x in resumen_items:
        fletero_nombre = (x.fletero or "").strip()
        if fletero_nombre not in resumen_dict:
            resumen_dict[fletero_nombre] = {
                "fletero": fletero_nombre,
                "cuota_social_hasta": None,
                "seguro_carga_hasta": None,
                "seguro_accidentes_hasta": None,
                "seguro_particular_hasta": None,
                "otros_hasta": None,
                "ultimo_registro": x.periodo,
            }

        r = resumen_dict[fletero_nombre]

        if x.cuota_social and to_decimal(x.cuota_social) > 0:
            r["cuota_social_hasta"] = x.periodo
        if x.seguro_carga and to_decimal(x.seguro_carga) > 0:
            r["seguro_carga_hasta"] = x.periodo
        if x.seguro_accidentes and to_decimal(x.seguro_accidentes) > 0:
            r["seguro_accidentes_hasta"] = x.periodo
        if x.seguro_particular and to_decimal(x.seguro_particular) > 0:
            r["seguro_particular_hasta"] = x.periodo
        if x.otros_descuentos and to_decimal(x.otros_descuentos) > 0:
            r["otros_hasta"] = x.periodo

        if not r["ultimo_registro"] or x.periodo > r["ultimo_registro"]:
            r["ultimo_registro"] = x.periodo

    resumen_fleteros = list(resumen_dict.values())
    resumen_fleteros.sort(key=lambda x: x["fletero"])

    ultimo_periodo = None
    if resumen_items:
        ultimo_periodo = max(x.periodo for x in resumen_items)

    return render_template(
        "cuotas_seguros.html",
        items=items,
        resumen_fleteros=resumen_fleteros,
        fleteros=fleteros,
        today_month=date.today().strftime("%Y-%m"),
        filters={"periodo": periodo_q, "fletero": fletero_q},
        stats={
            "cantidad": len(items),
            "total": total_mes,
            "fleteros": len(resumen_fleteros),
            "ultimo_periodo": ultimo_periodo,
        },
    )
@pagos_bp.route("/cuotas-seguros/<int:item_id>/editar", methods=["GET", "POST"])
@login_required
def editar_cuota_seguro(item_id):
    item = CuotaSeguro.query.get_or_404(item_id)
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]

    if request.method == "POST":
        periodo_raw = request.form.get("periodo", "").strip()
        fletero = request.form.get("fletero", "").strip()

        if not periodo_raw or not fletero:
            flash("Período y fletero son obligatorios.", "warning")
            return redirect(url_for("editar_cuota_seguro", item_id=item.id))

        try:
            year, month = periodo_raw.split("-")
            item.periodo = date(int(year), int(month), 1)
        except (TypeError, ValueError):
            flash("Período inválido.", "warning")
            return redirect(url_for("editar_cuota_seguro", item_id=item.id))

        item.fletero = fletero
        item.cuota_social = quantize_money(to_decimal(request.form.get("cuota_social", "0")))
        item.seguro_carga = quantize_money(to_decimal(request.form.get("seguro_carga", "0")))
        item.seguro_accidentes = quantize_money(to_decimal(request.form.get("seguro_accidentes", "0")))
        item.seguro_particular = quantize_money(to_decimal(request.form.get("seguro_particular", "0")))
        item.otros_descuentos = quantize_money(to_decimal(request.form.get("otros_descuentos", "0")))
        item.observaciones = request.form.get("observaciones", "").strip() or None

        upsert_maestro(FleteroMaster, fletero)

        db.session.commit()
        flash("Registro actualizado.", "success")
        return redirect(url_for("pagos.cuotas_seguros"))

    return render_template("cuota_seguro_form.html", item=item, fleteros=fleteros)
    
@pagos_bp.route("/cuotas-seguros/<int:item_id>/agregar-liquidacion", methods=["POST"])
@login_required
def agregar_cuota_seguro_a_liquidacion(item_id):
    item = CuotaSeguro.query.get_or_404(item_id)
    liquidacion_id_raw = request.form.get("liquidacion_id", "").strip()

    if not liquidacion_id_raw:
        flash("Seleccioná una liquidación.", "warning")
        return redirect(url_for("pagos.cuotas_seguros"))

    try:
        liquidacion_id = int(liquidacion_id_raw)
    except ValueError:
        flash("Liquidación inválida.", "warning")
        return redirect(url_for("pagos.cuotas_seguros"))

    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)

    if (liquidacion.fletero or "").strip().lower() != (item.fletero or "").strip().lower():
        flash("La liquidación seleccionada no corresponde al mismo fletero.", "warning")
        return redirect(url_for("pagos.cuotas_seguros"))

    importe = quantize_money(item.total)
    if importe <= 0:
        flash("El registro no tiene importe para aplicar.", "warning")
        return redirect(url_for("pagos.cuotas_seguros"))

    concepto = f"Cuotas/Seguros {item.periodo.strftime('%m/%Y')}"
    existente = LiquidacionDescuento.query.filter_by(
        liquidacion_id=liquidacion.id,
        concepto=concepto,
        importe=importe,
    ).first()
    if existente:
        flash("Este concepto ya fue agregado a esa liquidación.", "warning")
        return redirect(url_for("pagos.cuotas_seguros"))

    db.session.add(LiquidacionDescuento(
        liquidacion_id=liquidacion.id,
        concepto=concepto,
        importe=importe,
    ))
    db.session.flush()

    recalcular_liquidacion(liquidacion)
    db.session.commit()
    flash(f"Se agregó {concepto} en liquidación #{liquidacion.id}.", "success")
    return redirect(url_for("detalle_liquidacion", liquidacion_id=liquidacion.id))

# =========================
# LIQUIDACIONES
# =========================
