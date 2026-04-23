import re
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from sqlalchemy import or_, func
from datetime import datetime, date
from decimal import Decimal

from extensions import db
from models import Factura, Pago, SaldoFavor, Viaje
from routes.helpers import (
    login_required, actualizar_estado_factura,
    parse_factura_pdf, crear_factura_y_viajes_desde_importacion,
)
from utils import to_decimal, quantize_money

facturas_bp = Blueprint("facturas", __name__)

@login_required
def facturas():
    q = request.args.get("q", "").strip()
    estado = request.args.get("estado", "").strip()
    vencida = request.args.get("vencida", "").strip()

    query = Factura.query

    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                Factura.numero_factura.ilike(like),
                Factura.cliente.ilike(like),
            )
        )

    if estado:
        query = query.filter(Factura.estado_pago == estado)

    items = query.order_by(Factura.fecha.desc(), Factura.id.desc()).all()

    if vencida == "si":
        items = [x for x in items if x.vencida]
    elif vencida == "no":
        items = [x for x in items if not x.vencida]

    cantidad = len(items)
    total_vencido = quantize_money(sum((to_decimal(x.saldo_pendiente) for x in items if x.vencida), Decimal("0")))
    total_adeudado = quantize_money(sum((to_decimal(x.saldo_pendiente) for x in items if x.estado_pago != "pagada"), Decimal("0")))
    cantidad_vencidas = sum(1 for x in items if x.vencida)
    saldo_favor_total = quantize_money(
        sum((to_decimal(x.importe) for x in SaldoFavor.query.filter_by(aplicado=False).all()), Decimal("0"))
    )

    return render_template(
        "facturas.html",
        items=items,
        q=q,
        estado=estado,
        vencida=vencida,
        stats={
            "cantidad": cantidad,
            "vencido": total_vencido,
            "adeudado": total_adeudado,
            "cantidad_vencidas": cantidad_vencidas,
            "saldo_favor_total": saldo_favor_total,
        },
    )


@facturas_bp.route("/facturas/importar-pdf", methods=["GET", "POST"])
@login_required
def importar_factura_pdf():
    preview = session.get("factura_pdf_preview")

    if request.method == "POST":
        archivo = request.files.get("archivo_pdf")
        if not archivo or not archivo.filename:
            flash("Seleccioná un PDF de factura.", "warning")
            return redirect(url_for("facturas.importar_factura_pdf"))

        if not archivo.filename.lower().endswith(".pdf"):
            flash("El archivo debe ser PDF.", "warning")
            return redirect(url_for("facturas.importar_factura_pdf"))

        try:
            parsed = parse_factura_pdf(archivo)
            session["factura_pdf_preview"] = parsed
            flash("Factura leída correctamente. Revisá la vista previa antes de importar.", "success")
        except Exception as exc:
            session.pop("factura_pdf_preview", None)
            flash(f"No se pudo leer la factura: {exc}", "warning")

        return redirect(url_for("facturas.importar_factura_pdf"))

    return render_template("factura_importar_pdf.html", preview=preview)


@facturas_bp.route("/facturas/importar-pdf/confirmar", methods=["POST"])
@login_required
def confirmar_importacion_factura_pdf():
    preview = session.get("factura_pdf_preview")
    if not preview:
        flash("No hay ninguna factura pendiente de importar.", "warning")
        return redirect(url_for("facturas.importar_factura_pdf"))

    accion = request.form.get("accion", "factura_y_viajes")
    crear_viajes = accion == "factura_y_viajes"

    try:
        factura = crear_factura_y_viajes_desde_importacion(preview, crear_viajes=crear_viajes)
        session.pop("factura_pdf_preview", None)
        flash("Factura importada correctamente.", "success")
        return redirect(url_for("detalle_factura", factura_id=factura.id))
    except Exception as exc:
        flash(f"No se pudo importar la factura: {exc}", "warning")
        return redirect(url_for("facturas.importar_factura_pdf"))


@facturas_bp.route("/facturas/importar-pdf/cancelar", methods=["POST"])
@login_required
def cancelar_importacion_factura_pdf():
    session.pop("factura_pdf_preview", None)
    flash("Vista previa descartada.", "success")
    return redirect(url_for("facturas.importar_factura_pdf"))
@facturas_bp.route("/cobranzas")
@login_required
def cobranzas():
    hoy = date.today()
    fecha_7 = hoy + timedelta(days=7)

    facturas_abiertas = Factura.query.filter(Factura.estado_pago != "pagada").all()
    facturas_vencidas = [f for f in facturas_abiertas if f.vencida]
    facturas_a_vencer_7 = [
        f for f in facturas_abiertas
        if f.fecha_vencimiento >= hoy and f.fecha_vencimiento <= fecha_7
    ]

    saldo_favor_total = quantize_money(
        sum((to_decimal(x.importe) for x in SaldoFavor.query.filter_by(aplicado=False).all()), Decimal("0"))
    )

    total_adeudado = quantize_money(
        sum((to_decimal(f.saldo_pendiente) for f in facturas_abiertas), Decimal("0"))
    )

    total_vencido = quantize_money(
        sum((to_decimal(f.saldo_pendiente) for f in facturas_vencidas), Decimal("0"))
    )

    a_vencer_7 = quantize_money(
        sum((to_decimal(f.saldo_pendiente) for f in facturas_a_vencer_7), Decimal("0"))
    )

    # Clientes con deuda
    clientes_dict = {}
    for f in facturas_abiertas:
        cliente = f.cliente
        if cliente not in clientes_dict:
            clientes_dict[cliente] = {
                "cliente": cliente,
                "cantidad_facturas": 0,
                "total_adeudado": Decimal("0"),
                "total_vencido": Decimal("0"),
                "ultimo_pago": None,
            }

        clientes_dict[cliente]["cantidad_facturas"] += 1
        clientes_dict[cliente]["total_adeudado"] += to_decimal(f.saldo_pendiente)

        if f.vencida:
            clientes_dict[cliente]["total_vencido"] += to_decimal(f.saldo_pendiente)

    # último pago por cliente
    pagos_por_cliente = (
        Pago.query.order_by(Pago.fecha_pago.desc(), Pago.id.desc()).all()
    )
    for p in pagos_por_cliente:
        if p.productor in clientes_dict and clientes_dict[p.productor]["ultimo_pago"] is None:
            clientes_dict[p.productor]["ultimo_pago"] = p.fecha_pago.strftime("%d/%m/%Y")

    clientes_deuda = list(clientes_dict.values())
    clientes_deuda.sort(key=lambda x: x["total_adeudado"], reverse=True)

    for c in clientes_deuda:
        c["total_adeudado"] = quantize_money(c["total_adeudado"])
        c["total_vencido"] = quantize_money(c["total_vencido"])

    # últimos pagos
    ultimos_pagos = Pago.query.order_by(Pago.fecha_pago.desc(), Pago.id.desc()).limit(10).all()

    # análisis de velocidad de pago
    analisis_cliente = {}
    facturas_pagadas = Factura.query.filter(Factura.estado_pago == "pagada").all()

    for f in facturas_pagadas:
        fechas_reales = [
            aplicacion.pago.fecha_cobro_real
            for aplicacion in f.aplicaciones
            if aplicacion.pago and aplicacion.pago.fecha_cobro_real
        ]

        fecha_referencia = max(fechas_reales) if fechas_reales else f.ultima_fecha_pago

        if not fecha_referencia:
            continue

        dias = (fecha_referencia - f.fecha).days
        if dias < 0:
            dias = 0

        cliente = f.cliente
        if cliente not in analisis_cliente:
            analisis_cliente[cliente] = []

        analisis_cliente[cliente].append(dias)

    promedio_dias_cobro = 0
    cliente_mas_rapido = None
    dias_cliente_rapido = 0
    cliente_mas_lento = None
    dias_cliente_lento = 0

    if analisis_cliente:
        promedios = []
        todos = []

        for cliente, dias_lista in analisis_cliente.items():
            promedio = sum(dias_lista) / len(dias_lista)
            promedios.append((cliente, promedio))
            todos.extend(dias_lista)

        promedios.sort(key=lambda x: x[1])

        cliente_mas_rapido = promedios[0][0]
        dias_cliente_rapido = round(promedios[0][1])

        cliente_mas_lento = promedios[-1][0]
        dias_cliente_lento = round(promedios[-1][1])

        promedio_dias_cobro = round(sum(todos) / len(todos))

    return render_template(
        "cobranzas.html",
        stats={
            "total_adeudado": total_adeudado,
            "total_vencido": total_vencido,
            "a_vencer_7": a_vencer_7,
            "saldo_favor_total": saldo_favor_total,
            "promedio_dias_cobro": promedio_dias_cobro,
            "cliente_mas_rapido": cliente_mas_rapido,
            "dias_cliente_rapido": dias_cliente_rapido,
            "cliente_mas_lento": cliente_mas_lento,
            "dias_cliente_lento": dias_cliente_lento,
        },
        clientes_deuda=clientes_deuda[:15],
        facturas_vencidas=sorted(facturas_vencidas, key=lambda x: x.dias_vencida, reverse=True)[:15],
        ultimos_pagos=ultimos_pagos
    )

@facturas_bp.route("/facturas/<int:factura_id>")
@login_required
def detalle_factura(factura_id):
    factura = Factura.query.get_or_404(factura_id)
    viajes = (
        Viaje.query
        .filter(Viaje.factura == factura.numero_factura)
        .order_by(Viaje.fecha.asc(), Viaje.id.asc())
        .all()
    )
    saldos_favor_cliente = SaldoFavor.query.filter_by(productor=factura.cliente, aplicado=False).all()
    return render_template(
        "factura_detalle.html",
        factura=factura,
        viajes=viajes,
        saldos_favor_cliente=saldos_favor_cliente,
    )


@facturas_bp.route("/facturas/<int:factura_id>/eliminar", methods=["POST"])
@login_required
def eliminar_factura(factura_id):
    factura = Factura.query.get_or_404(factura_id)
    if factura.aplicaciones:
        flash("No se puede eliminar una factura con pagos aplicados.", "warning")
        return redirect(url_for("detalle_factura", factura_id=factura_id))
    db.session.delete(factura)
    db.session.commit()
    flash("Factura eliminada.", "success")
    return redirect(url_for("facturas.facturas"))


@facturas_bp.route("/facturas/<int:factura_id>/editar-percepciones", methods=["POST"])
@login_required
def editar_percepciones(factura_id):
    factura = Factura.query.get_or_404(factura_id)

    percepciones = to_decimal(request.form.get("percepciones", "0"))
    factura.percepciones = quantize_money(percepciones)
    factura.importe_total = quantize_money(
        to_decimal(factura.importe_neto) +
        to_decimal(factura.iva) +
        to_decimal(factura.percepciones)
    )

    actualizar_estado_factura(factura)
    db.session.commit()

    flash("Percepciones actualizadas.", "success")
    return redirect(url_for("detalle_factura", factura_id=factura.id))

