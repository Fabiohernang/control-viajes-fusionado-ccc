import re
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from sqlalchemy import or_, func
from datetime import datetime, date, timedelta
from decimal import Decimal

from extensions import db
from models import Factura, Pago, SaldoFavor, Viaje
from routes.helpers import (
    login_required, actualizar_estado_factura,
    crear_factura_y_viajes_desde_importacion,
)
from routes.factura_import_parsers import parse_factura_archivo as parse_factura_pdf
from utils import to_decimal, quantize_money

facturas_bp = Blueprint("facturas", __name__)

@facturas_bp.route("/facturas")
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
            flash("Seleccioná un archivo de factura.", "warning")
            return redirect(url_for("facturas.importar_factura_pdf"))

        nombre = archivo.filename.lower()
        if not (nombre.endswith(".pdf") or nombre.endswith(".xls") or nombre.endswith(".xlsx")):
            flash("El archivo debe ser PDF o Excel.", "warning")
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

# resto igual...
