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
    login_required, buscar_tarifa_por_km, hydrate_viaje, recalcular_liquidacion
)
from routes.import_parsers import parse_liquidacion_archivo
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
            flash("Formato no soportado. Subí Excel 8 (.xls/.xlsx) o PDF.", "warning")
            return redirect(url_for("liquidaciones.importar_liquidacion_pdf"))

        try:
            data = parse_liquidacion_archivo(archivo)

            resultados = []
            for item in data.get("items", []):
                ctg = str(item.get("ctg") or "").strip()
                coincidencias = Viaje.query.filter_by(ctg=ctg).all() if ctg else []
                resultados.append({
                    "item": item,
                    "coincidencias": coincidencias,
                    "cantidad": len(coincidencias),
                })

            tipo = "Excel" if nombre.endswith(".xls") or nombre.endswith(".xlsx") else "PDF"
            flash(f"{tipo} procesado correctamente", "success")
            return render_template("liquidacion_preview.html", data=data, resultados=resultados)

        except Exception as e:
            print("ERROR importar_liquidacion_archivo:", e)
            flash(f"Error procesando archivo: {e}", "danger")
            return redirect(url_for("liquidaciones.importar_liquidacion_pdf"))

    return render_template("importar_liquidacion_pdf.html")

# (resto del archivo igual)
