import csv
import io
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, Response, session, g
from sqlalchemy import or_, func, text
from datetime import datetime, date, timedelta
from decimal import Decimal

from extensions import db
from models import Viaje, Factura, Pago, LiquidacionFletero
from routes.helpers import login_required, get_monthly_stats
from utils import to_decimal, quantize_money

main_bp = Blueprint("main", __name__)

# RUTAS
# =========================

@main_bp.route("/")
@login_required
def index():
    hoy = date.today()
    anio_ant = hoy.year - 1 if hoy.month == 1 else hoy.year
    mes_ant  = 12          if hoy.month == 1 else hoy.month - 1

    # ── Conteos de viajes (mes actual y anterior) ──────────────────────────
    cantidad_viajes_mes = Viaje.query.filter(
        func.extract("year",  Viaje.fecha) == hoy.year,
        func.extract("month", Viaje.fecha) == hoy.month,
    ).count()

    cantidad_viajes_mes_anterior = Viaje.query.filter(
        func.extract("year",  Viaje.fecha) == anio_ant,
        func.extract("month", Viaje.fecha) == mes_ant,
    ).count()

    variacion_viajes = 0
    if cantidad_viajes_mes_anterior > 0:
        variacion_viajes = round(
            ((cantidad_viajes_mes - cantidad_viajes_mes_anterior) / cantidad_viajes_mes_anterior) * 100
        )

    # ── Totales del mes actual (agregados en DB) ───────────────────────────
    total_facturado_mes = to_decimal(
        db.session.query(func.coalesce(func.sum(Factura.importe_total), 0))
        .filter(
            func.extract("year",  Factura.fecha) == hoy.year,
            func.extract("month", Factura.fecha) == hoy.month,
        ).scalar()
    )

    total_cobrado_mes = to_decimal(
        db.session.query(func.coalesce(func.sum(Pago.total_aplicable), 0))
        .filter(
            func.extract("year",  Pago.fecha_pago) == hoy.year,
            func.extract("month", Pago.fecha_pago) == hoy.month,
        ).scalar()
    )

    liquidaciones_mes = LiquidacionFletero.query.filter(
    func.extract("year", LiquidacionFletero.fecha) == hoy.year,
    func.extract("month", LiquidacionFletero.fecha) == hoy.month,
).all()

    total_pagado_fleteros_mes = quantize_money(
    sum((to_decimal(l.total_pagado) for l in liquidaciones_mes), Decimal("0"))
)

    # Pendiente = suma de importe_total de facturas no pagadas
    # (aproximación válida para KPI de dashboard)
    total_pendiente = to_decimal(
        db.session.query(func.coalesce(func.sum(Factura.importe_total), 0))
        .filter(Factura.estado_pago != "pagada")
        .scalar()
    )

    # ── Alertas operativas (queries ligeras) ───────────────────────────────
    viajes_pendientes_count = Viaje.query.filter_by(liquidado=False).count()

    ctg_repetidos_count = (
        db.session.query(Viaje.ctg)
        .filter(Viaje.ctg.isnot(None), Viaje.ctg != "")
        .group_by(Viaje.ctg)
        .having(func.count(Viaje.id) > 1)
        .count()
    )

    alertas = []
    if ctg_repetidos_count:
        alertas.append(f"Hay {ctg_repetidos_count} CTG repetido(s) para revisar.")
    if viajes_pendientes_count:
        alertas.append(f"Hay {viajes_pendientes_count} viaje(s) pendiente(s) de liquidar.")

    # ── Últimos registros (solo 8 filas cada uno) ──────────────────────────
    ultimas_facturas     = Factura.query.order_by(Factura.fecha.desc()).limit(8).all()
    ultimos_pagos        = Pago.query.order_by(Pago.fecha_pago.desc()).limit(8).all()
    ultimas_liquidaciones = LiquidacionFletero.query.order_by(LiquidacionFletero.fecha.desc()).limit(8).all()
    viajes_pendientes    = (
        Viaje.query.filter_by(liquidado=False)
        .order_by(Viaje.fecha.desc())
        .limit(8).all()
    )

    stats = {
        "cantidad_viajes_mes":        cantidad_viajes_mes,
        "cantidad_viajes_mes_anterior": cantidad_viajes_mes_anterior,
        "variacion_viajes":           variacion_viajes,
        "total_facturado_mes":        quantize_money(total_facturado_mes),
        "total_cobrado_mes":          quantize_money(total_cobrado_mes),
        "total_pendiente":            quantize_money(total_pendiente),
        "total_pagado_fleteros_mes":  quantize_money(total_pagado_fleteros_mes),
        "viajes_pendientes":          viajes_pendientes_count,
    }

    return render_template(
        "index.html",
        stats=stats,
        ultimas_facturas=ultimas_facturas,
        ultimos_pagos=ultimos_pagos,
        ultimas_liquidaciones=ultimas_liquidaciones,
        viajes_pendientes=viajes_pendientes,
        alertas=alertas,
    )

@main_bp.route("/reportes")
@login_required
def reportes():
    today = date.today()
    month = int(request.args.get("month", today.month))
    year = int(request.args.get("year", today.year))

    stats = get_monthly_stats(year, month)
    ccc_stats = ccc_month_summary(year, month)

    viajes_mes = (
        Viaje.query.filter(
            func.extract("year", Viaje.fecha) == year,
            func.extract("month", Viaje.fecha) == month,
        )
        .order_by(Viaje.fecha.asc(), Viaje.id.asc())
        .all()
    )

    # Cobranzas del mes: pagos aplicados en ese periodo
    pagos_mes = Pago.query.filter(
        func.extract("year", Pago.fecha_pago) == year,
        func.extract("month", Pago.fecha_pago) == month,
    ).all()
    total_cobrado_mes = quantize_money(sum((to_decimal(p.total_aplicable) for p in pagos_mes), Decimal("0")))
    cantidad_pagos_mes = len(pagos_mes)

    return render_template(
        "reportes.html",
        selected_month=month,
        selected_year=year,
        stats=stats,
        ccc_stats=ccc_stats,
        viajes=viajes_mes,
        total_cobrado_mes=total_cobrado_mes,
        cantidad_pagos_mes=cantidad_pagos_mes,
    )


@main_bp.route("/reportes/lucas/export")
@login_required
def exportar_reporte_lucas():
    today = date.today()
    month = int(request.args.get("month", today.month))
    year = int(request.args.get("year", today.year))

    viajes_mes = (
        Viaje.query.filter(
            func.extract("year", Viaje.fecha) == year,
            func.extract("month", Viaje.fecha) == month,
        )
        .order_by(Viaje.fecha.asc(), Viaje.id.asc())
        .all()
    )

    total_lucas = quantize_money(sum((to_decimal(v.comision_lucas) for v in viajes_mes), Decimal("0")))

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    writer.writerow([f"Reporte Comisión Lucas {month:02d}/{year}"])
    writer.writerow([])
    writer.writerow([
        "Fecha", "Cliente", "Factura", "Fletero", "Origen", "Destino", "KG", "Total Importe", "Comisión Lucas",
    ])

    for v in viajes_mes:
        writer.writerow([
            v.fecha.strftime("%d/%m/%Y") if v.fecha else "",
            v.cliente or "",
            v.factura or "",
            v.fletero or "",
            v.origen or "",
            v.destino or "",
            str(v.kg or ""),
            str(v.total_importe or 0),
            str(v.comision_lucas or 0),
        ])

    writer.writerow([])
    writer.writerow(["TOTAL COMISIÓN LUCAS", "", "", "", "", "", "", "", str(total_lucas)])

    csv_data = output.getvalue()
    output.close()

    filename = f"reporte_lucas_{year}_{month:02d}.csv"
    return Response(
        csv_data,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

