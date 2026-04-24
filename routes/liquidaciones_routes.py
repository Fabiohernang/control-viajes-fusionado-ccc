from flask import Blueprint, render_template, request, redirect, url_for
from extensions import db
from models import LiquidacionFletero, Viaje, LiquidacionItem
from routes.helpers import login_required, recalcular_liquidacion
from utils import to_decimal, quantize_money
from datetime import date

liquidaciones_bp = Blueprint("liquidaciones", __name__)

@liquidaciones_bp.route("/liquidaciones")
@login_required
def liquidaciones():
    liquidaciones = LiquidacionFletero.query.order_by(LiquidacionFletero.fecha.desc()).all()
    return render_template("liquidaciones.html", liquidaciones=liquidaciones)

@liquidaciones_bp.route("/liquidaciones/nueva", methods=["GET","POST"])
@login_required
def nueva_liquidacion():
    if request.method == "POST":
        fletero = request.form.get("fletero")
        fecha = request.form.get("fecha") or date.today()

        liq = LiquidacionFletero(fletero=fletero, fecha=fecha)
        db.session.add(liq)
        db.session.flush()

        viaje_ids = request.form.getlist("viaje_ids")
        for vid in viaje_ids:
            v = db.session.get(Viaje, int(vid))
            if v:
                v.liquidado = True
                liq.items.append(LiquidacionItem(viaje_id=v.id, importe=quantize_money(to_decimal(v.importe_con_iva))))

        recalcular_liquidacion(liq)
        db.session.commit()

        return redirect(url_for("liquidaciones.liquidaciones"))

    viajes = Viaje.query.filter_by(liquidado=False).all()
    fleteros = sorted(set([v.fletero for v in Viaje.query.all() if v.fletero]))
    return render_template("liquidacion_form.html", viajes=viajes, fleteros=fleteros)
