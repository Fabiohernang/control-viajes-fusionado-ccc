from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g
from datetime import datetime, date
from decimal import Decimal

from extensions import db
from models import CCCAccion, CCCCuenta, CCCMovimiento, CCCPeriodo
from routes.helpers import login_required
from services.ccc_service import (
    ccc_float, ccc_decimal,
    ccc_serialize_movimiento, ccc_serialize_accion, ccc_serialize_cuenta,
    ccc_parse_date, ccc_block_due_date, ccc_rules_for_tipo,
    ccc_calc_coef, ccc_calc_mora, ccc_estado_para_bloque,
    ccc_build_blocks_for_cuenta, ccc_month_summary,
    ccc_get_message_template, ccc_set_message_template, ccc_format_message,
    parse_liquidacion_pdf,
)
from utils import to_decimal, quantize_money

ccc_bp = Blueprint("ccc", __name__)

@login_required
def ccc_index():
    return render_template("ccc.html", usuario=session.get("nombre") or session.get("username") or "")


@ccc_bp.route("/api/ccc/upload", methods=["POST"])
@login_required
def ccc_upload():
    data = request.get_json(silent=True) or {}
    cuentas = data.get("cuentas", [])
    archivo = (data.get("archivo") or "desconocido").strip()
    sector = (data.get("sector") or "clientes").strip().lower()
    usuario = session.get("nombre") or session.get("username") or ""

    sectores_validos = {"clientes", "orden_externos", "orden_socios", "socios_particulares", "telefonos"}
    if sector not in sectores_validos:
        return jsonify({"ok": False, "error": "Sector inválido"}), 400

    periodos_anteriores = CCCPeriodo.query.filter_by(sector=sector).all()
    periodo_ids_anteriores = [p.id for p in periodos_anteriores]
    if periodo_ids_anteriores:
        CCCMovimiento.query.filter(CCCMovimiento.periodo_id.in_(periodo_ids_anteriores)).delete(synchronize_session=False)
        CCCPeriodo.query.filter(CCCPeriodo.id.in_(periodo_ids_anteriores)).delete(synchronize_session=False)

    periodo = CCCPeriodo(
        fecha_carga=date.today(),
        archivo=archivo[:255],
        sector=sector,
        usuario=usuario[:120] if usuario else None,
    )
    db.session.add(periodo)
    db.session.flush()

    codigos_cargados = set()

    for c in cuentas:
        codigo = (c.get("codigo") or "").strip()
        if not codigo:
            continue

        codigos_cargados.add(codigo)

        cuenta = CCCCuenta.query.filter_by(codigo=codigo).first()
        if not cuenta:
            cuenta = CCCCuenta(codigo=codigo)
            db.session.add(cuenta)

        cuenta.nombre = (c.get("nombre") or "").strip()
        cuenta.domicilio = (c.get("domicilio") or "").strip() or None
        cuenta.localidad = (c.get("localidad") or "").strip() or None
        cuenta.tipo = sector
        cuenta.saldo = ccc_decimal(c.get("saldo", 0))
        cuenta.fecha_actualizacion = date.today()

        CCCMovimiento.query.filter_by(cuenta_codigo=codigo, sector=sector).delete()

        for m in c.get("movimientos", []):
            mov = CCCMovimiento(
                cuenta_codigo=codigo,
                fecha=(m.get("fecha") or "").strip(),
                comprobante=(m.get("comprobante") or "").strip(),
                tipo=(m.get("tipo") or "").strip(),
                descripcion=(m.get("descripcion") or "").strip(),
                fecha_vto=(m.get("fechaVto") or m.get("fecha_vto") or "").strip(),
                debe=ccc_decimal(m.get("debe", 0)),
                haber=ccc_decimal(m.get("haber", 0)),
                saldo=ccc_decimal(m.get("saldo", 0)),
                periodo_id=periodo.id,
                sector=sector,
            )
            db.session.add(mov)

    db.session.commit()

    return jsonify({"ok": True, "periodo_id": periodo.id, "cuentas": len(codigos_cargados), "sector": sector})



@ccc_bp.route("/api/ccc/sector/<sector>", methods=["DELETE"])
@login_required
def ccc_delete_sector(sector):
    sector = (sector or "").strip().lower()

    sectores_validos = {"clientes", "orden_externos", "orden_socios", "socios_particulares", "telefonos"}
    if sector not in sectores_validos:
        return jsonify({"ok": False, "error": "Sector inválido"}), 400

    # Borrar movimientos por sector (incluye NDA-MANUAL con periodo_id=None)
    CCCMovimiento.query.filter_by(sector=sector).delete(synchronize_session=False)

    # Borrar periodos del sector
    CCCPeriodo.query.filter_by(sector=sector).delete(synchronize_session=False)

    # Borrar acciones de cuentas de este sector
    codigos = [c.codigo for c in CCCCuenta.query.filter_by(tipo=sector).all()]
    if codigos:
        CCCAccion.query.filter(CCCAccion.cuenta_codigo.in_(codigos)).delete(synchronize_session=False)

    # Eliminar las filas de CCCCuenta completamente (no solo resetear saldo)
    CCCCuenta.query.filter_by(tipo=sector).delete(synchronize_session=False)

    db.session.commit()

    return jsonify({"ok": True, "sector": sector, "eliminadas": len(codigos)})


@ccc_bp.route("/api/ccc/todo", methods=["DELETE"])
@login_required
def ccc_delete_all():
    CCCMovimiento.query.delete(synchronize_session=False)
    CCCPeriodo.query.delete(synchronize_session=False)
    CCCAccion.query.delete(synchronize_session=False)
    CCCCuenta.query.delete(synchronize_session=False)  # Eliminar cuentas completamente
    db.session.commit()
    return jsonify({"ok": True})


@ccc_bp.route("/api/ccc/cuentas")
@login_required
def ccc_listar_cuentas():
    tipo = (request.args.get("tipo") or "").strip()
    busqueda = (request.args.get("q") or "").strip()

    query = CCCCuenta.query
    if tipo:
        query = query.filter(CCCCuenta.tipo == tipo)
    if busqueda:
        like = f"%{busqueda}%"
        query = query.filter(or_(CCCCuenta.nombre.ilike(like), CCCCuenta.codigo.ilike(like)))

    cuentas = query.order_by(CCCCuenta.nombre.asc()).all()
    # Only return cuentas that have movimientos (defense against orphaned rows)
    result = []
    for c in cuentas:
        movs_count = CCCMovimiento.query.filter_by(cuenta_codigo=c.codigo).count()
        if movs_count > 0 or (c.estado_manual and c.estado_manual.strip()):
            result.append(ccc_serialize_cuenta(c))
    return jsonify(result)


@ccc_bp.route("/api/ccc/cuentas/<codigo>", methods=["GET", "PUT"])
@login_required
def ccc_cuenta_detalle(codigo):
    cuenta = CCCCuenta.query.filter_by(codigo=codigo).first_or_404()

    if request.method == "PUT":
        data = request.get_json(silent=True) or {}
        cuenta.estado_manual = (data.get("estado_manual") or "").strip() or None
        cuenta.obs_manual = (data.get("obs_manual") or "").strip() or None
        db.session.commit()
        return jsonify({"ok": True})

    return jsonify(ccc_serialize_cuenta(cuenta))


@ccc_bp.route("/api/ccc/accion", methods=["POST"])
@login_required
def ccc_accion():
    data = request.get_json(silent=True) or {}
    codigo = (data.get("codigo") or "").strip()
    tipo = (data.get("tipo") or "").strip()
    concepto = (data.get("concepto") or "").strip()
    monto = ccc_decimal(data.get("monto", 0))
    fecha_accion = (data.get("fecha") or date.today().isoformat()).strip()
    usuario = session.get("nombre") or session.get("username") or ""

    cuenta = CCCCuenta.query.filter_by(codigo=codigo).first_or_404()

    accion = CCCAccion(
        cuenta_codigo=codigo,
        fecha=fecha_accion,
        tipo=tipo[:30] if tipo else "obs",
        concepto=concepto,
        monto=monto,
        usuario=usuario[:120] if usuario else None,
    )
    db.session.add(accion)

    if tipo == "saldada":
        cuenta.estado_manual = "saldada"
        cuenta.obs_manual = concepto or "Marcada como saldada manualmente"
    elif tipo == "nda":
        mov = CCCMovimiento(
            cuenta_codigo=codigo,
            fecha=fecha_accion,
            comprobante="NDA-MANUAL",
            tipo="NDA",
            descripcion=concepto or "Nota de débito manual",
            fecha_vto="",
            debe=monto,
            haber=Decimal("0"),
            saldo=Decimal("0"),
            periodo_id=None,
        )
        db.session.add(mov)

    db.session.commit()
    return jsonify({"ok": True})


@ccc_bp.route("/api/ccc/accion/<int:accion_id>", methods=["DELETE"])
@login_required
def ccc_eliminar_accion(accion_id):
    accion = CCCAccion.query.get_or_404(accion_id)
    db.session.delete(accion)
    db.session.commit()
    return jsonify({"ok": True})




@ccc_bp.route("/api/ccc/resumen-mensual")
@login_required
def ccc_resumen_mensual():
    fecha_ref = (request.args.get("fecha") or date.today().isoformat()).strip()
    fecha = ccc_parse_date(fecha_ref) or date.today()
    return jsonify(ccc_month_summary(fecha.year, fecha.month))

@ccc_bp.route("/api/ccc/stats")
@login_required
def ccc_stats():
    fecha_ref_raw = (request.args.get("fecha") or date.today().isoformat()).strip()
    fecha_ref = ccc_parse_date(fecha_ref_raw) or date.today()

    cuentas = CCCCuenta.query.order_by(CCCCuenta.nombre.asc()).all()
    total_cuentas = len(cuentas)
    ultimo = CCCPeriodo.query.order_by(CCCPeriodo.id.desc()).first()

    ultimo_periodo = None
    if ultimo:
        ultimo_periodo = {
            "id": ultimo.id,
            "fecha_carga": ultimo.fecha_carga.isoformat() if ultimo.fecha_carga else "",
            "archivo": ultimo.archivo or "",
            "usuario": ultimo.usuario or "",
            "creado": ultimo.created_at.isoformat() if ultimo.created_at else "",
        }

    avisar = 0
    suspender = 0
    con_mora = 0
    al_dia = 0
    total_mora = Decimal("0")

    for cuenta in cuentas:
        bloques = ccc_build_blocks_for_cuenta(cuenta, fecha_ref=fecha_ref)
        cuenta_tiene_abierto = False
        cuenta_esta_al_dia = True

        for b in bloques:
            if b["pendiente"] <= 0:
                continue

            cuenta_tiene_abierto = True

            if b["estado"] == "avisar":
                avisar += 1
                cuenta_esta_al_dia = False
            elif b["estado"] == "suspender":
                suspender += 1
                cuenta_esta_al_dia = False
            elif b["estado"] == "con_mora":
                con_mora += 1
                cuenta_esta_al_dia = False
            elif b["estado"] in ("pendiente", "vence_hoy"):
                cuenta_esta_al_dia = False

            total_mora += Decimal(str(b["total_mora"]))

        if not cuenta_tiene_abierto or cuenta_esta_al_dia:
            al_dia += 1

    return jsonify({
        "total_cuentas": total_cuentas,
        "ultimo_periodo": ultimo_periodo,
        "fecha_referencia": fecha_ref.isoformat(),
        "avisar": avisar,
        "suspender": suspender,
        "con_mora": con_mora,
        "al_dia": al_dia,
        "total_mora": float(quantize_money(total_mora)),
    })
    
@ccc_bp.route("/api/ccc/mensaje", methods=["GET"])
@login_required
def ccc_get_message():
    vencimiento = (request.args.get("vencimiento") or "").strip()

    return jsonify({
        "template": ccc_get_message_template(),
        "preview": ccc_format_message(vencimiento),
    })


@ccc_bp.route("/api/ccc/mensaje", methods=["POST"])
@login_required
def ccc_save_message():
    data = request.get_json(silent=True) or {}
    texto = (data.get("mensaje") or "").strip()

    if not texto:
        return jsonify({"ok": False, "error": "El mensaje no puede quedar vacío."}), 400

    ccc_set_message_template(texto)

    return jsonify({
        "ok": True,
        "template": texto,
    })

# =========================
