from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation

from extensions import db
from models import AppConfig, CCCCuenta, CCCMovimiento, CCCAccion
from utils import to_decimal, quantize_money


def ccc_float(value, default=0.0):
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def ccc_decimal(value, default="0"):
    try:
        if value is None or value == "":
            value = default
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(str(default))


def ccc_serialize_movimiento(m):
    return {
        "id": m.id,
        "cuenta_codigo": m.cuenta_codigo,
        "fecha": m.fecha or "",
        "comprobante": m.comprobante or "",
        "tipo": m.tipo or "",
        "descripcion": m.descripcion or "",
        "fecha_vto": m.fecha_vto or "",
        "debe": float(m.debe or 0),
        "haber": float(m.haber or 0),
        "saldo": float(m.saldo or 0),
        "periodo_id": m.periodo_id,
    }


def ccc_serialize_accion(a):
    return {
        "id": a.id,
        "cuenta_codigo": a.cuenta_codigo,
        "fecha": a.fecha or "",
        "tipo": a.tipo or "",
        "concepto": a.concepto or "",
        "monto": float(a.monto or 0),
        "usuario": a.usuario or "",
        "creado": a.created_at.isoformat() if a.created_at else "",
    }


def ccc_serialize_cuenta(c):
    return {
        "id": c.id,
        "codigo": c.codigo,
        "nombre": c.nombre or "",
        "domicilio": c.domicilio or "",
        "localidad": c.localidad or "",
        "tipo": c.tipo or "clientes",
        "saldo": float(c.saldo or 0),
        "estado_manual": c.estado_manual or "",
        "obs_manual": c.obs_manual or "",
        "fecha_actualizacion": c.fecha_actualizacion.isoformat() if c.fecha_actualizacion else "",
        "movimientos": [ccc_serialize_movimiento(m) for m in CCCMovimiento.query.filter_by(cuenta_codigo=c.codigo).order_by(CCCMovimiento.id.asc()).all()],
        "acciones": [ccc_serialize_accion(a) for a in CCCAccion.query.filter_by(cuenta_codigo=c.codigo).order_by(CCCAccion.created_at.desc()).all()],
    }


def ccc_parse_date(value):
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def ccc_block_due_date(fecha_mov):
    if not fecha_mov:
        return None

    dia = fecha_mov.day

    if 1 <= dia <= 7:
        return fecha_mov.replace(day=9)

    if 8 <= dia <= 15:
        return fecha_mov.replace(day=17)

    if 16 <= dia <= 22:
        return fecha_mov.replace(day=24)

    # 23 al 31 -> día 2 del mes siguiente
    if fecha_mov.month == 12:
        return date(fecha_mov.year + 1, 1, 2)
    return date(fecha_mov.year, fecha_mov.month + 1, 2)


def ccc_rules_for_tipo(tipo):
    tipo = (tipo or "clientes").strip().lower()

    if tipo == "clientes":
        return {"aviso": True, "mora": True, "suspension": True}

    if tipo == "orden_externos":
        return {"aviso": True, "mora": False, "suspension": False}

    if tipo == "orden_socios":
        return {"aviso": True, "mora": False, "suspension": False}

    if tipo == "socios_particulares":
        return {"aviso": True, "mora": False, "suspension": False}

    if tipo == "telefonos":
        return {"aviso": True, "mora": False, "suspension": False}

    return {"aviso": True, "mora": True, "suspension": True}


def ccc_calc_coef(dias_vencidos, tasa_mensual=Decimal("0.07")):
    if dias_vencidos <= 0:
        return Decimal("0")
    # Fórmula EXACTA del Excel:
    # =POTENCIA(((tasa/30)+1);dias)-1
    return (((tasa_mensual / Decimal("30")) + Decimal("1")) ** Decimal(dias_vencidos)) - Decimal("1")


def ccc_calc_mora(monto, dias_vencidos, tasa_mensual=Decimal("0.07")):
    monto = to_decimal(monto)
    if monto <= 0 or dias_vencidos <= 0:
        return {
            "coeficiente": Decimal("0"),
            "interes": Decimal("0"),
            "iva": Decimal("0"),
            "total": Decimal("0"),
        }

    coef = ccc_calc_coef(dias_vencidos, tasa_mensual)
    interes = quantize_money(monto * coef)
    iva = quantize_money(interes * Decimal("0.21"))
    total = quantize_money(interes + iva)

    return {
        "coeficiente": coef,
        "interes": interes,
        "iva": iva,
        "total": total,
    }


def ccc_estado_para_bloque(tipo, fecha_vto, fecha_ref):
    if not fecha_vto:
        return "sin_vencimiento"

    dias = (fecha_ref - fecha_vto).days
    reglas = ccc_rules_for_tipo(tipo)

    if dias < 0:
        return "al_dia"

    if dias == 0:
        return "vence_hoy"

    # Día 3 -> avisar
    if dias >= 3 and reglas["aviso"]:
        if reglas["suspension"] and dias >= 4:
            return "suspender"
        if reglas["mora"]:
            return "avisar"
        return "avisar"

    # Clientes: si pasó el vto pero todavía no llegó al aviso, muestra con mora
    if dias > 0 and reglas["mora"]:
        return "con_mora"

    return "pendiente"


def ccc_build_blocks_for_cuenta(cuenta, fecha_ref=None):
    if fecha_ref is None:
        fecha_ref = date.today()

    movimientos = (
        CCCMovimiento.query
        .filter_by(cuenta_codigo=cuenta.codigo)
        .order_by(CCCMovimiento.id.asc())
        .all()
    )

    bloques = {}
    pagos = []

    for m in movimientos:
        fecha_mov = ccc_parse_date(m.fecha)
        if not fecha_mov:
            continue

        tipo_mov = (m.tipo or "").strip().upper()
        debe = to_decimal(m.debe)
        haber = to_decimal(m.haber)

        # pagos/recibos
        if haber > 0:
            pagos.append(haber)
            continue

        # solo deuda real
        if debe <= 0:
            continue

        # ignorar NDA para el cálculo base del panel
        if tipo_mov == "NDA":
            continue

        fecha_vto = ccc_block_due_date(fecha_mov)
        if not fecha_vto:
            continue

        key = fecha_vto.isoformat()

        if key not in bloques:
            bloques[key] = {
                "fecha_vto": fecha_vto,
                "monto": Decimal("0"),
                "movimientos": [],
            }

        bloques[key]["monto"] += debe
        bloques[key]["movimientos"].append(m)

    # aplicar pagos FIFO por bloque vencimiento más viejo primero
    bloques_ordenados = sorted(bloques.values(), key=lambda x: x["fecha_vto"])
    total_pago = sum(pagos, Decimal("0"))

    for b in bloques_ordenados:
        monto = b["monto"]
        aplicado = min(monto, total_pago) if total_pago > 0 else Decimal("0")
        pendiente = monto - aplicado
        total_pago -= aplicado

        dias = max((fecha_ref - b["fecha_vto"]).days, 0)
        reglas = ccc_rules_for_tipo(cuenta.tipo)

        mora = {"coeficiente": Decimal("0"), "interes": Decimal("0"), "iva": Decimal("0"), "total": Decimal("0")}
        if pendiente > 0 and reglas["mora"] and dias > 0:
            mora = ccc_calc_mora(pendiente, dias)

        b["aplicado"] = quantize_money(aplicado)
        b["pendiente"] = quantize_money(pendiente)
        b["dias"] = dias
        b["estado"] = ccc_estado_para_bloque(cuenta.tipo, b["fecha_vto"], fecha_ref) if pendiente > 0 else "saldado"
        b["coeficiente"] = float(mora["coeficiente"])
        b["interes"] = float(mora["interes"])
        b["iva"] = float(mora["iva"])
        b["total_mora"] = float(mora["total"])
        b["monto"] = float(quantize_money(monto))
        b["aplicado_float"] = float(b["aplicado"])
        b["pendiente_float"] = float(b["pendiente"])

    return bloques_ordenados


def ccc_month_summary(year, month):
    movimientos = CCCMovimiento.query.all()
    cuentas = {c.codigo: c for c in CCCCuenta.query.all()}

    facturado = Decimal("0")
    cobrado = Decimal("0")
    combustible_facturado = Decimal("0")
    combustible_cobrado = Decimal("0")

    tipos_factura = {"FAA", "FAB", "FAC", "FAD"}
    tipos_cobro = {"REC", "NDC"}

    for m in movimientos:
        fecha = ccc_parse_date(m.fecha)
        if not fecha or fecha.year != year or fecha.month != month:
            continue

        tipo = (m.tipo or "").upper().strip()
        cuenta = cuentas.get(m.cuenta_codigo)
        cuenta_tipo = (cuenta.tipo if cuenta else "clientes")

        if tipo in tipos_factura and to_decimal(m.debe) > 0:
            facturado += to_decimal(m.debe)
            if cuenta_tipo in {"clientes", "orden_externos", "orden_socios"}:
                combustible_facturado += to_decimal(m.debe)

        if tipo in tipos_cobro and to_decimal(m.haber) > 0:
            cobrado += to_decimal(m.haber)
            if cuenta_tipo in {"clientes", "orden_externos", "orden_socios"}:
                combustible_cobrado += to_decimal(m.haber)

    pendiente = Decimal("0")
    pendiente_combustible = Decimal("0")
    mora_total = Decimal("0")
    iva_mora_total = Decimal("0")
    avisos = 0
    suspendibles = 0
    al_dia = 0

    if year == date.today().year and month == date.today().month:
        fecha_ref = date.today()
    else:
        if month == 12:
            fecha_ref = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            fecha_ref = date(year, month + 1, 1) - timedelta(days=1)

    for c in cuentas.values():
        saldo = to_decimal(c.saldo)
        if saldo > 0:
            pendiente += saldo
            if (c.tipo or "clientes") in {"clientes", "orden_externos", "orden_socios"}:
                pendiente_combustible += saldo

        bloques = ccc_build_blocks_for_cuenta(c, fecha_ref=fecha_ref)
        tiene_abierto = False

        for b in bloques:
            if b["pendiente"] > 0:
                tiene_abierto = True
                mora_total += Decimal(str(b["interes"]))
                iva_mora_total += Decimal(str(b["iva"]))
                if b["estado"] == "avisar":
                    avisos += 1
                if b["estado"] == "suspender":
                    suspendibles += 1

        if not tiene_abierto:
            al_dia += 1

    return {
        "facturado": float(quantize_money(facturado)),
        "cobrado": float(quantize_money(cobrado)),
        "pendiente": float(quantize_money(pendiente)),
        "combustible_facturado": float(quantize_money(combustible_facturado)),
        "combustible_cobrado": float(quantize_money(combustible_cobrado)),
        "combustible_pendiente": float(quantize_money(pendiente_combustible)),
        "mora_total": float(quantize_money(mora_total)),
        "iva_mora_total": float(quantize_money(iva_mora_total)),
        "avisos": avisos,
        "suspendibles": suspendibles,
        "al_dia": al_dia,
    }

def ccc_get_message_template():
    item = db.session.get(AppConfig, "ccc_message_template")
    if item and (item.value or "").strip():
        return item.value

    return """Buenos días,

Adjuntamos el resumen de cuenta corriente.

Recordamos que el plazo de vencimiento es hasta el día {VENCIMIENTO}.

Muchas gracias.
Saludos."""


def ccc_set_message_template(texto):
    texto = (texto or "").strip()

    item = db.session.get(AppConfig, "ccc_message_template")
    if not item:
        item = AppConfig(key="ccc_message_template", value=texto)
        db.session.add(item)
    else:
        item.value = texto

    db.session.commit()


def ccc_format_message(vencimiento_texto=None):
    plantilla = ccc_get_message_template()
    vencimiento_texto = (vencimiento_texto or "").strip() or "[COMPLETAR]"
    return plantilla.replace("{VENCIMIENTO}", vencimiento_texto)

def parse_liquidacion_pdf(file_storage):
    import re
    from decimal import Decimal
    from PyPDF2 import PdfReader

    def normalize_spaces(value):
        return " ".join((value or "").replace("\xa0", " ").split())

    def parse_local_decimal(value):
        value = normalize_spaces(value)
        if not value:
            return Decimal("0")
        value = value.replace(".", "").replace(",", ".")
        return Decimal(value)

    reader = PdfReader(file_storage)
    raw_pages = [page.extract_text() or "" for page in reader.pages]
    text = "\n".join(raw_pages)
    lines = [normalize_spaces(line) for line in text.splitlines() if normalize_spaces(line)]

print("====== LINEAS LIQUIDACION PDF ======")
for idx, line in enumerate(lines[:80], start=1):
    print(f"{idx:02d}: {line}")
print("====== FIN LINEAS LIQUIDACION PDF ======")

    numero = None
    fecha = None
    fletero = None
    total_bruto = Decimal("0")
    items = []

    # -------- cabecera --------
    for i, line in enumerate(lines):
        upper = line.upper()

        if line == "Número:" and i + 1 < len(lines):
            numero = lines[i + 1]
        elif line.startswith("Número:"):
            numero = normalize_spaces(line.replace("Número:", "").strip())

        if line == "Fecha :" and i + 1 < len(lines):
            fecha = lines[i + 1]
        elif line.startswith("Fecha"):
            m_fecha = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", line)
            if m_fecha:
                fecha = m_fecha.group(1)

        if upper == "NOMBRE" and i + 1 < len(lines):
            fletero = lines[i + 1]

    # -------- total --------
    for i, line in enumerate(lines):
        if line.upper() == "TOTAL" and i + 1 < len(lines):
            total_bruto = parse_local_decimal(lines[i + 1])
            break

    # -------- items --------
    i = 0
    while i < len(lines):
        line = lines[i]

        # renglón con fecha + destino
        m_fecha_dest = re.match(r"^(\d{1,2}/\d{1,2}/\d{4})\s+00:00:00\s+(.+)$", line)
        if not m_fecha_dest:
            i += 1
            continue

        fecha_item = m_fecha_dest.group(1)
        destino = normalize_spaces(m_fecha_dest.group(2))

        if i + 2 >= len(lines):
            i += 1
            continue

        linea_cliente = lines[i + 1]
        linea_datos = lines[i + 2]

        # Línea cliente / chofer / mercadería
        m_cli = re.search(
            r"Cliente:\s*(.*?)\s+Chofer:\s*(.*?)\s+Mercadería:\s*(.*)$",
            linea_cliente,
            re.IGNORECASE
        )

        # Línea nro viaje / ctg / origen / kilos / tarifa / kms / importe
        m_det = re.match(
            r"^(\d+)\s+(\d+)\s+([A-Z]+)\s+([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)\s+([\d\.,]+)$",
            linea_datos
        )

        if m_cli and m_det:
            cliente = normalize_spaces(m_cli.group(1))
            chofer = normalize_spaces(m_cli.group(2))
            mercaderia = normalize_spaces(m_cli.group(3))

            nro_viaje = m_det.group(1)
            ctg = m_det.group(2)
            origen = normalize_spaces(m_det.group(3))
            kilos = parse_local_decimal(m_det.group(4))
            tarifa = parse_local_decimal(m_det.group(5))
            kms = parse_local_decimal(m_det.group(6))
            importe = parse_local_decimal(m_det.group(7))

            items.append({
                "fecha": fecha_item,
                "destino": destino,
                "cliente": cliente,
                "chofer": chofer,
                "fletero": chofer,
                "mercaderia": mercaderia,
                "nro_viaje": nro_viaje,
                "ctg": ctg,
                "origen": origen,
                "kg": kilos,
                "tarifa": tarifa,
                "kms": kms,
                "importe": importe,
            })
            i += 3
            continue

        i += 1

    # si no agarró el nombre de arriba, usamos el chofer del primer item
    if not fletero and items:
        fletero = items[0]["fletero"]

    return {
        "numero": numero,
        "fecha": fecha,
        "fletero": fletero,
        "items": items,
        "total_bruto": total_bruto,
    }
