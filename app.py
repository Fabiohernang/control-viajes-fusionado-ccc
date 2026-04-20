import os
import csv
import io
import re
from functools import wraps
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from pypdf import PdfReader

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response, session, g
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, func, text
from werkzeug.security import generate_password_hash, check_password_hash

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "cambiar-esto-en-produccion")
app.config["SESSION_PERMANENT"] = False
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)

database_url = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'app.db')}")
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)


# =========================
# MODELOS
# =========================

class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False, unique=True, index=True)
    nombre = db.Column(db.String(120), nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    activo = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str):
        return check_password_hash(self.password_hash, password)


class AppConfig(db.Model):
    __tablename__ = "app_config"
    key = db.Column(db.String(100), primary_key=True)
    value = db.Column(db.String(255), nullable=False)


class Productor(db.Model):
    __tablename__ = "productores"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), nullable=False, unique=True, index=True)
    activo = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class FleteroMaster(db.Model):
    __tablename__ = "fleteros_master"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), nullable=False, unique=True, index=True)
    activo = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Tarifario(db.Model):
    __tablename__ = "tarifario"

    id = db.Column(db.Integer, primary_key=True)
    km = db.Column(db.Integer, nullable=False, unique=True, index=True)
    tarifa_tn = db.Column(db.Numeric(14, 2), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Viaje(db.Model):
    __tablename__ = "viajes"

    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False, index=True)
    cliente = db.Column(db.String(200), nullable=False, index=True)
    factura = db.Column(db.String(50), nullable=True, index=True)
    fletero = db.Column(db.String(200), nullable=False, index=True)
    socio = db.Column(db.Boolean, nullable=False, default=False)
    ctg = db.Column(db.String(50), nullable=True)
    origen = db.Column(db.String(120), nullable=True)
    destino = db.Column(db.String(120), nullable=True)
    kilometros = db.Column(db.Numeric(12, 2), nullable=True)
    tarifa = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    descuento = db.Column(db.Numeric(12, 4), nullable=False, default=0)
    kg = db.Column(db.Numeric(12, 4), nullable=False, default=0)
    liquidado = db.Column(db.Boolean, nullable=False, default=False)

    total_importe = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    importe_con_iva = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    comision = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    comision_lucas = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    observaciones = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def recalcular(
        self,
        iva=Decimal("0.21"),
        socio_rate=Decimal("0.06"),
        no_socio_rate=Decimal("0.10"),
        lucas_rate=Decimal("0.015"),
    ):
        tarifa = to_decimal(self.tarifa)
        descuento = to_decimal(self.descuento)
        kg = to_decimal(self.kg)

        total = (tarifa - (tarifa * descuento)) * kg
        iva_total = total * (Decimal("1.00") + iva)
        rate = socio_rate if self.socio else no_socio_rate
        comision = iva_total * rate
        comision_lucas = total * lucas_rate

        self.total_importe = quantize_money(total)
        self.importe_con_iva = quantize_money(iva_total)
        self.comision = quantize_money(comision)
        self.comision_lucas = quantize_money(comision_lucas)


class Factura(db.Model):
    __tablename__ = "facturas"

    id = db.Column(db.Integer, primary_key=True)
    numero_factura = db.Column(db.String(30), nullable=False, unique=True, index=True)
    fecha = db.Column(db.Date, nullable=False, index=True)
    fecha_vencimiento = db.Column(db.Date, nullable=False, index=True)
    cliente = db.Column(db.String(200), nullable=False, index=True)

    importe_neto = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    iva = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    percepciones = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    importe_total = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    estado_pago = db.Column(db.String(20), nullable=False, default="pendiente", index=True)
    observaciones = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    aplicaciones = db.relationship(
        "PagoAplicacion",
        back_populates="factura",
        cascade="all, delete-orphan",
        lazy=True,
    )

    @property
    def pago_acumulado(self):
        return quantize_money(sum((to_decimal(a.importe_pago) for a in self.aplicaciones), Decimal("0")))

    @property
    def retenciones_acumuladas(self):
        return quantize_money(sum((to_decimal(a.importe_retenciones) for a in self.aplicaciones), Decimal("0")))

    @property
    def total_aplicado(self):
        return quantize_money(sum((to_decimal(a.total_aplicado) for a in self.aplicaciones), Decimal("0")))

    @property
    def saldo_pendiente(self):
        saldo = to_decimal(self.importe_total) - self.total_aplicado
        if saldo < 0:
            saldo = Decimal("0")
        return quantize_money(saldo)

    @property
    def cantidad_viajes(self):
        return Viaje.query.filter_by(factura=self.numero_factura).count()

    @property
    def vencida(self):
        return self.estado_pago != "pagada" and self.fecha_vencimiento < date.today() and self.saldo_pendiente > 0

    @property
    def dias_vencida(self):
        if not self.vencida:
            return 0
        return (date.today() - self.fecha_vencimiento).days

    @property
    def ultima_fecha_pago(self):
        fechas = [a.pago.fecha_pago for a in self.aplicaciones if a.pago and a.pago.fecha_pago]
        return max(fechas) if fechas else None

    @property
    def total_pendiente_cliente(self):
        # Optimized: uses aggregate query instead of loading all facturas
        from sqlalchemy import func as _func
        result = db.session.query(
            _func.coalesce(_func.sum(Factura.importe_total), 0)
        ).filter(
            Factura.cliente == self.cliente,
            Factura.estado_pago != "pagada"
        ).scalar()
        return quantize_money(to_decimal(result))


class Pago(db.Model):
    __tablename__ = "pagos"

    id = db.Column(db.Integer, primary_key=True)
    fecha_pago = db.Column(db.Date, nullable=False, index=True)
    fecha_cobro_real = db.Column(db.Date, nullable=True, index=True)
    productor = db.Column(db.String(200), nullable=False, index=True)

    medio_pago = db.Column(db.String(50), nullable=False, default="Transferencia")
    numero_referencia = db.Column(db.String(100), nullable=True)

    importe = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    retenciones = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    total_aplicable = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    observaciones = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    aplicaciones = db.relationship(
        "PagoAplicacion",
        back_populates="pago",
        cascade="all, delete-orphan",
        lazy=True,
    )

    saldo_favor = db.relationship(
        "SaldoFavor",
        back_populates="pago_origen",
        cascade="all, delete-orphan",
        lazy=True,
    )


class PagoAplicacion(db.Model):
    __tablename__ = "pago_aplicaciones"

    id = db.Column(db.Integer, primary_key=True)
    pago_id = db.Column(db.Integer, db.ForeignKey("pagos.id"), nullable=False, index=True)
    factura_id = db.Column(db.Integer, db.ForeignKey("facturas.id"), nullable=False, index=True)

    importe_pago = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    importe_retenciones = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    total_aplicado = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    pago = db.relationship("Pago", back_populates="aplicaciones")
    factura = db.relationship("Factura", back_populates="aplicaciones")


class SaldoFavor(db.Model):
    __tablename__ = "saldos_favor"

    id = db.Column(db.Integer, primary_key=True)
    productor = db.Column(db.String(200), nullable=False, index=True)
    pago_origen_id = db.Column(db.Integer, db.ForeignKey("pagos.id"), nullable=True, index=True)
    importe = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    aplicado = db.Column(db.Boolean, nullable=False, default=False)
    observaciones = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    pago_origen = db.relationship("Pago", back_populates="saldo_favor")


class CajaMovimiento(db.Model):
    __tablename__ = "caja_movimientos"

    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False, index=True)
    tipo = db.Column(db.String(20), nullable=False, index=True)  # ingreso | egreso
    concepto = db.Column(db.String(200), nullable=True, index=True)
    medio = db.Column(db.String(50), nullable=True)  # efectivo, transferencia, cheque
    importe = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    observaciones = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class CuotaSeguro(db.Model):
    __tablename__ = "cuotas_seguros"

    id = db.Column(db.Integer, primary_key=True)
    periodo = db.Column(db.Date, nullable=False, index=True)  # primer día del mes
    fletero = db.Column(db.String(200), nullable=False, index=True)
    cuota_social = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    seguro_carga = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    seguro_accidentes = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    seguro_particular = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    otros_descuentos = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    observaciones = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    @property
    def total(self):
        return quantize_money(
            to_decimal(self.cuota_social)
            + to_decimal(self.seguro_carga)
            + to_decimal(self.seguro_accidentes)
            + to_decimal(self.seguro_particular)
            + to_decimal(self.otros_descuentos)
        )

class LiquidacionFletero(db.Model):
    __tablename__ = "liquidaciones_fletero"

    id = db.Column(db.Integer, primary_key=True)
    fecha = db.Column(db.Date, nullable=False, index=True)
    fletero = db.Column(db.String(200), nullable=False, index=True)
    factura_fletero = db.Column(db.String(50), nullable=True, index=True)
    observaciones = db.Column(db.Text, nullable=True)

    total_bruto = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    total_descuentos = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    neto_pagar = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    estado = db.Column(db.String(20), nullable=False, default="pendiente", index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    items = db.relationship(
        "LiquidacionItem",
        back_populates="liquidacion",
        cascade="all, delete-orphan",
        lazy=True,
    )
    descuentos = db.relationship(
        "LiquidacionDescuento",
        back_populates="liquidacion",
        cascade="all, delete-orphan",
        lazy=True,
    )
    pagos = db.relationship(
        "LiquidacionPago",
        back_populates="liquidacion",
        cascade="all, delete-orphan",
        lazy=True,
    )

    @property
    def total_pagado(self):
        return quantize_money(sum((to_decimal(p.importe) for p in self.pagos), Decimal("0")))

    @property
    def saldo_pendiente(self):
        saldo = to_decimal(self.neto_pagar) - self.total_pagado
        if saldo < 0:
            saldo = Decimal("0")
        return quantize_money(saldo)


class LiquidacionItem(db.Model):
    __tablename__ = "liquidacion_items"

    id = db.Column(db.Integer, primary_key=True)
    liquidacion_id = db.Column(db.Integer, db.ForeignKey("liquidaciones_fletero.id"), nullable=False, index=True)
    viaje_id = db.Column(db.Integer, db.ForeignKey("viajes.id"), nullable=False, index=True)
    importe = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    liquidacion = db.relationship("LiquidacionFletero", back_populates="items")
    viaje = db.relationship("Viaje")


class LiquidacionDescuento(db.Model):
    __tablename__ = "liquidacion_descuentos"

    id = db.Column(db.Integer, primary_key=True)
    liquidacion_id = db.Column(db.Integer, db.ForeignKey("liquidaciones_fletero.id"), nullable=False, index=True)
    concepto = db.Column(db.String(100), nullable=False, index=True)
    importe = db.Column(db.Numeric(14, 2), nullable=False, default=0)

    liquidacion = db.relationship("LiquidacionFletero", back_populates="descuentos")


class LiquidacionPago(db.Model):
    __tablename__ = "liquidacion_pagos"

    id = db.Column(db.Integer, primary_key=True)
    liquidacion_id = db.Column(db.Integer, db.ForeignKey("liquidaciones_fletero.id"), nullable=False, index=True)
    fecha = db.Column(db.Date, nullable=False)
    medio = db.Column(db.String(50), nullable=False)
    numero = db.Column(db.String(100), nullable=True)
    importe = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    observaciones = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    liquidacion = db.relationship("LiquidacionFletero", back_populates="pagos")



class CCCPeriodo(db.Model):
    __tablename__ = "ccc_periodos"

    id = db.Column(db.Integer, primary_key=True)
    fecha_carga = db.Column(db.Date, nullable=False, default=date.today)
    archivo = db.Column(db.String(255), nullable=False, default="")
    sector = db.Column(db.String(50), nullable=False, default="clientes", index=True)
    usuario = db.Column(db.String(120), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class CCCCuenta(db.Model):
    __tablename__ = "ccc_cuentas"

    id = db.Column(db.Integer, primary_key=True)
    codigo = db.Column(db.String(50), nullable=False, unique=True, index=True)
    nombre = db.Column(db.String(255), nullable=False, default="")
    domicilio = db.Column(db.String(255), nullable=True)
    localidad = db.Column(db.String(255), nullable=True)
    tipo = db.Column(db.String(50), nullable=False, default="clientes")  # clientes, orden_externos, orden_socios, socios_particulares, telefonos
    saldo = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    estado_manual = db.Column(db.String(50), nullable=True)
    obs_manual = db.Column(db.Text, nullable=True)
    fecha_actualizacion = db.Column(db.Date, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class CCCMovimiento(db.Model):
    __tablename__ = "ccc_movimientos"

    id = db.Column(db.Integer, primary_key=True)
    cuenta_codigo = db.Column(db.String(50), db.ForeignKey("ccc_cuentas.codigo"), nullable=False, index=True)
    fecha = db.Column(db.String(20), nullable=True)
    comprobante = db.Column(db.String(80), nullable=True)
    tipo = db.Column(db.String(20), nullable=True)
    descripcion = db.Column(db.Text, nullable=True)
    fecha_vto = db.Column(db.String(20), nullable=True)
    debe = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    haber = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    saldo = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    periodo_id = db.Column(db.Integer, db.ForeignKey("ccc_periodos.id"), nullable=True)
    sector = db.Column(db.String(50), nullable=False, default="clientes", index=True)

    cuenta = db.relationship("CCCCuenta", backref=db.backref("movimientos_rel", lazy="dynamic"))
    periodo = db.relationship("CCCPeriodo", backref=db.backref("movimientos_rel", lazy="dynamic"))


class CCCAccion(db.Model):
    __tablename__ = "ccc_acciones"

    id = db.Column(db.Integer, primary_key=True)
    cuenta_codigo = db.Column(db.String(50), db.ForeignKey("ccc_cuentas.codigo"), nullable=False, index=True)
    fecha = db.Column(db.String(20), nullable=True)
    tipo = db.Column(db.String(30), nullable=False)  # saldada, nda, obs, pago_parcial
    concepto = db.Column(db.Text, nullable=True)
    monto = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    usuario = db.Column(db.String(120), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    cuenta = db.relationship("CCCCuenta", backref=db.backref("acciones_rel", lazy="dynamic"))

# =========================
# HELPERS
# =========================

def to_decimal(value, default="0"):
    if value is None or value == "":
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def quantize_money(value):
    return value.quantize(Decimal("0.01"))


def get_config_decimal(key, default):
    item = db.session.get(AppConfig, key)
    return to_decimal(item.value if item else default)


def set_default_config():
    defaults = {
        "iva_rate": "0.21",
        "socio_commission_rate": "0.06",
        "no_socio_commission_rate": "0.10",
        "lucas_commission_rate": "0.015",
        "ccc_message_template": """Buenos días,

Adjuntamos el resumen de cuenta corriente.

Recordamos que el plazo de vencimiento es hasta el día {VENCIMIENTO}.

Muchas gracias.
Saludos.""",
    }
    changed = False
    for key, value in defaults.items():
        if not db.session.get(AppConfig, key):
            db.session.add(AppConfig(key=key, value=value))
            changed = True
    if changed:
        db.session.commit()




def parse_date_safe(value):
    value = (value or "").strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def normalize_spaces(value):
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def normalize_title_keep_upper(value):
    value = normalize_spaces(value)
    if not value:
        return value
    return " ".join(word if word.isupper() else word.capitalize() for word in value.split())


def parse_local_decimal(value, default="0"):
    value = normalize_spaces(value)
    if not value:
        return Decimal(default)
    cleaned = value.replace(".", "").replace(",", ".")
    return to_decimal(cleaned, default)


def extract_first_match(pattern, text, flags=0, group=1, default=None):
    match = re.search(pattern, text, flags)
    if not match:
        return default
    return match.group(group).strip()


def parse_amount_from_lines(lines, keyword):
    for idx, line in enumerate(lines):
        if keyword not in line:
            continue

        candidates = [line]
        if idx + 1 < len(lines):
            candidates.append(lines[idx + 1])
        if idx + 2 < len(lines):
            candidates.append(lines[idx + 2])

        for candidate in candidates:
            nums = re.findall(r"\$?\s*([\d\.,]+)", candidate)
            nums = [n for n in nums if any(ch.isdigit() for ch in n)]
            if nums:
                return parse_local_decimal(nums[-1])
    return Decimal("0")


def parse_factura_pdf(file_storage):
    reader = PdfReader(file_storage)
    layout_pages = [page.extract_text(extraction_mode="layout") or "" for page in reader.pages]
    raw_pages = [page.extract_text() or "" for page in reader.pages]

    layout_text = "\n".join(layout_pages)
    compact_text = normalize_spaces(" ".join(raw_pages))
    layout_lines = [line.strip() for line in layout_text.splitlines()]

    numero_factura = extract_first_match(r"N[º°]\s*([0-9]{4}-[0-9]{8})", layout_text)
    fecha_factura = parse_date_safe(extract_first_match(r"FECHA:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", layout_text, default=""))
    fecha_vencimiento = parse_date_safe(extract_first_match(r"Fecha de Vencimiento\s*:?[ ]*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", layout_text, default=""))

    cliente = normalize_spaces(extract_first_match(r"SEÑOR/ES:\s*(.*?)\s*Cliente Nº:", layout_text, flags=re.S, default=""))
    cliente_numero = extract_first_match(r"Cliente Nº:\s*([0-9\.]+)", layout_text, default="")
    cuit_cliente = extract_first_match(r"([0-9]{2}-[0-9]{8}-[0-9])C\.U\.I\.T\.", layout_text, default="")
    condicion_pago = extract_first_match(r"Condición de Pago:\s*(.*?)\s*Fecha de Vencimiento", layout_text, flags=re.S, default="")

    subtotal = parse_amount_from_lines(layout_lines, "Subtotal")
    iva = parse_amount_from_lines(layout_lines, "I.V.A. INSC %")
    percepciones = parse_amount_from_lines(layout_lines, "PERC. IIBB")
    impuesto = parse_amount_from_lines(layout_lines, "IMPUESTO")
    total = Decimal("0")
    for line in layout_lines:
        if "TOTAL" in line and "$" in line:
            nums = re.findall(r"\$\s*([\d\.,]+)", line)
            if nums:
                total = parse_local_decimal(nums[-1])
                break
    if total == 0:
        total = subtotal + iva + percepciones + impuesto

    item_pattern = re.compile(r"1,00\s+([\d\.,]+)\s+([\d\.,]+)\s*(Socio .*?)CTG:\s*(\d+)", re.S)
    detail_pattern = re.compile(
        r"Socio\s+(.*?),\s*desde\s+(.*?)\s+hasta\s+(.*?)\s+\((\d+)km\.\)\s+([\d\.]+)\s+kg de\s+([A-ZÁÉÍÓÚÑ ]+)\.\s*Tarifa\s*\$([\d\.]+)",
        re.I
    )

    items = []
    for match in item_pattern.finditer(compact_text):
        importe_total = parse_local_decimal(match.group(2))
        body = normalize_spaces(match.group(3))
        ctg = match.group(4)
        detail = detail_pattern.search(body)
        if not detail:
            continue

        fletero_raw, origen, destino, km, kg_raw, producto, tarifa_raw = detail.groups()
        fletero = normalize_title_keep_upper(fletero_raw)
        origen = normalize_title_keep_upper(origen)
        destino = normalize_title_keep_upper(destino)
        producto = normalize_title_keep_upper(producto)

        kg_decimal = to_decimal(kg_raw, "0")
        kg_tn = quantize_money(kg_decimal / Decimal("1000"))
        tarifa = to_decimal(tarifa_raw, "0")

        items.append({
            "fletero": fletero,
            "socio": True,
            "origen": origen,
            "destino": destino,
            "kilometros": int(km),
            "kg": float(kg_tn),
            "kg_bruto": kg_raw,
            "producto": producto,
            "tarifa": str(tarifa),
            "ctg": ctg,
            "importe_total": str(importe_total),
        })

    parsed = {
        "numero_factura": numero_factura or "",
        "fecha": fecha_factura.isoformat() if fecha_factura else "",
        "fecha_vencimiento": fecha_vencimiento.isoformat() if fecha_vencimiento else "",
        "cliente": cliente,
        "cliente_numero": cliente_numero,
        "cuit_cliente": cuit_cliente,
        "condicion_pago": condicion_pago,
        "subtotal": str(quantize_money(subtotal)),
        "iva": str(quantize_money(iva)),
        "percepciones": str(quantize_money(percepciones + impuesto)),
        "impuesto": str(quantize_money(impuesto)),
        "total": str(quantize_money(total)),
        "items": items,
        "cantidad_items": len(items),
    }

    if not parsed["numero_factura"] or not parsed["cliente"] or not parsed["fecha"]:
        raise ValueError("No se pudieron detectar correctamente los datos principales de la factura.")

    return parsed


def crear_factura_y_viajes_desde_importacion(data, crear_viajes=True):
    numero_factura = (data.get("numero_factura") or "").strip()
    if Factura.query.filter_by(numero_factura=numero_factura).first():
        raise ValueError(f"La factura {numero_factura} ya existe.")

    fecha_factura = parse_date_safe(data.get("fecha")) or date.today()
    fecha_vencimiento = parse_date_safe(data.get("fecha_vencimiento")) or (fecha_factura + timedelta(days=20))
    cliente = (data.get("cliente") or "").strip()

    upsert_maestro(Productor, cliente)

    factura = Factura(
        numero_factura=numero_factura,
        fecha=fecha_factura,
        fecha_vencimiento=fecha_vencimiento,
        cliente=cliente,
        importe_neto=quantize_money(to_decimal(data.get("subtotal", "0"))),
        iva=quantize_money(to_decimal(data.get("iva", "0"))),
        percepciones=quantize_money(to_decimal(data.get("percepciones", "0"))),
        importe_total=quantize_money(to_decimal(data.get("total", "0"))),
        estado_pago="pendiente",
        observaciones=f"Importada desde PDF. Cliente N° {data.get('cliente_numero') or '-'}",
    )
    db.session.add(factura)

    if crear_viajes:
        iva_rate = get_config_decimal("iva_rate", "0.21")
        socio_rate = get_config_decimal("socio_commission_rate", "0.06")
        no_socio_rate = get_config_decimal("no_socio_commission_rate", "0.10")
        lucas_rate = get_config_decimal("lucas_commission_rate", "0.015")

        for item in data.get("items", []):
            fletero = (item.get("fletero") or "").strip()
            if fletero:
                upsert_maestro(FleteroMaster, fletero)

            viaje = Viaje(
                fecha=fecha_factura,
                cliente=cliente,
                factura=numero_factura,
                fletero=fletero,
                socio=bool(item.get("socio", True)),
                ctg=(item.get("ctg") or "").strip() or None,
                origen=(item.get("origen") or "").strip() or None,
                destino=(item.get("destino") or "").strip() or None,
                kilometros=to_decimal(item.get("kilometros", "0")),
                tarifa=to_decimal(item.get("tarifa", "0")),
                descuento=Decimal("0"),
                kg=to_decimal(item.get("kg", "0")),
                liquidado=False,
                observaciones=f"Producto: {item.get('producto') or '-'} | Kg factura: {item.get('kg_bruto') or '-'}",
            )
            viaje.recalcular(
                iva=iva_rate,
                socio_rate=socio_rate,
                no_socio_rate=no_socio_rate,
                lucas_rate=lucas_rate,
            )
            db.session.add(viaje)

    db.session.commit()
    sincronizar_factura_por_numero(numero_factura)
    db.session.commit()

    return factura


def ensure_schema():
    engine = db.engine

    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE
            """))

            conn.execute(text("""
                ALTER TABLE facturas
                ADD COLUMN IF NOT EXISTS percepciones NUMERIC(14,2) NOT NULL DEFAULT 0
            """))
            conn.execute(text("""
                ALTER TABLE facturas
                ADD COLUMN IF NOT EXISTS fecha_vencimiento DATE
            """))
            conn.execute(text("""
                UPDATE facturas
                SET percepciones = 0
                WHERE percepciones IS NULL
            """))
            conn.execute(text("""
                UPDATE facturas
                SET fecha_vencimiento = fecha + INTERVAL '20 day'
                WHERE fecha_vencimiento IS NULL
            """))

            conn.execute(text("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS medio_pago VARCHAR(50)"))
            conn.execute(text("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS numero_referencia VARCHAR(100)"))
            conn.execute(text("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS fecha_cobro_real DATE"))
            conn.execute(text("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS importe NUMERIC(14,2) NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS retenciones NUMERIC(14,2) NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS total_aplicable NUMERIC(14,2) NOT NULL DEFAULT 0"))
            conn.execute(text("UPDATE pagos SET medio_pago = 'Transferencia' WHERE medio_pago IS NULL"))
            conn.execute(text("ALTER TABLE pagos ALTER COLUMN medio_pago SET DEFAULT 'Transferencia'"))
            conn.execute(text("ALTER TABLE pagos ALTER COLUMN medio_pago SET NOT NULL"))
            conn.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'pagos'
                          AND column_name = 'importe_total'
                    ) THEN
                        UPDATE pagos
                        SET importe_total = COALESCE(importe_total, total_aplicable, importe, 0)
                        WHERE importe_total IS NULL;
                        ALTER TABLE pagos ALTER COLUMN importe_total SET DEFAULT 0;
                        ALTER TABLE pagos ALTER COLUMN importe_total SET NOT NULL;
                    END IF;
                END $$;
            """))

            conn.execute(text("""
                ALTER TABLE pago_aplicaciones
                ADD COLUMN IF NOT EXISTS importe_pago NUMERIC(14,2)
            """))
            conn.execute(text("""
                ALTER TABLE pago_aplicaciones
                ADD COLUMN IF NOT EXISTS importe_retenciones NUMERIC(14,2)
            """))
            conn.execute(text("""
                ALTER TABLE pago_aplicaciones
                ADD COLUMN IF NOT EXISTS total_aplicado NUMERIC(14,2)
            """))

            conn.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'pago_aplicaciones'
                          AND column_name = 'importe_aplicado'
                    ) THEN
                        UPDATE pago_aplicaciones
                        SET importe_pago = COALESCE(importe_pago, importe_aplicado, 0)
                        WHERE importe_pago IS NULL;

                        UPDATE pago_aplicaciones
                        SET importe_retenciones = COALESCE(importe_retenciones, 0)
                        WHERE importe_retenciones IS NULL;

                        UPDATE pago_aplicaciones
                        SET total_aplicado = COALESCE(total_aplicado, importe_aplicado, 0)
                        WHERE total_aplicado IS NULL;
                    ELSE
                        UPDATE pago_aplicaciones
                        SET importe_pago = COALESCE(importe_pago, 0)
                        WHERE importe_pago IS NULL;

                        UPDATE pago_aplicaciones
                        SET importe_retenciones = COALESCE(importe_retenciones, 0)
                        WHERE importe_retenciones IS NULL;

                        UPDATE pago_aplicaciones
                        SET total_aplicado = COALESCE(total_aplicado, COALESCE(importe_pago, 0) + COALESCE(importe_retenciones, 0))
                        WHERE total_aplicado IS NULL;
                    END IF;
                END $$;
            """))
            conn.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'pago_aplicaciones'
                          AND column_name = 'importe_aplicado'
                    ) THEN
                        UPDATE pago_aplicaciones
                        SET importe_aplicado = COALESCE(importe_aplicado, total_aplicado, importe_pago, 0)
                        WHERE importe_aplicado IS NULL;

                        ALTER TABLE pago_aplicaciones
                        ALTER COLUMN importe_aplicado SET DEFAULT 0;
                    END IF;
                END $$;
            """))
            conn.execute(text("""
                ALTER TABLE pago_aplicaciones
                ALTER COLUMN importe_pago SET DEFAULT 0
            """))
            conn.execute(text("""
                ALTER TABLE pago_aplicaciones
                ALTER COLUMN importe_retenciones SET DEFAULT 0
            """))
            conn.execute(text("""
                ALTER TABLE pago_aplicaciones
                ALTER COLUMN total_aplicado SET DEFAULT 0
            """))
            conn.execute(text("""
                UPDATE pago_aplicaciones
                SET importe_pago = 0
                WHERE importe_pago IS NULL
            """))
            conn.execute(text("""
                UPDATE pago_aplicaciones
                SET importe_retenciones = 0
                WHERE importe_retenciones IS NULL
            """))
            conn.execute(text("""
                UPDATE pago_aplicaciones
                SET total_aplicado = COALESCE(importe_pago, 0) + COALESCE(importe_retenciones, 0)
                WHERE total_aplicado IS NULL OR total_aplicado = 0
            """))

            conn.execute(text("""
                ALTER TABLE liquidacion_pagos
                ADD COLUMN IF NOT EXISTS numero VARCHAR(100)
            """))
            conn.execute(text("""
                ALTER TABLE liquidacion_pagos
                ADD COLUMN IF NOT EXISTS observaciones TEXT
            """))
            conn.execute(text("""
                ALTER TABLE liquidacion_pagos
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            """))

            conn.execute(text("""
                ALTER TABLE ccc_periodos
                ADD COLUMN IF NOT EXISTS sector VARCHAR(50) NOT NULL DEFAULT 'clientes'
            """))
            conn.execute(text("""
                ALTER TABLE ccc_movimientos
                ADD COLUMN IF NOT EXISTS sector VARCHAR(50) NOT NULL DEFAULT 'clientes'
            """))
            conn.execute(text("ALTER TABLE caja_movimientos ADD COLUMN IF NOT EXISTS concepto VARCHAR(200)"))
            conn.execute(text("ALTER TABLE caja_movimientos ADD COLUMN IF NOT EXISTS medio VARCHAR(50)"))


def upsert_maestro(model, nombre):
    nombre = (nombre or "").strip()
    if not nombre:
        return
    existente = model.query.filter(func.lower(model.nombre) == nombre.lower()).first()
    if not existente:
        db.session.add(model(nombre=nombre))


def buscar_tarifa_por_km(km_value):
    km_decimal = to_decimal(km_value, "0")
    if km_decimal <= 0:
        return None

    km_int = int(km_decimal)

    exacta = Tarifario.query.filter(Tarifario.km == km_int).first()
    if exacta:
        return exacta

    cercana = (
        Tarifario.query
        .filter(Tarifario.km <= km_int)
        .order_by(Tarifario.km.desc())
        .first()
    )
    return cercana


def parse_tarifario_text(texto):
    registros = []
    errores = []

    lineas = texto.splitlines()
    for i, linea in enumerate(lineas, start=1):
        raw = linea.strip()
        if not raw:
            continue

        raw = raw.replace("\t", "=").replace(";", "=").replace(":", "=")
        if "=" not in raw:
            errores.append(f"Línea {i}: formato inválido. Usá km=tarifa")
            continue

        km_str, tarifa_str = raw.split("=", 1)
        km_str = km_str.strip()
        tarifa_str = tarifa_str.strip().replace(".", "").replace(",", ".")

        if not km_str.isdigit():
            errores.append(f"Línea {i}: km inválido")
            continue

        try:
            km = int(km_str)
            tarifa = Decimal(tarifa_str)
        except Exception:
            errores.append(f"Línea {i}: tarifa inválida")
            continue

        registros.append((km, tarifa))

    return registros, errores


def actualizar_estado_factura(factura):
    aplicado = factura.total_aplicado
    total = to_decimal(factura.importe_total)

    if aplicado <= 0:
        factura.estado_pago = "pendiente"
    elif aplicado < total:
        factura.estado_pago = "parcial"
    else:
        factura.estado_pago = "pagada"


def sincronizar_factura_por_numero(numero_factura):
    numero_factura = (numero_factura or "").strip()
    if not numero_factura:
        return

    viajes = (
        Viaje.query
        .filter(Viaje.factura == numero_factura)
        .order_by(Viaje.fecha.asc(), Viaje.id.asc())
        .all()
    )

    factura = Factura.query.filter_by(numero_factura=numero_factura).first()

    if not viajes:
        if factura and not factura.aplicaciones:
            db.session.delete(factura)
        elif factura:
            factura.importe_neto = Decimal("0")
            factura.iva = Decimal("0")
            factura.importe_total = quantize_money(to_decimal(factura.percepciones or 0))
            actualizar_estado_factura(factura)
        return

    neto = sum((to_decimal(v.total_importe) for v in viajes), Decimal("0"))
    total_sin_percepciones = sum((to_decimal(v.importe_con_iva) for v in viajes), Decimal("0"))
    iva = total_sin_percepciones - neto

    fecha_ref = viajes[-1].fecha
    cliente_ref = viajes[0].cliente

    if not factura:
        factura = Factura(
            numero_factura=numero_factura,
            fecha=fecha_ref,
            fecha_vencimiento=fecha_ref + timedelta(days=20),
            cliente=cliente_ref,
            percepciones=Decimal("0"),
        )
        db.session.add(factura)

    factura.fecha = fecha_ref
    factura.fecha_vencimiento = fecha_ref + timedelta(days=20)
    factura.cliente = cliente_ref
    factura.importe_neto = quantize_money(neto)
    factura.iva = quantize_money(iva)

    percepciones = to_decimal(factura.percepciones or 0)
    factura.importe_total = quantize_money(total_sin_percepciones + percepciones)

    actualizar_estado_factura(factura)


def recalcular_liquidacion(liquidacion):
    bruto = sum((to_decimal(x.importe) for x in liquidacion.items), Decimal("0"))
    descuentos = sum((to_decimal(x.importe) for x in liquidacion.descuentos), Decimal("0"))
    neto = bruto - descuentos

    if neto < 0:
        neto = Decimal("0")

    liquidacion.total_bruto = quantize_money(bruto)
    liquidacion.total_descuentos = quantize_money(descuentos)
    liquidacion.neto_pagar = quantize_money(neto)

    total_pagado = sum((to_decimal(p.importe) for p in liquidacion.pagos), Decimal("0"))

    if total_pagado <= 0:
        liquidacion.estado = "pendiente"
    elif total_pagado < neto:
        liquidacion.estado = "parcial"
    else:
        liquidacion.estado = "pagada"


def get_monthly_stats(year: int, month: int):
    viajes_mes = Viaje.query.filter(
        func.extract("year", Viaje.fecha) == year,
        func.extract("month", Viaje.fecha) == month,
    )

    cantidad_viajes = viajes_mes.count()
    cantidad_liquidados = viajes_mes.filter(Viaje.liquidado.is_(True)).count()

    total_facturado = viajes_mes.with_entities(
        func.coalesce(func.sum(Viaje.total_importe), 0)
    ).scalar() or 0

    total_liquidado = viajes_mes.filter(Viaje.liquidado.is_(True)).with_entities(
        func.coalesce(func.sum(Viaje.total_importe), 0)
    ).scalar() or 0

    total_comisiones = viajes_mes.with_entities(
        func.coalesce(func.sum(Viaje.comision), 0)
    ).scalar() or 0

    total_comision_lucas = viajes_mes.with_entities(
        func.coalesce(func.sum(Viaje.comision_lucas), 0)
    ).scalar() or 0

    return {
        "cantidad_viajes": cantidad_viajes,
        "cantidad_liquidados": cantidad_liquidados,
        "total_facturado": total_facturado,
        "total_liquidado": total_liquidado,
        "total_comisiones": total_comisiones,
        "total_comision_lucas": total_comision_lucas,
    }


def ensure_default_users():
    defaults = [
        ("fabio", "Fabio", "Fabio1234"),
        ("matias", "Matías", "Matias1234"),
    ]
    changed = False

    for username, nombre, password in defaults:
        user = User.query.filter(func.lower(User.username) == username.lower()).first()
        if not user:
            user = User(username=username, nombre=nombre, activo=True)
            user.set_password(password)
            db.session.add(user)
            changed = True

    if changed:
        db.session.commit()



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

def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)
    return wrapper


# =========================
# FILTROS
# =========================

@app.template_filter("ars")
def ars(value):
    if value is None:
        return ""
    try:
        val = float(value)
        text = f"{val:,.2f}"
        return "$ " + text.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return value


@app.template_filter("si_no")
def si_no(value):
    return "Sí" if value else "No"


# =========================
# LOGIN / LOGOUT
# =========================

@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "").strip()

        user = User.query.filter(func.lower(User.username) == username).first()

        if not user or not user.activo or not user.check_password(password):
            flash("Usuario o contraseña incorrectos.", "warning")
            return render_template("login.html")

        session.clear()
        session.permanent = False
        session["user_id"] = user.id
        session["username"] = user.username
        session["nombre"] = user.nombre

        return redirect(url_for("index"))

    return render_template("login.html")


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    session.clear()
    flash("Sesión cerrada correctamente.", "success")
    return redirect(url_for("login"))


@app.route("/mi-cuenta/contrasena", methods=["GET", "POST"])
@login_required
def cambiar_contrasena():
    if request.method == "POST":
        current_password = request.form.get("current_password", "").strip()
        new_password = request.form.get("new_password", "").strip()
        confirm_password = request.form.get("confirm_password", "").strip()

        user = g.user
        if not user:
            flash("Sesión inválida. Volvé a iniciar sesión.", "warning")
            return redirect(url_for("login"))

        if not user.check_password(current_password):
            flash("La contraseña actual no es correcta.", "warning")
            return render_template("change_password.html")

        if len(new_password) < 8:
            flash("La nueva contraseña debe tener al menos 8 caracteres.", "warning")
            return render_template("change_password.html")

        if new_password != confirm_password:
            flash("La confirmación no coincide con la nueva contraseña.", "warning")
            return render_template("change_password.html")

        if new_password == current_password:
            flash("La nueva contraseña debe ser distinta a la actual.", "warning")
            return render_template("change_password.html")

        user.set_password(new_password)
        db.session.commit()
        flash("Contraseña actualizada correctamente.", "success")
        return redirect(url_for("index"))

    return render_template("change_password.html")


# =========================
# RUTAS
# =========================

@app.route("/")
@login_required
def index():
    q = request.args.get("q", "").strip()
    cliente = request.args.get("cliente", "").strip()
    fletero = request.args.get("fletero", "").strip()
    estado = request.args.get("estado", "").strip()
    month = request.args.get("month", "").strip()

    query = Viaje.query

    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                Viaje.cliente.ilike(like),
                Viaje.fletero.ilike(like),
                Viaje.factura.ilike(like),
                Viaje.ctg.ilike(like),
                Viaje.origen.ilike(like),
                Viaje.destino.ilike(like),
            )
        )

    if cliente:
        query = query.filter(Viaje.cliente.ilike(f"%{cliente}%"))

    if fletero:
        query = query.filter(Viaje.fletero.ilike(f"%{fletero}%"))

    if estado == "liquidado":
        query = query.filter(Viaje.liquidado.is_(True))
    elif estado == "pendiente":
        query = query.filter(Viaje.liquidado.is_(False))

    if month:
        try:
            year_part, month_part = month.split("-")
            query = query.filter(
                func.extract("year", Viaje.fecha) == int(year_part),
                func.extract("month", Viaje.fecha) == int(month_part),
            )
        except ValueError:
            pass

    viajes = query.order_by(Viaje.fecha.desc(), Viaje.id.desc()).all()

    stats_raw = query.with_entities(
        func.count(Viaje.id),
        func.coalesce(func.sum(Viaje.total_importe), 0),
        func.coalesce(func.sum(Viaje.importe_con_iva), 0),
        func.coalesce(func.sum(Viaje.comision), 0),
        func.coalesce(func.sum(Viaje.comision_lucas), 0),
    ).first()

    cantidad = stats_raw[0] or 0
    total_importe = stats_raw[1] or 0
    total_con_iva = stats_raw[2] or 0
    total_comision = stats_raw[3] or 0
    total_comision_lucas = stats_raw[4] or 0

    pendientes = query.filter(Viaje.liquidado.is_(False)).count()
    liquidados = query.filter(Viaje.liquidado.is_(True)).count()

    return render_template(
        "index.html",
        viajes=viajes,
        q=q,
        cliente=cliente,
        fletero=fletero,
        estado=estado,
        month=month,
        stats={
            "cantidad": cantidad,
            "total_importe": total_importe,
            "importe_con_iva": total_con_iva,
            "total_comision": total_comision,
            "total_comision_lucas": total_comision_lucas,
            "pendientes": pendientes,
            "liquidados": liquidados,
        },
    )


@app.route("/reportes")
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


@app.route("/reportes/lucas/export")
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


@app.route("/api/tarifa")
@login_required
def api_tarifa():
    km = request.args.get("km", "").strip()

    if not km:
        return jsonify({"tarifa": None})

    match = buscar_tarifa_por_km(km)
    if not match:
        return jsonify({"tarifa": None})

    return jsonify({
        "tarifa": float(match.tarifa_tn),
        "km_encontrado": match.km,
    })


@app.route("/viajes/nuevo", methods=["GET", "POST"])
@login_required
def nuevo_viaje():
    if request.method == "POST":
        viaje = Viaje()
        hydrate_viaje(viaje, request.form)

        db.session.add(viaje)
        db.session.commit()

        if viaje.factura:
            sincronizar_factura_por_numero(viaje.factura)
            db.session.commit()

        flash("Viaje creado correctamente.", "success")
        return redirect(url_for("index"))

    productores = [p.nombre for p in Productor.query.order_by(Productor.nombre.asc()).all()]
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]
    return render_template("form.html", viaje=None, productores=productores, fleteros=fleteros)


@app.route("/viajes/<int:viaje_id>/editar", methods=["GET", "POST"])
@login_required
def editar_viaje(viaje_id):
    viaje = Viaje.query.get_or_404(viaje_id)

    if request.method == "POST":
        factura_anterior = (viaje.factura or "").strip()

        hydrate_viaje(viaje, request.form)
        db.session.commit()

        factura_nueva = (viaje.factura or "").strip()

        if factura_anterior:
            sincronizar_factura_por_numero(factura_anterior)
        if factura_nueva and factura_nueva != factura_anterior:
            sincronizar_factura_por_numero(factura_nueva)
        elif factura_nueva:
            sincronizar_factura_por_numero(factura_nueva)

        db.session.commit()

        flash("Viaje actualizado correctamente.", "success")
        return redirect(url_for("index"))

    productores = [p.nombre for p in Productor.query.order_by(Productor.nombre.asc()).all()]
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]
    return render_template("form.html", viaje=viaje, productores=productores, fleteros=fleteros)


@app.route("/viajes/<int:viaje_id>/eliminar", methods=["POST"])
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
    return redirect(url_for("index"))


@app.route("/viajes/<int:viaje_id>/toggle-liquidado", methods=["POST"])
@login_required
def toggle_liquidado(viaje_id):
    viaje = Viaje.query.get_or_404(viaje_id)
    viaje.liquidado = not viaje.liquidado
    db.session.commit()

    estado = "liquidado" if viaje.liquidado else "pendiente"
    flash(f"Viaje marcado como {estado}.", "success")
    return redirect(url_for("index"))


@app.route("/configuracion", methods=["GET", "POST"])
@login_required
def configuracion():
    if request.method == "POST":
        for key in [
            "iva_rate",
            "socio_commission_rate",
            "no_socio_commission_rate",
            "lucas_commission_rate",
        ]:
            value = request.form.get(key, "").strip() or "0"
            item = db.session.get(AppConfig, key)
            if item:
                item.value = value
            else:
                db.session.add(AppConfig(key=key, value=value))
        db.session.commit()
        flash("Configuración guardada.", "success")
        return redirect(url_for("configuracion"))

    config = {
        "iva_rate": str(get_config_decimal("iva_rate", "0.21")),
        "socio_commission_rate": str(get_config_decimal("socio_commission_rate", "0.06")),
        "no_socio_commission_rate": str(get_config_decimal("no_socio_commission_rate", "0.10")),
        "lucas_commission_rate": str(get_config_decimal("lucas_commission_rate", "0.015")),
    }
    return render_template("configuracion.html", config=config)


@app.route("/configuracion/reset-datos", methods=["POST"])
@login_required
def resetear_datos_operativos():
    password = (request.form.get("password_reset") or "").strip()
    if password != "BORRAR2026":
        flash("Contraseña incorrecta para borrar datos.", "warning")
        return redirect(url_for("configuracion"))

    try:
        with db.session.begin():
            db.session.execute(text("TRUNCATE TABLE pago_aplicaciones, saldos_favor, liquidacion_items, liquidacion_descuentos, liquidacion_pagos, liquidaciones_fletero, facturas, pagos, viajes, caja_movimientos, cuotas_seguros RESTART IDENTITY CASCADE"))
        flash("Se borraron todos los datos operativos. El sistema quedó listo para arrancar de cero.", "success")
    except Exception as exc:
        db.session.rollback()
        flash(f"No se pudieron borrar los datos: {exc}", "warning")

    return redirect(url_for("configuracion"))


@app.route("/recalcular", methods=["POST"])
@login_required
def recalcular_todo():
    iva = get_config_decimal("iva_rate", "0.21")
    socio_rate = get_config_decimal("socio_commission_rate", "0.06")
    no_socio_rate = get_config_decimal("no_socio_commission_rate", "0.10")
    lucas_rate = get_config_decimal("lucas_commission_rate", "0.015")

    viajes = Viaje.query.all()
    facturas_afectadas = set()

    for viaje in viajes:
        viaje.recalcular(
            iva=iva,
            socio_rate=socio_rate,
            no_socio_rate=no_socio_rate,
            lucas_rate=lucas_rate,
        )
        if viaje.factura:
            facturas_afectadas.add(viaje.factura.strip())

    db.session.commit()

    for numero in facturas_afectadas:
        sincronizar_factura_por_numero(numero)

    db.session.commit()

    liquidaciones_existentes = LiquidacionFletero.query.all()
    for liq in liquidaciones_existentes:
        recalcular_liquidacion(liq)

    db.session.commit()

    flash("Se recalcularon todos los viajes y liquidaciones con la configuración actual.", "success")
    return redirect(url_for("index"))


@app.route("/tarifario", methods=["GET", "POST"])
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
                return redirect(url_for("tarifario"))

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
            return redirect(url_for("tarifario"))

        elif accion == "vaciar":
            Tarifario.query.delete()
            db.session.commit()
            flash("Se eliminó todo el tarifario.", "success")
            return redirect(url_for("tarifario"))

    items = Tarifario.query.order_by(Tarifario.km.asc()).limit(1000).all()
    total_items = Tarifario.query.count()

    return render_template("tarifario.html", items=items, total_items=total_items)


@app.route("/facturas")
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


@app.route("/facturas/importar-pdf", methods=["GET", "POST"])
@login_required
def importar_factura_pdf():
    preview = session.get("factura_pdf_preview")

    if request.method == "POST":
        archivo = request.files.get("archivo_pdf")
        if not archivo or not archivo.filename:
            flash("Seleccioná un PDF de factura.", "warning")
            return redirect(url_for("importar_factura_pdf"))

        if not archivo.filename.lower().endswith(".pdf"):
            flash("El archivo debe ser PDF.", "warning")
            return redirect(url_for("importar_factura_pdf"))

        try:
            parsed = parse_factura_pdf(archivo)
            session["factura_pdf_preview"] = parsed
            flash("Factura leída correctamente. Revisá la vista previa antes de importar.", "success")
        except Exception as exc:
            session.pop("factura_pdf_preview", None)
            flash(f"No se pudo leer la factura: {exc}", "warning")

        return redirect(url_for("importar_factura_pdf"))

    return render_template("factura_importar_pdf.html", preview=preview)


@app.route("/facturas/importar-pdf/confirmar", methods=["POST"])
@login_required
def confirmar_importacion_factura_pdf():
    preview = session.get("factura_pdf_preview")
    if not preview:
        flash("No hay ninguna factura pendiente de importar.", "warning")
        return redirect(url_for("importar_factura_pdf"))

    accion = request.form.get("accion", "factura_y_viajes")
    crear_viajes = accion == "factura_y_viajes"

    try:
        factura = crear_factura_y_viajes_desde_importacion(preview, crear_viajes=crear_viajes)
        session.pop("factura_pdf_preview", None)
        flash("Factura importada correctamente.", "success")
        return redirect(url_for("detalle_factura", factura_id=factura.id))
    except Exception as exc:
        flash(f"No se pudo importar la factura: {exc}", "warning")
        return redirect(url_for("importar_factura_pdf"))


@app.route("/facturas/importar-pdf/cancelar", methods=["POST"])
@login_required
def cancelar_importacion_factura_pdf():
    session.pop("factura_pdf_preview", None)
    flash("Vista previa descartada.", "success")
    return redirect(url_for("importar_factura_pdf"))
@app.route("/cobranzas")
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

@app.route("/facturas/<int:factura_id>")
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


@app.route("/facturas/<int:factura_id>/eliminar", methods=["POST"])
@login_required
def eliminar_factura(factura_id):
    factura = Factura.query.get_or_404(factura_id)
    if factura.aplicaciones:
        flash("No se puede eliminar una factura con pagos aplicados.", "warning")
        return redirect(url_for("detalle_factura", factura_id=factura_id))
    db.session.delete(factura)
    db.session.commit()
    flash("Factura eliminada.", "success")
    return redirect(url_for("facturas"))


@app.route("/facturas/<int:factura_id>/editar-percepciones", methods=["POST"])
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


@app.route("/pagos")
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

@app.route("/caja", methods=["GET", "POST"])
@login_required
def caja():
    if request.method == "POST":
        fecha = request.form.get("fecha", "").strip()
        tipo = request.form.get("tipo", "").strip().lower()
        importe = to_decimal(request.form.get("importe", "0"))
        observaciones = request.form.get("observaciones", "").strip() or None

        if tipo not in {"ingreso", "egreso"}:
            flash("Tipo de movimiento inválido.", "warning")
            return redirect(url_for("caja"))

        if importe <= 0:
            flash("El importe debe ser mayor a 0.", "warning")
            return redirect(url_for("caja"))

        try:
            fecha_obj = datetime.strptime(fecha, "%Y-%m-%d").date()
        except ValueError:
            flash("Fecha inválida.", "warning")
            return redirect(url_for("caja"))

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
        return redirect(url_for("caja"))

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


@app.route("/pagos/nuevo", methods=["GET", "POST"])
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
            return redirect(url_for("nuevo_pago"))

        upsert_maestro(Productor, productor)

        total_aplicable = quantize_money(importe + retenciones)

        factura_ids = request.form.getlist("factura_ids")
        factura_ids = [int(x) for x in factura_ids if str(x).strip()]

        if not factura_ids:
            flash("Tenés que seleccionar al menos una factura.", "warning")
            return redirect(url_for("nuevo_pago"))

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
        return redirect(url_for("pagos"))

    return render_template(
        "pago_form.html",
        facturas_pendientes=facturas_pendientes,
        productores=productores,
        pago=None,
        modo_edicion=False
    )

@app.route("/pagos/<int:pago_id>/editar", methods=["GET", "POST"])
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
        return redirect(url_for("pagos"))

    return render_template(
        "pago_form.html",
        pago=pago,
        productores=productores,
        modo_edicion=True
    )

@app.route("/cuotas-seguros", methods=["GET", "POST"])
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
            return redirect(url_for("cuotas_seguros"))
        if not fletero:
            flash("Tenés que indicar el fletero.", "warning")
            return redirect(url_for("cuotas_seguros"))

        try:
            year, month = periodo_raw.split("-")
            periodo = date(int(year), int(month), 1)
        except (TypeError, ValueError):
            flash("Período inválido.", "warning")
            return redirect(url_for("cuotas_seguros"))

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
        return redirect(url_for("cuotas_seguros"))

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
@app.route("/cuotas-seguros/<int:item_id>/editar", methods=["GET", "POST"])
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
        return redirect(url_for("cuotas_seguros"))

    return render_template("cuota_seguro_form.html", item=item, fleteros=fleteros)
@app.route("/cuotas-seguros/<int:item_id>/agregar-liquidacion", methods=["POST"])
@login_required
def agregar_cuota_seguro_a_liquidacion(item_id):
    item = CuotaSeguro.query.get_or_404(item_id)
    liquidacion_id_raw = request.form.get("liquidacion_id", "").strip()

    if not liquidacion_id_raw:
        flash("Seleccioná una liquidación.", "warning")
        return redirect(url_for("cuotas_seguros"))

    try:
        liquidacion_id = int(liquidacion_id_raw)
    except ValueError:
        flash("Liquidación inválida.", "warning")
        return redirect(url_for("cuotas_seguros"))

    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)

    if (liquidacion.fletero or "").strip().lower() != (item.fletero or "").strip().lower():
        flash("La liquidación seleccionada no corresponde al mismo fletero.", "warning")
        return redirect(url_for("cuotas_seguros"))

    importe = quantize_money(item.total)
    if importe <= 0:
        flash("El registro no tiene importe para aplicar.", "warning")
        return redirect(url_for("cuotas_seguros"))

    concepto = f"Cuotas/Seguros {item.periodo.strftime('%m/%Y')}"
    existente = LiquidacionDescuento.query.filter_by(
        liquidacion_id=liquidacion.id,
        concepto=concepto,
        importe=importe,
    ).first()
    if existente:
        flash("Este concepto ya fue agregado a esa liquidación.", "warning")
        return redirect(url_for("cuotas_seguros"))

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

@app.route("/liquidaciones")
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


@app.route("/liquidaciones/buscar-pagos")
@login_required
def buscar_pagos_fleteros():
    q = request.args.get("q", "").strip()
    medio = request.args.get("medio", "").strip()
    fecha_desde_raw = request.args.get("fecha_desde", "").strip()
    fecha_hasta_raw = request.args.get("fecha_hasta", "").strip()

    query = LiquidacionPago.query.join(LiquidacionFletero)

    fecha_desde = None
    fecha_hasta = None

    if fecha_desde_raw:
        try:
            fecha_desde = datetime.strptime(fecha_desde_raw, "%Y-%m-%d").date()
            query = query.filter(LiquidacionPago.fecha >= fecha_desde)
        except ValueError:
            flash("Fecha desde inválida.", "warning")

    if fecha_hasta_raw:
        try:
            fecha_hasta = datetime.strptime(fecha_hasta_raw, "%Y-%m-%d").date()
            query = query.filter(LiquidacionPago.fecha <= fecha_hasta)
        except ValueError:
            flash("Fecha hasta inválida.", "warning")

    if medio:
        query = query.filter(LiquidacionPago.medio == medio)

    if q:
        like = f"%{q}%"
        importe_buscado = None

        try:
            importe_buscado = quantize_money(to_decimal(q))
        except Exception:
            importe_buscado = None

        filtros = [
            LiquidacionPago.numero.ilike(like),
            LiquidacionFletero.fletero.ilike(like),
            LiquidacionFletero.factura_fletero.ilike(like),
            LiquidacionPago.observaciones.ilike(like),
        ]

        if importe_buscado is not None:
            filtros.append(LiquidacionPago.importe == importe_buscado)

        query = query.filter(or_(*filtros))

    items = query.order_by(LiquidacionPago.fecha.desc(), LiquidacionPago.id.desc()).all()

    total = quantize_money(sum((to_decimal(x.importe) for x in items), Decimal("0")))

    stats = {
        "cantidad": len(items),
        "total": total,
    }

    return render_template(
        "buscar_pagos_fleteros.html",
        items=items,
        q=q,
        medio=medio,
        fecha_desde=fecha_desde_raw,
        fecha_hasta=fecha_hasta_raw,
        stats=stats,
    )
@app.route("/liquidaciones/nueva", methods=["GET", "POST"])
@login_required
def nueva_liquidacion():
    fleteros = [f.nombre for f in FleteroMaster.query.order_by(FleteroMaster.nombre.asc()).all()]

    if request.method == "POST":
        fecha_raw = request.form.get("fecha", "")
        fecha_liq = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()
        fletero = request.form.get("fletero", "").strip()
        factura_fletero = request.form.get("factura_fletero", "").strip() or None
        observaciones = request.form.get("observaciones", "").strip() or None
        estado = request.form.get("estado", "pendiente").strip()

        if not fletero:
            flash("Tenés que indicar el fletero.", "warning")
            return redirect(url_for("nueva_liquidacion"))

        upsert_maestro(FleteroMaster, fletero)

        liquidacion = LiquidacionFletero(
            fecha=fecha_liq,
            fletero=fletero,
            factura_fletero=factura_fletero,
            observaciones=observaciones,
            estado=estado,
        )
        db.session.add(liquidacion)
        db.session.flush()

        viaje_ids = request.form.getlist("viaje_ids")
        viaje_ids = [int(x) for x in viaje_ids if str(x).strip()]

        for viaje_id in viaje_ids:
            viaje = Viaje.query.get(viaje_id)
            if not viaje:
                continue
            db.session.add(LiquidacionItem(
                liquidacion_id=liquidacion.id,
                viaje_id=viaje.id,
                importe=quantize_money(to_decimal(viaje.total_importe)),
            ))

        conceptos = request.form.getlist("descuento_concepto[]")
        importes = request.form.getlist("descuento_importe[]")

        for concepto, importe_desc in zip(conceptos, importes):
            concepto = (concepto or "").strip()
            importe_dec = to_decimal(importe_desc, "0")
            if not concepto or importe_dec <= 0:
                continue
            db.session.add(LiquidacionDescuento(
                liquidacion_id=liquidacion.id,
                concepto=concepto,
                importe=quantize_money(importe_dec),
            ))

        db.session.flush()
        recalcular_liquidacion(liquidacion)
        db.session.commit()

        flash("Liquidación creada correctamente.", "success")
        return redirect(url_for("liquidaciones"))

    viajes = Viaje.query.order_by(Viaje.fecha.desc(), Viaje.id.desc()).all()
    return render_template("liquidacion_form.html", fleteros=fleteros, viajes=viajes, liquidacion=None)


@app.route("/liquidaciones/<int:liquidacion_id>/editar", methods=["GET", "POST"])
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
            viaje = Viaje.query.get(viaje_id)
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
        return redirect(url_for("detalle_liquidacion", liquidacion_id=liquidacion.id))

    viajes = Viaje.query.order_by(Viaje.fecha.desc(), Viaje.id.desc()).all()
    return render_template("liquidacion_form.html", fleteros=fleteros, viajes=viajes, liquidacion=liquidacion)


@app.route("/liquidaciones/<int:liquidacion_id>/eliminar", methods=["POST"])
@login_required
def eliminar_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    db.session.delete(liquidacion)
    db.session.commit()
    flash("Liquidación eliminada.", "success")
    return redirect(url_for("liquidaciones"))


@app.route("/liquidaciones/<int:liquidacion_id>")
@login_required
def detalle_liquidacion(liquidacion_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    recalcular_liquidacion(liquidacion)
    db.session.commit()
    return render_template("liquidacion_detalle.html", liquidacion=liquidacion)


@app.route("/liquidaciones/<int:liquidacion_id>/pago", methods=["GET", "POST"])
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
            return redirect(url_for("pagar_liquidacion", liquidacion_id=liquidacion.id))

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
        return redirect(url_for("detalle_liquidacion", liquidacion_id=liquidacion.id))

    return render_template(
        "liquidacion_pago_form.html",
        liquidacion=liquidacion,
        fecha_hoy=date.today().strftime("%Y-%m-%d"),
        pago=None,
        accion_url=url_for("pagar_liquidacion", liquidacion_id=liquidacion.id),
        titulo="Registrar pago de liquidación",
        boton="Guardar pago"
    )


@app.route("/liquidaciones/<int:liquidacion_id>/pago/<int:pago_id>/editar", methods=["GET", "POST"])
@login_required
def editar_pago_liquidacion(liquidacion_id, pago_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    pago = LiquidacionPago.query.get_or_404(pago_id)

    if pago.liquidacion_id != liquidacion.id:
        flash("El pago no corresponde a esta liquidación.", "warning")
        return redirect(url_for("detalle_liquidacion", liquidacion_id=liquidacion.id))

    if request.method == "POST":
        fecha_raw = request.form.get("fecha", "")
        pago.fecha = datetime.strptime(fecha_raw, "%Y-%m-%d").date() if fecha_raw else date.today()
        pago.medio = request.form.get("medio", "").strip()
        pago.numero = request.form.get("numero", "").strip() or None
        pago.importe = quantize_money(to_decimal(request.form.get("importe", "0")))
        pago.observaciones = request.form.get("observaciones", "").strip() or None

        if not pago.medio or to_decimal(pago.importe) <= 0:
            flash("Completá medio e importe del pago.", "warning")
            return redirect(url_for("editar_pago_liquidacion", liquidacion_id=liquidacion.id, pago_id=pago.id))

        recalcular_liquidacion(liquidacion)
        db.session.commit()

        flash("Pago actualizado.", "success")
        return redirect(url_for("detalle_liquidacion", liquidacion_id=liquidacion.id))

    return render_template(
        "liquidacion_pago_form.html",
        liquidacion=liquidacion,
        fecha_hoy=pago.fecha.strftime("%Y-%m-%d"),
        pago=pago,
        accion_url=url_for("editar_pago_liquidacion", liquidacion_id=liquidacion.id, pago_id=pago.id),
        titulo="Editar pago de liquidación",
        boton="Guardar cambios"
    )


@app.route("/liquidaciones/<int:liquidacion_id>/pago/<int:pago_id>/eliminar", methods=["POST"])
@login_required
def eliminar_pago_liquidacion(liquidacion_id, pago_id):
    liquidacion = LiquidacionFletero.query.get_or_404(liquidacion_id)
    pago = LiquidacionPago.query.get_or_404(pago_id)

    if pago.liquidacion_id != liquidacion.id:
        flash("El pago no corresponde a esta liquidación.", "warning")
        return redirect(url_for("detalle_liquidacion", liquidacion_id=liquidacion.id))

    db.session.delete(pago)
    db.session.flush()

    recalcular_liquidacion(liquidacion)
    db.session.commit()

    flash("Pago eliminado correctamente.", "success")
    return redirect(url_for("detalle_liquidacion", liquidacion_id=liquidacion.id))


@app.route("/liquidaciones/<int:liquidacion_id>/recibo")
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
    viaje.kilometros = to_decimal(form.get("kilometros", "0"))

    tarifa_manual = form.get("tarifa", "").strip()
    usar_tarifario = form.get("usar_tarifario") == "si"

    if usar_tarifario and viaje.kilometros and to_decimal(viaje.kilometros) > 0:
        match = buscar_tarifa_por_km(viaje.kilometros)
        if match:
            viaje.tarifa = to_decimal(match.tarifa_tn)
        else:
            viaje.tarifa = to_decimal(tarifa_manual, "0")
    else:
        viaje.tarifa = to_decimal(tarifa_manual, "0")

    viaje.descuento = to_decimal(form.get("descuento", "0"))
    viaje.kg = to_decimal(form.get("kg", "0"))
    viaje.liquidado = form.get("liquidado") == "si"
    viaje.observaciones = form.get("observaciones", "").strip() or None

    upsert_maestro(Productor, viaje.cliente)
    upsert_maestro(FleteroMaster, viaje.fletero)

    viaje.recalcular(
        iva=get_config_decimal("iva_rate", "0.21"),
        socio_rate=get_config_decimal("socio_commission_rate", "0.06"),
        no_socio_rate=get_config_decimal("no_socio_commission_rate", "0.10"),
        lucas_rate=get_config_decimal("lucas_commission_rate", "0.015"),
    )



# =========================
# CUENTAS CORRIENTES
# =========================

@app.route("/ccc")
@login_required
def ccc_index():
    return render_template("ccc.html", usuario=session.get("nombre") or session.get("username") or "")


@app.route("/api/ccc/upload", methods=["POST"])
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



@app.route("/api/ccc/sector/<sector>", methods=["DELETE"])
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


@app.route("/api/ccc/todo", methods=["DELETE"])
@login_required
def ccc_delete_all():
    CCCMovimiento.query.delete(synchronize_session=False)
    CCCPeriodo.query.delete(synchronize_session=False)
    CCCAccion.query.delete(synchronize_session=False)
    CCCCuenta.query.delete(synchronize_session=False)  # Eliminar cuentas completamente
    db.session.commit()
    return jsonify({"ok": True})


@app.route("/api/ccc/cuentas")
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


@app.route("/api/ccc/cuentas/<codigo>", methods=["GET", "PUT"])
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


@app.route("/api/ccc/accion", methods=["POST"])
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


@app.route("/api/ccc/accion/<int:accion_id>", methods=["DELETE"])
@login_required
def ccc_eliminar_accion(accion_id):
    accion = CCCAccion.query.get_or_404(accion_id)
    db.session.delete(accion)
    db.session.commit()
    return jsonify({"ok": True})




@app.route("/api/ccc/resumen-mensual")
@login_required
def ccc_resumen_mensual():
    fecha_ref = (request.args.get("fecha") or date.today().isoformat()).strip()
    fecha = ccc_parse_date(fecha_ref) or date.today()
    return jsonify(ccc_month_summary(fecha.year, fecha.month))

@app.route("/api/ccc/stats")
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
    
@app.route("/api/ccc/mensaje", methods=["GET"])
@login_required
def ccc_get_message():
    vencimiento = (request.args.get("vencimiento") or "").strip()

    return jsonify({
        "template": ccc_get_message_template(),
        "preview": ccc_format_message(vencimiento),
    })


@app.route("/api/ccc/mensaje", methods=["POST"])
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
# INIT
# =========================

with app.app_context():
    db.create_all()
    ensure_schema()
    set_default_config()
    ensure_default_users()

    liquidaciones_existentes = LiquidacionFletero.query.all()
    for liq in liquidaciones_existentes:
        recalcular_liquidacion(liq)

    db.session.commit()


if __name__ == "__main__":
    app.run(debug=True)
