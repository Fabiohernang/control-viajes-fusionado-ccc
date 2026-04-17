from datetime import datetime, date
from decimal import Decimal

from werkzeug.security import generate_password_hash, check_password_hash

from extensions import db
from utils import to_decimal, quantize_money


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
        facturas_cliente = Factura.query.filter_by(cliente=self.cliente).all()
        return quantize_money(sum((to_decimal(f.saldo_pendiente) for f in facturas_cliente), Decimal("0")))


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
