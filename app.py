import os
import csv
import io
import re
from functools import wraps
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response, session, g
)
from sqlalchemy import or_, func, text
from werkzeug.security import generate_password_hash, check_password_hash

from extensions import db
from flask_wtf.csrf import CSRFProtect
from models import (
    User, AppConfig, Productor, FleteroMaster, Tarifario, Viaje,
    Factura, Pago, PagoAplicacion, SaldoFavor, CajaMovimiento,
    CuotaSeguro, LiquidacionFletero, LiquidacionItem,
    LiquidacionDescuento, LiquidacionPago,
    CCCPeriodo, CCCCuenta, CCCMovimiento, CCCAccion,
)
from utils import to_decimal, quantize_money

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
_secret_key = os.getenv("SECRET_KEY")
if not _secret_key:
    import warnings
    warnings.warn(
        "SECRET_KEY no está configurada. Usando clave de desarrollo — "
        "NO usar en producción.",
        stacklevel=2,
    )
    _secret_key = "dev-insecure-key-cambiar-en-produccion"

app.config["SECRET_KEY"] = _secret_key
app.config["SESSION_PERMANENT"] = False
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)

database_url = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'app.db')}")
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)
csrf = CSRFProtect(app)

# =========================
# BLUEPRINTS
# =========================

from routes.auth_routes import auth_bp
from routes.main_routes import main_bp
from routes.viajes_routes import viajes_bp
from routes.facturas_routes import facturas_bp
from routes.pagos_routes import pagos_bp
from routes.liquidaciones_routes import liquidaciones_bp
from routes.ccc_routes import ccc_bp

app.register_blueprint(auth_bp)
app.register_blueprint(main_bp)
app.register_blueprint(viajes_bp)
app.register_blueprint(facturas_bp)
app.register_blueprint(pagos_bp)
app.register_blueprint(liquidaciones_bp)
app.register_blueprint(ccc_bp)
csrf.exempt(ccc_bp)  # Las rutas /api/ccc/* reciben JSON, no formularios HTML

# =========================
# FILTROS TEMPLATE
# =========================

@app.template_filter("ars")
def ars(value):
    if value is None:
        return ""
    try:
        from decimal import Decimal
        v = Decimal(str(value))
        parts = f"{abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"-$ {parts}" if v < 0 else f"$ {parts}"
    except Exception:
        return str(value)


@app.template_filter("si_no")
def si_no(value):
    if value is None:
        return ""
    return "Sí" if value else "No"


# =========================
# BEFORE REQUEST
# =========================

@app.before_request
def load_user():
    g.user = None
    user_id = session.get("user_id")
    if user_id:
        g.user = db.session.get(User, user_id)


# =========================
# STARTUP HELPERS
# =========================

def ensure_schema():
    """
    Agrega columnas que pueden faltar en bases existentes.
    Compatible con PostgreSQL (producción) y SQLite (desarrollo local).
    Cada ALTER se ejecuta en su propio try/except para que un fallo
    no detenga el resto.
    """
    is_postgres = db.engine.dialect.name == "postgresql"

    columnas = [
        ("viajes",                  "producto",             "VARCHAR(120)"),
        ("viajes",                  "ctg",                  "VARCHAR(50)"),
        ("viajes",                  "origen",               "VARCHAR(120)"),
        ("viajes",                  "destino",              "VARCHAR(120)"),
        ("viajes",                  "kilometros",           "NUMERIC(12,2)"),
        ("viajes",                  "observaciones",        "TEXT"),
        ("viajes",                  "comision_lucas",       "NUMERIC(14,2) DEFAULT 0"),
        ("facturas",                "percepciones",         "NUMERIC(14,2) DEFAULT 0"),
        ("pagos",                   "retencion_iibb",       "NUMERIC(14,2) DEFAULT 0"),
        ("pagos",                   "retencion_iva",        "NUMERIC(14,2) DEFAULT 0"),
        ("pagos",                   "retencion_ganancias",  "NUMERIC(14,2) DEFAULT 0"),
        ("pago_aplicaciones",       "importe_retenciones",  "NUMERIC(14,2) DEFAULT 0"),
        ("liquidaciones_fleteros",  "subtotal",             "NUMERIC(14,2) DEFAULT 0"),
        ("liquidaciones_fleteros",  "total_descuentos",     "NUMERIC(14,2) DEFAULT 0"),
        ("liquidaciones_fleteros",  "total_pagado",         "NUMERIC(14,2) DEFAULT 0"),
        ("liquidaciones_fleteros",  "saldo",                "NUMERIC(14,2) DEFAULT 0"),
        ("ccc_movimientos",         "sector",               "VARCHAR(50) NOT NULL DEFAULT 'clientes'"),
        ("caja_movimientos",        "concepto",             "VARCHAR(200)"),
        ("caja_movimientos",        "medio",                "VARCHAR(50)"),
    ]

    with db.engine.connect() as conn:
        for tabla, columna, tipo in columnas:
            try:
                if is_postgres:
                    sql = f"ALTER TABLE {tabla} ADD COLUMN IF NOT EXISTS {columna} {tipo}"
                else:
                    # SQLite no soporta IF NOT EXISTS en ALTER TABLE
                    sql = f"ALTER TABLE {tabla} ADD COLUMN {columna} {tipo}"
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                # La columna ya existe u otro error no crítico — continuar
                conn.rollback()


def set_default_config():
    defaults = {
        "iva": "0.21",
        "comision_socio": "0.06",
        "comision_no_socio": "0.10",
        "comision_lucas": "0.015",
    }
    changed = False
    for key, value in defaults.items():
        if not db.session.get(AppConfig, key):
            db.session.add(AppConfig(key=key, value=value))
            changed = True
    if changed:
        db.session.commit()


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


# =========================
# INIT
# =========================

with app.app_context():
    db.create_all()
    ensure_schema()
    set_default_config()
    ensure_default_users()
    db.session.commit()


if __name__ == "__main__":
    app.run(debug=True)
