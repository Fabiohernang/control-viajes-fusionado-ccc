from functools import wraps

from flask import render_template, request, redirect, url_for, flash, session, g
from sqlalchemy import func

from extensions import db
from models import User


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)
    return wrapper


def register_auth_hooks(app):
    @app.before_request
    def load_logged_user():
        g.user = None
        user_id = session.get("user_id")
        if user_id:
            g.user = db.session.get(User, user_id)


def register_auth_routes(app):
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
