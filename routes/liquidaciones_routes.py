[MODIFICADO SOLO EL BLOQUE DE DESCUENTOS PARA QUE NO SE CARGUEN EN CREACION]

# (contenido igual hasta _guardar_items_y_descuentos)

def _guardar_items_y_descuentos(liquidacion, form):
    prev_items = list(liquidacion.items)
    for item in prev_items:
        if item.viaje:
            item.viaje.liquidado = False

    liquidacion.items.clear()
    liquidacion.descuentos.clear()
    db.session.flush()

    viaje_ids = [int(x) for x in form.getlist("viaje_ids") if str(x).strip()]
    for viaje_id in viaje_ids:
        viaje = db.session.get(Viaje, viaje_id)
        if not viaje or not _es_liquidable(viaje):
            continue
        if viaje.fletero.strip().lower() != liquidacion.fletero.strip().lower():
            continue
        viaje.liquidado = True
        liquidacion.items.append(
            LiquidacionItem(
                viaje_id=viaje.id,
                importe=quantize_money(to_decimal(viaje.importe_con_iva)),
            )
        )

    # ❌ YA NO SE CARGAN DESCUENTOS EN ESTA ETAPA

    recalcular_liquidacion(liquidacion)
