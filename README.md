# Control de viajes web

Sistema web simple para reemplazar el Excel de control de viajes.

## Qué hace esta versión
- Alta, edición y eliminación de viajes
- Filtros por mes, cliente, fletero y estado
- Cálculo automático de:
  - Total importe
  - Importe + IVA
  - Comisión
  - Comisión Lucas
- Configuración de porcentajes sin tocar código
- Base de datos PostgreSQL en Render
- Importación inicial desde tu Excel

## Fórmulas que replica
- **Total importe** = `(tarifa - (tarifa * descuento)) * kg`
- **Importe + IVA** = `total_importe * (1 + IVA)`
- **Comisión** = `importe_con_iva * porcentaje`
- **Comisión Lucas** = `total_importe * 0.015`

## Ojo con esto
En tu Excel se ve que:
- antes había hojas sin columna **LIQUIDADO**
- en hojas viejas la comisión de socio aparece al 5%
- después aparece al 6%

Por eso en esta app los porcentajes son **configurables**. No quedan clavados en una fórmula escondida.

## Cómo probarlo local
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt
python app.py
```

## Cómo importar tu Excel
1. Poné tu archivo en la raíz del proyecto con nombre:
   `Control de viajes.xlsx`
2. Ejecutá:
```bash
python import_from_excel.py
```

## Despliegue en Render
### Opción manual
1. Subí este proyecto a GitHub.
2. En Render creá:
   - un **PostgreSQL**
   - un **Web Service**
3. En el Web Service:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn app:app`
4. Agregá variables:
   - `SECRET_KEY`
   - `DATABASE_URL` (la del Postgres de Render)

### Opción con render.yaml
Podés crear el servicio desde el blueprint del repo y Render toma la config sola.

## Qué te conviene hacer después
- tabla de clientes
- tabla de fleteros
- exportar a Excel
- login
- historial de cambios
- alertas de duplicados
