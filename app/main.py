from fastapi import FastAPI, HTTPException, Request
from app.models import RegistroEntrada
from app.database import (
    crear_tabla_registros,
    crear_tablas_resumen_tarjeta,
    insertar_registro,
    obtener_registros,
    insertar_resumen_tarjeta,
    existe_documento,
    get_sqlite_uuids,
    delete_expenses,
    insert_expenses,
    delete_incomes,
    insert_incomes
)
from app.googlesheet import(
    auth_in_gdrive,
    get_current_month_expenses,
    get_historic_expenses,
    get_current_month_income,
    get_historic_income
)
from datetime import datetime
import hashlib
import os
import json
import traceback
import time
from typing import Callable

# DEBUG remoto si está activo
if os.getenv("DEBUG_MODE") == "1":
    import debugpy
    debugpy.listen(("0.0.0.0", 5678))
    print("🛠 Esperando conexión de debugger...")

app = FastAPI()

# Crear las tablas al iniciar la app
crear_tabla_registros()
crear_tablas_resumen_tarjeta()

# Endpoint POST para /registro
@app.post("/registro")
def agregar_registro(registro: RegistroEntrada):
    exito = insertar_registro(
        uuid=registro.uuid,
        marca_temporal=registro.marca_temporal.isoformat(),
        descripcion=registro.descripcion,
        importe=registro.importe,
        tipo=registro.tipo
    )
    if not exito:
        raise HTTPException(status_code=409, detail="Registro duplicado (hash ya existe)")
    return {"status": "Registro agregado correctamente"}

# Endpoint GET para /registros/{anio}/{mes}
@app.get("/registros/{anio}/{mes}")
def listar_registros(anio: int, mes: int):
    registros = obtener_registros(anio, mes)
    return {"registros": registros}

# Nuevo endpoint POST para /loadCardResume
@app.post("/loadCardResume")
async def cargar_resumen_tarjeta(request: Request):
    card_type = request.headers.get('card_type')
    payload_bytes = await request.body()
    payload_dict = await request.json()

    # SHA del contenido bruto
    document_number = hashlib.sha256(payload_bytes).hexdigest()

    # Primer día del mes actual a las 00:00
    ahora = datetime.now()
    resume_date = datetime(ahora.year, ahora.month, 1, 0, 0, 0)

    if existe_documento(document_number):
        raise HTTPException(status_code=409, detail="Resumen ya existe")

    insertar_resumen_tarjeta(document_number, resume_date, payload_dict, card_type)

    return {"status": "Resumen de tarjeta cargado correctamente"}

@app.get("/getResumeExpenses/{anio}/{mes}")
@app.get("/getResumeExpenses/{anio}/{mes}/{card_type}")
@app.get("/getResumeExpenses/{anio}/{mes}/{card_type}/{holder}")
def get_resume_expenses(anio: int, mes: int, card_type: str = None, holder: str = None):
    from app.database import obtener_resumen
    resumen = obtener_resumen(anio, mes, card_type, holder)
    return resumen

@app.get("/getAvailableResumes/{anio}/{mes}")
def get_available_resumes(anio: int, mes: int):
    from app.database import obtener_tarjetas_disponibles

    return obtener_tarjetas_disponibles(anio, mes)

def sync_data(
    get_sheet_data_func: Callable,
    insert_func: Callable,
    delete_func: Callable,
    label: str
):
    start_time = time.time()
    print(f"\n🔄 Sincronizando: {label}...")

    try:
        client = auth_in_gdrive()
        print("✅ Autenticación en Google Drive completada")

        sheet = get_sheet_data_func(client)
        print(f"📄 Filas obtenidas desde Google Sheets: {len(sheet)}")

        sheet_uuids = set(row["UUID"] for row in sheet if row.get("UUID"))
        sqlite_uuids = get_sqlite_uuids()
        print(f"📂 UUIDs en hoja: {len(sheet_uuids)} | UUIDs en SQLite: {len(sqlite_uuids)}")

        uuids_to_insert = sheet_uuids - sqlite_uuids
        uuids_to_delete = sqlite_uuids - sheet_uuids

        rows_to_insert = [row for row in sheet if row.get("UUID") in uuids_to_insert]

        print(f"➕ Insertando {len(rows_to_insert)} nuevos registros")
        insert_func(rows_to_insert)

        print(f"🗑️ Eliminando {len(uuids_to_delete)} registros antiguos")
        delete_func(uuids_to_delete)

        duration = round(time.time() - start_time, 2)
        print(f"✅ Sincronización completada en {duration} segundos")

        return {
            "state": f"{label} updated",
            "added": len(uuids_to_insert),
            "deleted": len(uuids_to_delete),
            "duration_sec": duration
        }

    except Exception as e:
        print("❌ Error durante la sincronización:")
        traceback.print_exc()
        return {
            "state": f"Error syncing {label}",
            "error": str(e)
        }
    

@app.get("/syncHistoricExpenses")
def sync_historic_expenses():
    return sync_data(get_historic_expenses, insert_expenses, delete_expenses, "Historic expenses")

@app.get("/syncCurrentMonthExpenses")
def sync_current_month_expenses():
    return sync_data(get_current_month_expenses, insert_expenses, delete_expenses, "Monthly expenses")

@app.get("/syncHistoricIncome")
def sync_historic_income():
    return sync_data(get_historic_income, insert_incomes, delete_incomes, "Historic incomes")

@app.get("/syncCurrentMonthIncome")
def sync_current_month_income():
    return sync_data(get_current_month_income, insert_incomes, delete_incomes, "Monthly incomes")