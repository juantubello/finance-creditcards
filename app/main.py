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
    eliminar_varios_por_uuid,
    insertar_varios_registros
)
from app.googlesheet import(
    auth_in_gdrive,
    get_current_month_expenses,
    get_historic_expenses
)
from datetime import datetime
import hashlib
import os
import json

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

@app.get("/syncCurrentMonthExpenses")
def sync_current_month_expenses():
    client = auth_in_gdrive()
    sheet = get_current_month_expenses(client)
    sheet_uuids = set(row["UUID"] for row in sheet if row["UUID"])

    sqlite_uuids = get_sqlite_uuids()

    uuids_to_insert = sheet_uuids - sqlite_uuids
    uuids_to_delete = sqlite_uuids - sheet_uuids

    # Filtrar solo los que hay que insertar
    filas_a_insertar = [row for row in sheet if row["UUID"] in uuids_to_insert]

    eliminar_varios_por_uuid(uuids_to_delete)
    insertar_varios_registros(filas_a_insertar)

    return {
        "estado": "Sincronización completa optimizada",
        "agregados": len(uuids_to_insert),
        "eliminados": len(filas_a_insertar)
    }