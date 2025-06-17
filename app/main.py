from fastapi import FastAPI, HTTPException, Request
from app.models import RegistroEntrada
from app.database import (
    crear_tabla_registros,
    crear_tablas_resumen_tarjeta,
    insertar_registro,
    obtener_registros,
    insertar_resumen_tarjeta,
    existe_documento
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
    payload_bytes = await request.body()
    payload_dict = await request.json()

    # SHA del contenido bruto
    document_number = hashlib.sha256(payload_bytes).hexdigest()

    # Primer día del mes actual a las 00:00
    ahora = datetime.now()
    resume_date = datetime(ahora.year, ahora.month, 1, 0, 0, 0)

    if existe_documento(document_number):
        raise HTTPException(status_code=409, detail="Resumen ya existe")

    insertar_resumen_tarjeta(document_number, resume_date, payload_dict)

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