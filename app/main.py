from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.models import RegistroEntrada
from app.database import (
    crear_tabla_registros,
    crear_tablas_resumen_tarjeta,
    insertar_registro,
    obtener_registros,
    insertar_resumen_tarjeta,
    existe_documento,
    get_sqlite_expense_uuids,
    get_sqlite_income_uuids,
    delete_expenses,
    insert_expenses,
    delete_incomes,
    insert_incomes,
    create_income_table,
    get_incomes,
    obtener_tarjetas_disponibles
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
import httpx
import re
from pathlib import Path

# DEBUG remoto si está activo
if os.getenv("DEBUG_MODE") == "1":
    import debugpy
    debugpy.listen(("0.0.0.0", 5678))
    print("🛠 Esperando conexión de debugger...")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Crear las tablas al iniciar la app
crear_tabla_registros()
create_income_table()
crear_tablas_resumen_tarjeta()

# ------------------- Card Resume load -------------------

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

@app.get("/syncResumes")
async def sync_resumes():
    PARSE_PDF_ENDPOINT = os.getenv("PARSE_PDF_ENDPOINT")
    RESUMENES_DIR = Path(os.getenv("RESUMES_LOCAL_LOCATION", str(Path.home() / "resumenes")))

    tarjetas = ["visa", "mastercard"]
    resultados = []
    exitosos = []
    fallidos = []

    async with httpx.AsyncClient() as client:
        for tarjeta in tarjetas:
            carpeta = RESUMENES_DIR / tarjeta
            if not carpeta.exists():
                continue

            for pdf_file in carpeta.glob("*.pdf"):
                match = re.match(r"(\d{2})-(\d{4})\.pdf", pdf_file.name)
                if not match:
                    fallidos.append({"archivo": pdf_file.name, "motivo": "Nombre inválido", "card_type": tarjeta})
                    continue

                mes, anio = int(match.group(1)), int(match.group(2))
                resultado = {
                    "month": mes,
                    "year": anio,
                    "card_type": tarjeta,
                    "archivo": pdf_file.name
                }

                try:
                    with open(pdf_file, "rb") as f:
                        files = {"file": (pdf_file.name, f, "application/pdf")}
                        response = await client.post(PARSE_PDF_ENDPOINT, files=files)
                        response.raise_for_status()
                        parsed_data = response.json()
                        resultado["resume_data"] = parsed_data
                except httpx.HTTPError as e:
                    fallidos.append({"archivo": pdf_file.name, "motivo": f"POST fallido: {str(e)}", "card_type": tarjeta})
                    continue

                resultados.append(resultado)

    # Segunda etapa: insertar en BD los que tienen resume_data
    for r in resultados:
        if "resume_data" not in r:
            fallidos.append({"archivo": r["archivo"], "motivo": "PDF sin datos útiles", "card_type": r["card_type"]})
            continue

        try:
            payload_dict = r["resume_data"]
            
            payload_bytes = json.dumps(payload_dict, ensure_ascii=False).encode("utf-8")
            document_number = hashlib.sha256(payload_bytes).hexdigest()
            resume_date = datetime(r["year"], r["month"], 1, 0, 0, 0)

            if existe_documento(document_number):
                fallidos.append({"archivo": r["archivo"], "motivo": "Resumen ya existe", "card_type": r["card_type"]})
                continue

            insertar_resumen_tarjeta(document_number, resume_date, payload_dict, r["card_type"])
            exitosos.append({"archivo": r["archivo"], "card_type": r["card_type"]})
        except Exception as e:
            fallidos.append({"archivo": r["archivo"], "motivo": f"Error al guardar: {str(e)}", "card_type": r["card_type"]}) 

    return JSONResponse({
        "procesados_ok": exitosos,
        "procesados_fallidos": fallidos
    })

@app.get("/getResumeExpenses/{anio}/{mes}")
@app.get("/getResumeExpenses/{anio}/{mes}/{card_type}")
@app.get("/getResumeExpenses/{anio}/{mes}/{card_type}/{holder}")
def get_resume_expenses(anio: int, mes: int, card_type: str = None, holder: str = None):
    from app.database import obtener_resumen
    resumen = obtener_resumen(anio, mes, card_type, holder)
    return resumen

@app.get("/getAvailableResumes/{anio}/{mes}")
def get_available_resumes(anio: int, mes: int):
    return obtener_tarjetas_disponibles(anio, mes)

# -------------------------------------------------------------------------
# ------------------- Expenses & Income -----------------------------------
# -------------------------------------------------------------------------

# ++++ Fetch data ++++

@app.get("/expenses/{anio}/{mes}")
def get_expenses(anio: int, mes: int):
    registros = obtener_registros(anio, mes)
    return {"expenses": registros}

@app.get("/incomes/{anio}/{mes}")
def get_income(anio: int, mes: int):
    income = get_incomes(anio, mes)
    return {"income": income}

# ++++ Sync data ++++

def sync_data(
    get_sheet_data_func: Callable,
    insert_func: Callable,
    delete_func: Callable,
    get_sqlite_uuids_func: Callable,
    label: str
):
    start_time = time.time()
    print(f"\n synching: {label}...")

    try:
        client = auth_in_gdrive()
        print("Google drive authentication complete")

        sheet = get_sheet_data_func(client)
        print(f"Google Sheet rows obtained: {len(sheet)}")

        sheet_uuids = set(row["UUID"] for row in sheet if row.get("UUID"))
        sqlite_uuids = get_sqlite_uuids_func()
        print(f"UUIDs on google sheet: {len(sheet_uuids)} | UUIDs on SQLite: {len(sqlite_uuids)}")

        uuids_to_insert = sheet_uuids - sqlite_uuids
        uuids_to_delete = sqlite_uuids - sheet_uuids
        rows_to_insert = [row for row in sheet if row.get("UUID") in uuids_to_insert]

        print(f"Inserting {len(rows_to_insert)} new records")
        insert_func(rows_to_insert)

        print(f"Deleting {len(uuids_to_delete)} old records")
        delete_func(uuids_to_delete)

        duration = round(time.time() - start_time, 2)
        print(f"Synching complete in {duration} seconds")

        return {
            "state": f"{label} updated",
            "added": len(uuids_to_insert),
            "deleted": len(uuids_to_delete),
            "duration_sec": duration
        }

    except Exception as e:
        print("Error while synching")
        traceback.print_exc()
        return {
            "state": f"Error syncing {label}",
            "error": str(e)
        }
    
@app.get("/syncHistoricExpenses")
def sync_historic_expenses():
    return sync_data(get_historic_expenses, insert_expenses, delete_expenses, get_sqlite_expense_uuids, "Historic expenses")

@app.get("/syncCurrentMonthExpenses")
def sync_current_month_expenses():
    return sync_data(get_current_month_expenses, insert_expenses, delete_expenses, get_sqlite_expense_uuids, "Monthly expenses")

@app.get("/syncHistoricIncome")
def sync_historic_income():
    return sync_data(get_historic_income, insert_incomes, delete_incomes, get_sqlite_income_uuids, "Historic incomes")

@app.get("/syncCurrentMonthIncome")
def sync_current_month_income():
    return sync_data(get_current_month_income, insert_incomes, delete_incomes, get_sqlite_income_uuids, "Monthly incomes")