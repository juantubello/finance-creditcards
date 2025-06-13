from fastapi import FastAPI, HTTPException
from app.models import RegistroEntrada
from app.database import crear_tabla, insertar_registro, obtener_registros

app = FastAPI()

crear_tabla()
#test
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

@app.get("/registros/{anio}/{mes}")
def listar_registros(anio: int, mes: int):
    registros = obtener_registros(anio, mes)
    return {"registros": registros}
