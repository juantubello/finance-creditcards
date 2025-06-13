import sqlite3
from hashlib import sha256
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "registros.db"
DB_PATH.parent.mkdir(exist_ok=True, parents=True)

def conectar():
    return sqlite3.connect(DB_PATH)

def crear_tabla():
    with conectar() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS registros (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cable TEXT UNIQUE,
                marca_temporal TEXT,
                descripcion TEXT,
                importe REAL,
                tipo TEXT
            )
        """)
        conn.commit()

def insertar_registro(marca_temporal, descripcion, importe, tipo):
    raw = f"{marca_temporal}-{descripcion}-{importe}-{tipo}"
    cable = sha256(raw.encode('utf-8')).hexdigest()

    with conectar() as conn:
        try:
            conn.execute("""
                INSERT INTO registros (cable, marca_temporal, descripcion, importe, tipo)
                VALUES (?, ?, ?, ?, ?)
            """, (cable, marca_temporal, descripcion, importe, tipo))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

def obtener_registros(anio, mes):
    with conectar() as conn:
        cursor = conn.execute("""
            SELECT cable, marca_temporal, descripcion, importe, tipo
            FROM registros
            WHERE strftime('%Y', marca_temporal) = ? AND strftime('%m', marca_temporal) = ?
        """, (str(anio), f"{int(mes):02}"))
        return [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
