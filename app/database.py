import sqlite3
from hashlib import sha256
from pathlib import Path
from datetime import datetime

# Paths para cada base de datos
REGISTROS_DB = Path(__file__).resolve().parent.parent / "data" / "registros.db"
TARJETAS_DB = Path(__file__).resolve().parent.parent / "data" / "tarjetas.db"

# Asegurar que la carpeta data/ exista
REGISTROS_DB.parent.mkdir(exist_ok=True, parents=True)

def conectar(db_path: Path):
    return sqlite3.connect(db_path)


# ------------------- REGISTROS GENERALES -------------------

def crear_tabla_registros():
    with conectar(REGISTROS_DB) as conn:
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

    with conectar(REGISTROS_DB) as conn:
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
    with conectar(REGISTROS_DB) as conn:
        cursor = conn.execute("""
            SELECT cable, marca_temporal, descripcion, importe, tipo
            FROM registros
            WHERE strftime('%Y', marca_temporal) = ? AND strftime('%m', marca_temporal) = ?
        """, (str(anio), f"{int(mes):02}"))
        return [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]


# ------------------- RESUMEN TARJETAS -------------------

def crear_tablas_resumen_tarjeta():
    with conectar(TARJETAS_DB) as conn:
        # Tabla principal del resumen
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cards_resume_header (
                document_number TEXT PRIMARY KEY,
                card_type TEXT,
                resume_date TEXT,
                total_ars REAL,
                total_usd REAL
            )
        """)

        # Resumen por titular
        conn.execute("""
            CREATE TABLE IF NOT EXISTS card_resume_holder (
                document_number TEXT,
                holder TEXT,
                total_ars REAL,
                total_usd REAL,
                PRIMARY KEY (document_number, holder),
                FOREIGN KEY (document_number) REFERENCES cards_resume_header(document_number)
            )
        """)

        # Detalles por titular
        conn.execute("""
            CREATE TABLE IF NOT EXISTS card_holder_expenses (
                document_number TEXT,
                holder TEXT,
                position INTEGER,
                date TEXT,
                description TEXT,
                amount REAL,
                PRIMARY KEY (document_number, holder, position),
                FOREIGN KEY (document_number, holder) REFERENCES card_resume_holder(document_number, holder)
            )
        """)
        conn.commit()


def existe_documento(document_number):
    with conectar(TARJETAS_DB) as conn:
        cursor = conn.execute(
            "SELECT COUNT(*) FROM cards_resume_header WHERE document_number = ?", (document_number,)
        )
        return cursor.fetchone()[0] > 0


def insertar_resumen_tarjeta(document_number, resume_date, payload_dict):
    with conectar(TARJETAS_DB) as conn:
        for holder, data in payload_dict.items():
            total_ars = float(data["Total"]["pesos"].replace(".", "").replace(",", "."))
            total_usd = float(data["Total"]["dolares"].replace(",", "."))

            # Insertar header solo una vez (lo repetimos por cada holder, pero la PK lo previene)
            conn.execute("""
                INSERT OR IGNORE INTO cards_resume_header (document_number, card_type, resume_date, total_ars, total_usd)
                VALUES (?, ?, ?, ?, ?)
            """, (document_number, "Visa", resume_date.isoformat(), total_ars, total_usd))

            # Insertar holder resumen
            conn.execute("""
                INSERT INTO card_resume_holder (document_number, holder, total_ars, total_usd)
                VALUES (?, ?, ?, ?)
            """, (document_number, holder, total_ars, total_usd))

            # Insertar gastos del holder
            for idx, gasto in enumerate(data["Detail"]):
                fecha = gasto["fechaTimestamp"]
                descripcion = gasto["descripcion"]
                importe = float(gasto["importe"].replace(".", "").replace(",", "."))

                conn.execute("""
                    INSERT INTO card_holder_expenses (document_number, holder, position, date, description, amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (document_number, holder, idx, fecha, descripcion, importe))

        conn.commit()

def obtener_resumen(anio, mes, card_type=None, holder=None):
    resumen = {
        "cards": [],
        "total_ars_cards": "#Vacio por el momento",
        "total_usd_cards": "#vacio por el momento"
    }

    with conectar(TARJETAS_DB) as conn:
        filtro_fecha = datetime(anio, mes, 1).strftime('%Y-%m-01')

        query = """
                 SELECT * FROM cards_resume_header
                 WHERE strftime('%Y', resume_date) = ? AND strftime('%m', resume_date) = ?
                 """
        params = [str(anio), f"{int(mes):02}"]

        if card_type:
            query += " AND card_type = ?"
            params.append(card_type)

        for row in conn.execute(query, params).fetchall():
            card = {
                "card_type": row[1],
                "holders": [],
                "total_ars_card": "#Vacio por el momento",
                "total_usd_card": "#vacio por el momento"
            }

            doc_number = row[0]

            # Holders para esta tarjeta
            holder_query = "SELECT holder, total_ars, total_usd FROM card_resume_holder WHERE document_number = ?"
            holder_params = [doc_number]
            if holder:
                holder_query += " AND holder = ?"
                holder_params.append(holder)

            for h_row in conn.execute(holder_query, holder_params).fetchall():
                h_name, h_ars, h_usd = h_row
                holder_info = {
                    "holder": h_name,
                    "total_ars": f"{h_ars:,.2f}".replace(",", "#").replace(".", ",").replace("#", "."),
                    "total_usd": f"{h_usd:,.2f}".replace(",", "#").replace(".", ",").replace("#", "."),
                    "expenses": []
                }

                # Gastos
                expense_query = """
                    SELECT date, description, amount
                    FROM card_holder_expenses
                    WHERE document_number = ? AND holder = ?
                """
                for e_row in conn.execute(expense_query, (doc_number, h_name)).fetchall():
                    e_date, e_desc, e_amount = e_row
                    e_date_fmt = datetime.fromisoformat(e_date).strftime("%d-%b-%y")
                    expense = {
                        "date": e_date_fmt,
                        "descriptions": e_desc,
                        "amount": f"{e_amount:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
                    }
                    holder_info["expenses"].append(expense)

                card["holders"].append(holder_info)

            resumen["cards"].append(card)

    return resumen


def obtener_tarjetas_disponibles(anio, mes):
    resultado = []
    with conectar(TARJETAS_DB) as conn:
        filtro_fecha = datetime(anio, mes, 1).strftime('%Y-%m-01')
        cursor = conn.execute("""
            SELECT crh.card_type, crh.document_number, crh.resume_date, crh.total_ars, crh.total_usd, crh.card_type
            FROM cards_resume_header crh
            WHERE resume_date = ?
        """, (filtro_fecha,))

        tarjetas = {}
        for row in cursor.fetchall():
            card_type = row[0]
            doc_number = row[1]

            if card_type not in tarjetas:
                tarjetas[card_type] = []

            # Obtener holders asociados
            holder_cursor = conn.execute("""
                SELECT holder FROM card_resume_holder WHERE document_number = ?
            """, (doc_number,))
            holders = [h[0] for h in holder_cursor.fetchall()]
            tarjetas[card_type].extend(holders)

        return {
            "available_cards": [
                {"card_type": tipo, "holders": list(set(holders))}
                for tipo, holders in tarjetas.items()
            ]
        }
