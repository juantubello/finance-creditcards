import sqlite3
from pathlib import Path
from datetime import datetime
import requests

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
                uuid TEXT UNIQUE,
                marca_temporal TEXT,
                descripcion TEXT,
                importe REAL,
                tipo TEXT
            )
        """)
        conn.commit()

def create_income_table():
    with conectar(REGISTROS_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS income (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT UNIQUE,
                marca_temporal TEXT,
                descripcion TEXT,
                importe REAL,
                moneda TEXT
            )
        """)
        conn.commit()

def insertar_registro(uuid, marca_temporal, descripcion, importe, tipo):
    with conectar(REGISTROS_DB) as conn:
        try:
            conn.execute("""
                INSERT INTO registros (uuid, marca_temporal, descripcion, importe, tipo)
                VALUES (?, ?, ?, ?, ?)
            """, (uuid, marca_temporal, descripcion, importe, tipo))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

def obtener_registros(anio, mes):
    with conectar(REGISTROS_DB) as conn:
        cursor = conn.execute("""
            SELECT uuid, marca_temporal, descripcion, importe, tipo
            FROM registros
            WHERE strftime('%Y', marca_temporal) = ? AND strftime('%m', marca_temporal) = ?
        """, (str(anio), f"{int(mes):02}"))

        registros = []
        total_importe = 0.0
        totales_por_tipo = {}  # Diccionario para acumular totales por categoría

        for row in cursor.fetchall():
            uuid, marca_temporal, descripcion, importe, tipo = row
            total_importe += importe
            
            # Acumular el importe por tipo/categoría
            if tipo in totales_por_tipo:
                totales_por_tipo[tipo] += importe
            else:
                totales_por_tipo[tipo] = importe

            importe_str = f"{importe:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
            registros.append({
                "uuid": uuid,
                "datetime": marca_temporal,
                "description": descripcion,
                "amount": importe_str,
                "type": tipo
            })

        total_str = f"{total_importe:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
        
        # Formatear los totales por tipo con el formato de moneda
        totales_por_tipo_formateados = {
            tipo: f"{importe:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
            for tipo, importe in totales_por_tipo.items()
        }

        return {
            "total": total_str,
            "total_by_expense_type": totales_por_tipo_formateados,
            "expenses": registros
        }

def get_incomes(anio, mes):
 
 dolar_blue_buy = get_dolar_blue_buy()
  
 with conectar(REGISTROS_DB) as conn:
        cursor = conn.execute("""
            SELECT uuid, marca_temporal, descripcion, importe, moneda
            FROM income
            WHERE strftime('%Y', marca_temporal) = ? AND strftime('%m', marca_temporal) = ?
        """, (str(anio), f"{int(mes):02}"))

        registros = []
        total_ars = 0.0
        total_usd = 0.0

        for row in cursor.fetchall():
            uuid, marca_temporal, descripcion, importe, moneda = row     
            
            amount_ars = 0.0
            amount_usd = 0.0

            if moneda == "USD":
                amount_ars = importe * dolar_blue_buy
                amount_usd = importe
            else:
                amount_ars = importe
                amount_usd = importe / dolar_blue_buy

            amount_ars_str = f"{amount_ars:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
            amount_usd_str = f"{amount_usd:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")

            registros.append({
                "uuid": uuid,
                "datetime": marca_temporal,
                "description": descripcion,
                "amount_pesos": amount_ars_str,
                "amount_usd": amount_usd_str
             })
            
            total_ars += amount_ars
            total_usd += amount_usd
        
        total_ars_str = f"{total_ars:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
        total_usd_str = f"{total_usd:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")

        return {
            "total_ars": total_ars_str,
            "total_usd": total_usd_str,
            "incomes": registros
        }

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


def insertar_resumen_tarjeta(document_number, resume_date, payload_dict, card_type):
    with conectar(TARJETAS_DB) as conn:
        card_type = card_type
        total_ars = 0
        total_usd = 0

        for holder, data in payload_dict.items():
            if (holder != "Total"):
                continue
            total_ars = float(data["pesos"].replace(".", "").replace(",", "."))
            total_usd = float(data["dolares"].replace(",", "."))

        for holder, data in payload_dict.items():

            if holder == "Total":
                continue

            # Insertar header solo una vez (lo repetimos por cada holder, pero la PK lo previene)
            conn.execute("""
                INSERT OR IGNORE INTO cards_resume_header (document_number, card_type, resume_date, total_ars, total_usd)
                VALUES (?, ?, ?, ?, ?)
            """, (document_number, card_type, resume_date.isoformat(), total_ars, total_usd))

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

    total_ars_cards = 0
    total_usd_cards = 0

    resumen = {
        "cards": [],
        "total_ars_cards": total_ars_cards,
        "total_usd_cards": total_usd_cards
    }

    with conectar(TARJETAS_DB) as conn:

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
            total_ars_cards += row[3]
            total_usd_cards += row[4]

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
                    if "USD" not in e_desc:
                        expense = {
                            "date": e_date_fmt,
                            "descriptions": e_desc,
                            "amount_pesos": f"{e_amount:,.2f}".replace(",", "#").replace(".", ",").replace("#", "."),
                            "amount_usd":""
                        }
                    else:
                        expense = {
                            "date": e_date_fmt,
                            "descriptions": e_desc,
                            "amount_pesos": "",
                            "amount_usd": f"{e_amount:,.2f}".replace(",", "#").replace(".", ",").replace("#", "."),
                        }

                    holder_info["expenses"].append(expense)

                card["holders"].append(holder_info)

            resumen["cards"].append(card)
            resumen["total_ars_cards"] = f"{total_ars_cards:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")
            resumen["total_usd_cards"] = f"{total_usd_cards:,.2f}".replace(",", "#").replace(".", ",").replace("#", ".")

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
    
def get_sqlite_expense_uuids():
    with conectar(REGISTROS_DB) as conn:
        cursor = conn.execute("SELECT uuid FROM registros")
        return set(row[0] for row in cursor.fetchall())

def get_sqlite_income_uuids():
    with conectar(REGISTROS_DB) as conn:
        cursor = conn.execute("SELECT uuid FROM income")
        return set(row[0] for row in cursor.fetchall())

def delete_expenses(uuids: set):
    if not uuids:
        return
    with conectar(REGISTROS_DB) as conn:
        conn.executemany(
            "DELETE FROM registros WHERE uuid = ?",
            [(uuid,) for uuid in uuids]
        )
        conn.commit()

def insert_expenses(filas: list[dict]):
    if not filas:
        return
    datos = []
    for row in filas:
        try:
            uuid = row["UUID"]
            marca_temporal = datetime.strptime(row["Marca temporal"], "%d/%m/%Y %H:%M:%S").isoformat()
            descripcion = row["Descripción"]
            # ✅ Manejo correcto del formato $123,456.78
            importe_str = row["Importe"].replace("$", "").replace(",", "")
            importe = float(importe_str)
            tipo = row["Tipo de gatos"]
            datos.append((uuid, marca_temporal, descripcion, importe, tipo))
        except Exception as e:
            print(f"❌ Error procesando fila con UUID {row.get('UUID')}: {e}")

    with conectar(REGISTROS_DB) as conn:
        conn.executemany("""
            INSERT INTO registros (uuid, marca_temporal, descripcion, importe, tipo)
            VALUES (?, ?, ?, ?, ?)
        """, datos)
        conn.commit()

def delete_incomes(uuids: set):
    if not uuids:
        return
    with conectar(REGISTROS_DB) as conn:
        conn.executemany(
            "DELETE FROM income WHERE uuid = ?",
            [(uuid,) for uuid in uuids]
        )
        conn.commit()

def insert_incomes(filas: list[dict]):
    if not filas:
        return
    datos = []
    for row in filas:
        try:
            uuid = row["UUID"]
            marca_temporal = datetime.strptime(row["Marca temporal"], "%d/%m/%Y %H:%M:%S").isoformat()
            descripcion = row["Descripcion"]
            # ✅ Manejo correcto del formato $123,456.78
            importe_str = row["Importe"].replace("$", "").replace(",", "")
            importe = float(importe_str)
            moneda = row["Moneda"]
            datos.append((uuid, marca_temporal, descripcion, importe, moneda))
        except Exception as e:
            print(f"❌ Error procesando fila con UUID {row.get('UUID')}: {e}")

    with conectar(REGISTROS_DB) as conn:
        conn.executemany("""
            INSERT INTO income (uuid, marca_temporal, descripcion, importe, moneda)
            VALUES (?, ?, ?, ?, ?)
        """, datos)
        conn.commit()

def get_dolar_blue_buy():
 response = requests.get("https://api.bluelytics.com.ar/v2/latest")
 dolar_data = response.json()
 dolar_blue_data = dolar_data.get('blue')
 dolar_blue_buy = dolar_blue_data.get('value_buy')
 return dolar_blue_buy