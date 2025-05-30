import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import re

# ✅ Solicitar datos al usuario
archivo = input("📂 Ingresa el nombre del archivo Excel (con .xlsx): ")
base_datos = input("🔑 Ingresa el nombre de la base de datos MySQL: ")

# 🔐 Conexión a MySQL
usuario = 'root'
password = 'Aprendizaje3042.'
host = '127.0.0.1'
puerto = '3306'

# 🧠 Conectarse
engine = create_engine(f'mysql+pymysql://{usuario}:{password}@{host}:{puerto}/{base_datos}')
conn = engine.connect()
print("✅ Conexión a MySQL exitosa.")

# 📄 Leer las hojas del archivo Excel
try:
    hojas = pd.ExcelFile(archivo).sheet_names
    print(f"📄 Hojas encontradas: {hojas}")
except Exception as e:
    print("❌ Error al leer el archivo Excel:", e)
    exit()

# 🔎 Función para inferir tipos SQL
def inferir_sql(col, serie):
    if pd.api.types.is_integer_dtype(serie):
        return "BIGINT" if serie.max() > 2147483647 else "INT"
    elif pd.api.types.is_float_dtype(serie):
        return "DOUBLE"
    elif pd.api.types.is_datetime64_any_dtype(serie):
        return "DATE"
    else:
        longitud = serie.astype(str).str.len().max()
        return f"VARCHAR({min(int(longitud) + 10, 255)})"

# 🔁 Procesar cada hoja
for hoja in hojas:
    try:
        print(f"\n🔄 Procesando hoja: {hoja}")
        df = pd.read_excel(archivo, sheet_name=hoja)
        df.columns = [re.sub(r'\W+', '_', str(c).strip())[:64] for c in df.columns]  # Limitar a 64 caracteres

        nombre_tabla = hoja.strip().lower().replace(" ", "_")[:64]

        # Tipos SQL por columna
        tipos_sql = {col: inferir_sql(col, df[col]) for col in df.columns}
        columnas_sql = ",\n  ".join([f"`{col}` {tipo}" for col, tipo in tipos_sql.items()])

        # 🔄 Borrar tabla si existe
        try:
            conn.execute(text(f"DROP TABLE IF EXISTS `{nombre_tabla}`"))
            print(f"🧹 Tabla '{nombre_tabla}' eliminada (si existía).")
        except Exception as e:
            print(f"❌ Error eliminando tabla '{nombre_tabla}':", e)
            continue

        # 🏗️ Crear nueva tabla
        create_sql = f"""
        CREATE TABLE `{nombre_tabla}` (
          {columnas_sql}
        );
        """
        try:
            conn.execute(text(create_sql))
            conn.commit()
            print(f"✅ Tabla '{nombre_tabla}' creada.")
        except Exception as e:
            print(f"❌ Error creando tabla '{nombre_tabla}':", e)
            continue

        # 📥 Insertar datos
        try:
            df.to_sql(nombre_tabla, con=engine, if_exists='append', index=False)
            print(f"📥 Datos insertados en '{nombre_tabla}'.")
        except Exception as e:
            print(f"❌ Error insertando datos en '{nombre_tabla}':", e)

    except Exception as e:
        print(f"❌ Error procesando hoja '{hoja}':", e)

# 🔒 Cerrar conexión
conn.close()
print("\n🔒 Conexión cerrada.")
