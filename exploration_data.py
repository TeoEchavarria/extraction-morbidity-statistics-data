import pandas as pd
import os

# Escaneo rápido sin cargar todo en memoria
for archivo in os.listdir('excels/'):
    if archivo.endswith('.xlsx'):
        xl_file = pd.ExcelFile(f'excels/{archivo}')
        print(f"\n{archivo}:")
        print(f"  Hojas: {xl_file.sheet_names}")
        df_head = pd.read_excel(f'excels/{archivo}', nrows=3)
        print(f"  Columnas: {df_head.columns.tolist()}")