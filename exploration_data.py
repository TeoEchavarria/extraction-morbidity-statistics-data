import pandas as pd
import os
from collections import defaultdict
from pathlib import Path

print("="*80)
print("EXPLORACIÓN DE HEADERS - Procesando...")
print("="*80)

# Diccionario para almacenar resultados
resultados_exploracion = []

# Obtener archivos .xlsx y .xls
carpeta_excels = Path('excels')
archivos = sorted(list(carpeta_excels.glob('*.xlsx')) + list(carpeta_excels.glob('*.xls')))

def fusionar_headers_multilinea(archivo_path, hoja, fila_inicio, num_filas=2):
    """Fusiona headers que están en múltiples filas (merged cells)"""
    try:
        df_raw = pd.read_excel(archivo_path, sheet_name=hoja, header=None)
        
        if fila_inicio + num_filas > len(df_raw):
            return None
        
        # Obtener las filas
        filas = [df_raw.iloc[fila_inicio + i] for i in range(num_filas)]
        
        # Fusionar columnas
        headers_fusionados = []
        for col_idx in range(len(filas[0])):
            partes = []
            for fila in filas:
                valor = str(fila.iloc[col_idx]).strip()
                if valor and valor.lower() not in ['nan', 'unnamed']:
                    partes.append(valor)
            
            if partes:
                headers_fusionados.append('_'.join(partes))
            else:
                headers_fusionados.append(f'Unnamed_{col_idx}')
        
        return headers_fusionados
    except:
        return None

def es_grupo_valido(columnas):
    """Descarta grupos con demasiados Unnamed"""
    if len(columnas) == 0:
        return False
    
    unnamed_count = sum(1 for col in columnas if 'unnamed' in col.lower())
    porcentaje_unnamed = unnamed_count / len(columnas)
    
    # Rechazar si más del 60% son Unnamed
    return porcentaje_unnamed < 0.6

print(f"Analizando {len(archivos)} archivos...\n")

for archivo_path in archivos:
    archivo = archivo_path.name
    
    try:
        # Obtener nombre de la hoja principal
        xl_file = pd.ExcelFile(archivo_path)
        hoja = xl_file.sheet_names[0]
        for h in xl_file.sheet_names:
            if 'datos' in h.lower():
                hoja = h
                break
        
        # Probar las primeras 10 filas como headers (simple)
        for fila_header in range(10):
            try:
                df = pd.read_excel(archivo_path, sheet_name=hoja, header=fila_header, nrows=5)
                columnas = [str(col).strip() for col in df.columns]
                columnas_str = '|'.join(columnas)
                
                # Guardar resultado solo si es válido
                if es_grupo_valido(columnas):
                    resultados_exploracion.append({
                        'archivo': archivo,
                        'fila_header': fila_header,
                        'columnas': columnas,
                        'columnas_str': columnas_str,
                        'num_columnas': len(columnas),
                        'tipo': 'simple'
                    })
                
            except:
                pass
        
        # Probar headers multilineales (fusionando 2 filas)
        for fila_inicio in range(9):  # 0-8 para tener espacio para 2 filas
            headers_fusionados = fusionar_headers_multilinea(archivo_path, hoja, fila_inicio, 2)
            if headers_fusionados and es_grupo_valido(headers_fusionados):
                columnas_str = '|'.join(headers_fusionados)
                resultados_exploracion.append({
                    'archivo': archivo,
                    'fila_header': f'{fila_inicio}-{fila_inicio+1}',
                    'columnas': headers_fusionados,
                    'columnas_str': columnas_str,
                    'num_columnas': len(headers_fusionados),
                    'tipo': 'multilinea'
                })
    
    except Exception as e:
        pass

# Agrupar archivos con columnas similares
print(f"\n{'='*80}")
print("AGRUPACIÓN POR SIMILITUD DE COLUMNAS")
print(f"{'='*80}\n")

# Crear grupos basados en columnas_str
grupos = defaultdict(list)
for resultado in resultados_exploracion:
    key = resultado['columnas_str']
    grupos[key].append({
        'archivo': resultado['archivo'],
        'fila_header': resultado['fila_header'],
        'num_columnas': resultado['num_columnas'],
        'tipo': resultado['tipo']
    })

# Filtrar grupos con más de 1 archivo y ordenar por número de archivos (descendente)
grupos_importantes = {k: v for k, v in grupos.items() if len(v) > 1}
grupos_ordenados = sorted(grupos_importantes.items(), key=lambda x: len(x[1]), reverse=True)

print(f"Total de grupos únicos con 2+ archivos: {len(grupos_importantes)}\n")

for idx, (columnas_str, archivos_grupo) in enumerate(grupos_ordenados, 1):
    columnas = columnas_str.split('|')
    tipo_grupo = archivos_grupo[0]['tipo']
    
    print(f"\n{'─'*80}")
    print(f"GRUPO {idx} - {len(archivos_grupo)} archivos [{tipo_grupo.upper()}]")
    print(f"{'─'*80}")
    print(f"Columnas ({len(columnas)}): {columnas[:10]}{'...' if len(columnas) > 10 else ''}")
    print(f"\nArchivos:")
    for item in archivos_grupo:
        print(f"  • {item['archivo']} (fila {item['fila_header']})")

# Resumen final
print(f"\n\n{'='*80}")
print("RESUMEN")
print(f"{'='*80}")
print(f"Total de archivos analizados: {len(archivos)}")
print(f"Total de combinaciones únicas de columnas: {len(grupos)}")
print(f"Grupos con 2+ archivos: {len(grupos_importantes)}")
print(f"\nTop 3 grupos más grandes:")
for idx, (_, archivos_grupo) in enumerate(grupos_ordenados[:3], 1):
    print(f"  {idx}. {len(archivos_grupo)} archivos")