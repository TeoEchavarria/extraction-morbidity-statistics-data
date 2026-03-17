import pandas as pd
import os
import re
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

def normalizar_nombre_columna(col):
    """Normaliza nombres de columnas para estandarizar agrupación"""
    col = str(col).strip()
    col = col.lower()
    col = col.replace('unnamed: ', 'unnamed_')
    col = col.replace('\n', ' ').replace('\r', '')
    
    # Si es un número, normalizarlo
    try:
        num = float(col)
        if num.is_integer():
            col = str(int(num))
        else:
            col = str(num)
    except ValueError:
        pass
    
    col = ' '.join(col.split())
    return col

# ============================================================================
# MAPEO SEMÁNTICO MEJORADO
# ============================================================================

SINONIMOS_COLUMNAS = {
    # Códigos
    'codigo_diagnostico': [
        'código causa', 'cod_dx', 'codigo dx', 'codigo causa', 'cod causa', 
        'nro', 'código de causa', 'cod dx', 'codigo_causa'
    ],
    'codigo_municipio': [
        'código dane del municipio', 'cod_municipio', 'codigo municipio',
        'código de municipio', 'codigo dane', 'cod municipio', 'cod_mpio'
    ],
    
    # Descripción/Diagnóstico
    'diagnostico': [
        'causas', 'causa', 'diagnóstico', 'diagnostico', 'descripcion', 
        'descripción', 'dx', 'enfermedad'
    ],
    
    # Totales y conteos
    'total': ['total', 'total general', 'n°', 'n', 'numero', 'cantidad', 'casos'],
    
    # Porcentaje
    'porcentaje': ['%', 'distribución %', 'porcentaje', 'distribucion %', 'distribución', 'pct'],
    
    # Zona geográfica
    'zona_urbana': ['cabecera', 'urbana', 'urbano', 'zona_urbana', 'zona urbana'],
    'zona_rural': ['resto', 'rural', 'zona_rural', 'zona rural'],
    
    # Sexo
    'sexo_masculino': [
        'hombre', 'masculino', 'hombres', 'sexo_hombre', 'sexo hombre',
        'masc', 'm', 'sexo_masculino'
    ],
    'sexo_femenino': [
        'mujer', 'femenino', 'mujeres', 'sexo_mujer', 'sexo mujer',
        'fem', 'f', 'sexo_femenino'
    ],
    'sexo_no_definido': [
        'no definido / no reportado', 'no definido/ no reportado', 
        'no definido no reportado', 'no reportado', 'no definido', 
        'indeterminado', 'sin definir', 'nd'
    ],
    
    # Ubicación geográfica
    'subregion': [
        'subregiones', 'subregion', 'subregión', 'nom_regional', 'region', 
        'regional', 'subregiones y municipios', 'nombre regional'
    ],
    'municipio': [
        'municipios y distritos', 'municipio', 'municipios', 'nom_mpio', 
        'nom_municipio', 'nombre municipio', 'mpio', 'distrito'
    ],
}

def normalizar_semantico(col):
    """Normaliza semánticamente una columna a su nombre canónico"""
    col_normalizado = normalizar_nombre_columna(col)
    
    # Si es unnamed, marcador genérico
    if 'unnamed' in col_normalizado:
        return '_UNNAMED_'
    
    # Si es un número puro (grupo de edad), marcarlo
    try:
        num = int(col_normalizado)
        if 0 <= num <= 25:  # Grupos de edad típicos
            return '_GRUPO_EDAD_'
        return col_normalizado
    except:
        pass
    
    # Buscar en sinónimos (match exacto o contenido)
    for nombre_canonico, sinonimos in SINONIMOS_COLUMNAS.items():
        for sinonimo in sinonimos:
            if col_normalizado == sinonimo:
                return nombre_canonico
            # Match parcial solo para términos largos
            if len(sinonimo) > 4 and sinonimo in col_normalizado:
                return nombre_canonico
    
    return col_normalizado

def obtener_columnas_nucleo(columnas_semanticas):
    """Extrae solo las columnas importantes, ignorando UNNAMED y orden"""
    # Columnas importantes a detectar
    columnas_importantes = {
        'codigo_diagnostico', 'codigo_municipio', 'diagnostico', 'total', 
        'porcentaje', 'zona_urbana', 'zona_rural', 'sexo_masculino', 
        'sexo_femenino', 'sexo_no_definido', 'subregion', 'municipio'
    }
    
    # Extraer columnas importantes presentes
    presentes = set()
    tiene_grupos_edad = False
    
    for col in columnas_semanticas:
        if col == '_GRUPO_EDAD_':
            tiene_grupos_edad = True
        elif col in columnas_importantes:
            presentes.add(col)
    
    return presentes, tiene_grupos_edad

# ============================================================================
# CLASIFICACIÓN POR TIPO DE REPORTE
# ============================================================================

def clasificar_tipo_reporte(nombre_archivo, columnas_nucleo, tiene_grupos_edad):
    """Clasifica el archivo en un tipo de reporte basado en columnas + nombre"""
    nombre = nombre_archivo.lower()
    
    # Tipo AGRUPACION22 - tiene grupos de edad numéricos
    if tiene_grupos_edad:
        if 'municipio' in columnas_nucleo or 'mpio' in nombre or 'region' in nombre:
            return 'AGRUPACION22_MUNICIPIO'
        return 'AGRUPACION22_DEPTO'
    
    # Detectar por nivel geográfico
    tiene_municipio = 'municipio' in columnas_nucleo or 'codigo_municipio' in columnas_nucleo
    tiene_subregion = 'subregion' in columnas_nucleo
    
    # Detectar por nombre de archivo
    es_top10 = any(x in nombre for x in ['diezprimeras', 'diez_primeras', 'top10', '10_primeras'])
    es_por_mpio = any(x in nombre for x in ['mpio', 'municipio', 'por_mpio'])
    es_por_subregion = any(x in nombre for x in ['subregion', 'subregión', 'por_subregion'])
    es_departamento = 'departamento' in nombre or 'total_departamento' in nombre
    es_morbilidad = 'morbilidad' in nombre and 'formato' in nombre
    
    # Clasificar
    if es_morbilidad:
        return 'MORBILIDAD_FORMATO'
    
    if tiene_municipio or es_por_mpio:
        return 'CAUSAS_MUNICIPIO'
    
    if tiene_subregion or es_por_subregion:
        return 'CAUSAS_SUBREGION'
    
    if es_departamento:
        return 'CAUSAS_DEPARTAMENTO'
    
    # Por defecto, es TOP10 simple
    if 'codigo_diagnostico' in columnas_nucleo:
        return 'CAUSAS_CODIGO'
    
    return 'CAUSAS_SIMPLE'

def calcular_score_variante(resultado):
    """Calcula un score para elegir la mejor variante de un archivo"""
    columnas = resultado['columnas_semanticas']
    
    # Penalizar UNNAMED
    num_unnamed = sum(1 for c in columnas if c == '_UNNAMED_')
    
    # Premiar columnas mapeadas correctamente
    columnas_importantes = {
        'codigo_diagnostico', 'codigo_municipio', 'diagnostico', 'total', 
        'porcentaje', 'zona_urbana', 'zona_rural', 'sexo_masculino', 
        'sexo_femenino', 'subregion', 'municipio'
    }
    num_mapeadas = sum(1 for c in columnas if c in columnas_importantes)
    
    # Preferir simple sobre multilinea si empatan
    bonus_simple = 0.5 if resultado['tipo'] == 'simple' else 0
    
    return num_mapeadas * 10 - num_unnamed * 5 + bonus_simple

print(f"Analizando {len(archivos)} archivos...\n")

for archivo_path in archivos:
    archivo = archivo_path.name
    
    try:
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
                columnas_semanticas = [normalizar_semantico(c) for c in columnas]
                
                if es_grupo_valido(columnas):
                    columnas_nucleo, tiene_grupos_edad = obtener_columnas_nucleo(columnas_semanticas)
                    tipo_reporte = clasificar_tipo_reporte(archivo, columnas_nucleo, tiene_grupos_edad)
                    
                    resultados_exploracion.append({
                        'archivo': archivo,
                        'fila_header': fila_header,
                        'columnas': columnas,
                        'columnas_semanticas': columnas_semanticas,
                        'columnas_nucleo': columnas_nucleo,
                        'tiene_grupos_edad': tiene_grupos_edad,
                        'tipo_reporte': tipo_reporte,
                        'num_columnas': len(columnas),
                        'tipo': 'simple'
                    })
            except:
                pass
        
        # Probar headers multilineales
        for fila_inicio in range(9):
            headers_fusionados = fusionar_headers_multilinea(archivo_path, hoja, fila_inicio, 2)
            if headers_fusionados and es_grupo_valido(headers_fusionados):
                columnas_semanticas = [normalizar_semantico(c) for c in headers_fusionados]
                columnas_nucleo, tiene_grupos_edad = obtener_columnas_nucleo(columnas_semanticas)
                tipo_reporte = clasificar_tipo_reporte(archivo, columnas_nucleo, tiene_grupos_edad)
                
                resultados_exploracion.append({
                    'archivo': archivo,
                    'fila_header': f'{fila_inicio}-{fila_inicio+1}',
                    'columnas': headers_fusionados,
                    'columnas_semanticas': columnas_semanticas,
                    'columnas_nucleo': columnas_nucleo,
                    'tiene_grupos_edad': tiene_grupos_edad,
                    'tipo_reporte': tipo_reporte,
                    'num_columnas': len(headers_fusionados),
                    'tipo': 'multilinea'
                })
    except:
        pass

# ============================================================================
# SELECCIONAR MEJOR VARIANTE POR ARCHIVO
# ============================================================================

print(f"\n{'='*80}")
print("SELECCIÓN DE MEJOR VARIANTE POR ARCHIVO")
print(f"{'='*80}\n")

# Agrupar por archivo
variantes_por_archivo = defaultdict(list)
for r in resultados_exploracion:
    variantes_por_archivo[r['archivo']].append(r)

# Seleccionar la mejor variante de cada archivo
mejores_variantes = []
for archivo, variantes in variantes_por_archivo.items():
    # Calcular score para cada variante
    variantes_con_score = [(v, calcular_score_variante(v)) for v in variantes]
    # Seleccionar la de mayor score
    mejor = max(variantes_con_score, key=lambda x: x[1])
    mejores_variantes.append(mejor[0])

print(f"Archivos procesados: {len(mejores_variantes)}")
print(f"Variantes descartadas: {len(resultados_exploracion) - len(mejores_variantes)}")

# ============================================================================
# AGRUPAR POR TIPO DE REPORTE
# ============================================================================

print(f"\n{'='*80}")
print("AGRUPACIÓN POR TIPO DE REPORTE")
print(f"{'='*80}\n")

# Agrupar por tipo
grupos_por_tipo = defaultdict(list)
for v in mejores_variantes:
    grupos_por_tipo[v['tipo_reporte']].append(v)

# Ordenar por cantidad de archivos
grupos_ordenados = sorted(grupos_por_tipo.items(), key=lambda x: len(x[1]), reverse=True)

print(f"TIPOS DE REPORTE DETECTADOS: {len(grupos_por_tipo)}\n")

for idx, (tipo, archivos_grupo) in enumerate(grupos_ordenados, 1):
    print(f"\n{'─'*80}")
    print(f"TIPO {idx}: {tipo} ({len(archivos_grupo)} archivos)")
    print(f"{'─'*80}")
    
    # Mostrar columnas núcleo comunes
    todas_columnas = set()
    for v in archivos_grupo:
        todas_columnas.update(v['columnas_nucleo'])
    print(f"Columnas núcleo: {sorted(todas_columnas)}")
    
    # Mostrar ejemplo de columnas originales
    ejemplo = archivos_grupo[0]
    print(f"Ejemplo ({ejemplo['archivo']}, fila {ejemplo['fila_header']}):")
    print(f"  Original: {ejemplo['columnas'][:8]}{'...' if len(ejemplo['columnas']) > 8 else ''}")
    print(f"  Semántico: {ejemplo['columnas_semanticas'][:8]}{'...' if len(ejemplo['columnas_semanticas']) > 8 else ''}")
    
    print(f"\nArchivos:")
    for v in archivos_grupo[:15]:  # Limitar a 15 para no saturar
        tipo_marca = '[M]' if v['tipo'] == 'multilinea' else '[S]'
        print(f"  • {tipo_marca} {v['archivo']} (fila {v['fila_header']})")
    if len(archivos_grupo) > 15:
        print(f"  ... y {len(archivos_grupo) - 15} más")

# ============================================================================
# RESUMEN FINAL
# ============================================================================

print(f"\n\n{'='*80}")
print("RESUMEN FINAL")
print(f"{'='*80}")
print(f"Total de archivos analizados: {len(archivos)}")
print(f"Archivos con variante seleccionada: {len(mejores_variantes)}")
print(f"TIPOS DE REPORTE: {len(grupos_por_tipo)}")

print(f"\nDistribución por tipo:")
for tipo, archivos_grupo in grupos_ordenados:
    pct = len(archivos_grupo) / len(mejores_variantes) * 100
    print(f"  • {tipo}: {len(archivos_grupo)} archivos ({pct:.1f}%)")

# Archivos no clasificados o problemáticos
archivos_analizados = set(v['archivo'] for v in mejores_variantes)
archivos_faltantes = set(a.name for a in archivos) - archivos_analizados
if archivos_faltantes:
    print(f"\n⚠️  Archivos sin clasificar ({len(archivos_faltantes)}):")
    for a in sorted(archivos_faltantes)[:10]:
        print(f"  • {a}")

print(f"\n{'='*80}")
print("TIPOS DE REPORTE DISPONIBLES:")
print("─"*40)
print("• CAUSAS_SIMPLE      - Top 10 causas básico")
print("• CAUSAS_CODIGO      - Top 10 con código diagnóstico")
print("• CAUSAS_SUBREGION   - Causas por subregión")
print("• CAUSAS_MUNICIPIO   - Causas por municipio")
print("• CAUSAS_DEPARTAMENTO- Total departamento")
print("• AGRUPACION22_DEPTO - 22 grupos por edad (depto)")
print("• AGRUPACION22_MUNICIPIO - 22 grupos por municipio")
print("• MORBILIDAD_FORMATO - Formato especial morbilidad")
print(f"{'='*80}")