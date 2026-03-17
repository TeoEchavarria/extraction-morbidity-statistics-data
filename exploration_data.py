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
        # Seleccionar engine según extensión
        engine = 'xlrd' if str(archivo_path).endswith('.xls') else 'openpyxl'
        df_raw = pd.read_excel(archivo_path, sheet_name=hoja, header=None, engine=engine)
        
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
    except Exception as e:
        return None

def obtener_engine(archivo_path):
    """Retorna el engine correcto según la extensión del archivo"""
    return 'xlrd' if str(archivo_path).endswith('.xls') else 'openpyxl'

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
# CLASIFICACIÓN POR TIPO DE REPORTE - UNIFICADA
# ============================================================================

# Esquema base unificado para CAUSAS
# Campos obligatorios: diagnostico, total
# Campos opcionales: codigo_diagnostico, porcentaje, zona_*, sexo_*, subregion, municipio, codigo_municipio

ESQUEMA_CAUSAS = {
    'obligatorios': {'diagnostico', 'total'},
    'opcionales': {
        'codigo_diagnostico', 'porcentaje', 
        'zona_urbana', 'zona_rural',
        'sexo_masculino', 'sexo_femenino', 'sexo_no_definido',
        'subregion', 'municipio', 'codigo_municipio'
    }
}

def clasificar_tipo_reporte(nombre_archivo, columnas_nucleo, tiene_grupos_edad):
    """Clasifica el archivo en un tipo de reporte simplificado"""
    nombre = nombre_archivo.lower()
    
    # TIPO 1: AGRUPACION22 - tiene grupos de edad numéricos (0-21)
    if tiene_grupos_edad:
        return 'AGRUPACION22'
    
    # TIPO 2: CAUSAS - tiene diagnostico y/o total (con variantes de ubicación)
    tiene_diagnostico = 'diagnostico' in columnas_nucleo
    tiene_total = 'total' in columnas_nucleo
    tiene_codigo_dx = 'codigo_diagnostico' in columnas_nucleo
    
    if tiene_diagnostico or tiene_codigo_dx or tiene_total:
        return 'CAUSAS'
    
    # TIPO 3: OTRO - no encaja en ninguna categoría
    return 'OTRO'

def obtener_nivel_geografico(columnas_nucleo):
    """Determina el nivel geográfico de los datos"""
    tiene_municipio = 'municipio' in columnas_nucleo or 'codigo_municipio' in columnas_nucleo
    tiene_subregion = 'subregion' in columnas_nucleo
    
    if tiene_municipio:
        return 'MUNICIPIO'
    elif tiene_subregion:
        return 'SUBREGION'
    else:
        return 'DEPARTAMENTO'

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
        # Seleccionar engine según extensión
        engine = obtener_engine(archivo_path)
        
        xl_file = pd.ExcelFile(archivo_path, engine=engine)
        hoja = xl_file.sheet_names[0]
        for h in xl_file.sheet_names:
            if 'datos' in h.lower():
                hoja = h
                break
        
        # Probar las primeras 10 filas como headers (simple)
        for fila_header in range(10):
            try:
                df = pd.read_excel(archivo_path, sheet_name=hoja, header=fila_header, nrows=5, engine=engine)
                columnas = [str(col).strip() for col in df.columns]
                columnas_semanticas = [normalizar_semantico(c) for c in columnas]
                
                if es_grupo_valido(columnas):
                    columnas_nucleo, tiene_grupos_edad = obtener_columnas_nucleo(columnas_semanticas)
                    tipo_reporte = clasificar_tipo_reporte(archivo, columnas_nucleo, tiene_grupos_edad)
                    nivel_geo = obtener_nivel_geografico(columnas_nucleo)
                    
                    resultados_exploracion.append({
                        'archivo': archivo,
                        'fila_header': fila_header,
                        'columnas': columnas,
                        'columnas_semanticas': columnas_semanticas,
                        'columnas_nucleo': columnas_nucleo,
                        'tiene_grupos_edad': tiene_grupos_edad,
                        'tipo_reporte': tipo_reporte,
                        'nivel_geografico': nivel_geo,
                        'num_columnas': len(columnas),
                        'tipo': 'simple'
                    })
            except Exception as e:
                pass
        
        # Probar headers multilineales
        for fila_inicio in range(9):
            headers_fusionados = fusionar_headers_multilinea(archivo_path, hoja, fila_inicio, 2)
            if headers_fusionados and es_grupo_valido(headers_fusionados):
                columnas_semanticas = [normalizar_semantico(c) for c in headers_fusionados]
                columnas_nucleo, tiene_grupos_edad = obtener_columnas_nucleo(columnas_semanticas)
                tipo_reporte = clasificar_tipo_reporte(archivo, columnas_nucleo, tiene_grupos_edad)
                nivel_geo = obtener_nivel_geografico(columnas_nucleo)
                
                resultados_exploracion.append({
                    'archivo': archivo,
                    'fila_header': f'{fila_inicio}-{fila_inicio+1}',
                    'columnas': headers_fusionados,
                    'columnas_semanticas': columnas_semanticas,
                    'columnas_nucleo': columnas_nucleo,
                    'tiene_grupos_edad': tiene_grupos_edad,
                    'tipo_reporte': tipo_reporte,
                    'nivel_geografico': nivel_geo,
                    'num_columnas': len(headers_fusionados),
                    'tipo': 'multilinea'
                })
    except Exception as e:
        print(f"  ⚠️ Error procesando {archivo}: {type(e).__name__}")

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
print("AGRUPACIÓN POR TIPO DE REPORTE (UNIFICADO)")
print(f"{'='*80}\n")

# Agrupar por tipo
grupos_por_tipo = defaultdict(list)
for v in mejores_variantes:
    grupos_por_tipo[v['tipo_reporte']].append(v)

# Ordenar por cantidad de archivos
grupos_ordenados = sorted(grupos_por_tipo.items(), key=lambda x: len(x[1]), reverse=True)

print(f"TIPOS DE REPORTE DETECTADOS: {len(grupos_por_tipo)}\n")

for idx, (tipo, archivos_grupo) in enumerate(grupos_ordenados, 1):
    print(f"\n{'='*80}")
    print(f"TIPO {idx}: {tipo} ({len(archivos_grupo)} archivos)")
    print(f"{'='*80}")
    
    # Mostrar columnas núcleo comunes (unión de todas)
    todas_columnas = set()
    for v in archivos_grupo:
        todas_columnas.update(v['columnas_nucleo'])
    print(f"\nColumnas núcleo (todas las variantes): {sorted(todas_columnas)}")
    
    # Agrupar por nivel geográfico dentro del tipo
    por_nivel_geo = defaultdict(list)
    for v in archivos_grupo:
        por_nivel_geo[v['nivel_geografico']].append(v)
    
    for nivel_geo, archivos_nivel in sorted(por_nivel_geo.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"\n  {'─'*70}")
        print(f"  Nivel: {nivel_geo} ({len(archivos_nivel)} archivos)")
        print(f"  {'─'*70}")
        
        # Mostrar columnas específicas de este nivel
        cols_nivel = set()
        for v in archivos_nivel:
            cols_nivel.update(v['columnas_nucleo'])
        print(f"  Columnas: {sorted(cols_nivel)}")
        
        # Mostrar ejemplo
        ejemplo = archivos_nivel[0]
        print(f"  Ejemplo ({ejemplo['archivo']}):")
        print(f"    Fila header: {ejemplo['fila_header']}")
        print(f"    Original: {ejemplo['columnas'][:8]}{'...' if len(ejemplo['columnas']) > 8 else ''}")
        
        # Listar archivos
        print(f"  Archivos:")
        for v in archivos_nivel[:10]:
            tipo_marca = '[M]' if v['tipo'] == 'multilinea' else '[S]'
            print(f"    • {tipo_marca} {v['archivo']} (fila {v['fila_header']})")
        if len(archivos_nivel) > 10:
            print(f"    ... y {len(archivos_nivel) - 10} más")

# ============================================================================
# RESUMEN FINAL
# ============================================================================

print(f"\n\n{'='*80}")
print("RESUMEN FINAL")
print(f"{'='*80}")
print(f"Total de archivos analizados: {len(archivos)}")
print(f"Archivos con variante seleccionada: {len(mejores_variantes)}")
print(f"TIPOS DE REPORTE PRINCIPALES: {len(grupos_por_tipo)}")

print("\nDistribución por tipo:")
for tipo, archivos_grupo in grupos_ordenados:
    pct = len(archivos_grupo) / len(mejores_variantes) * 100
    
    # Contar por nivel geográfico
    por_nivel = defaultdict(int)
    for v in archivos_grupo:
        por_nivel[v['nivel_geografico']] += 1
    
    niveles_str = ', '.join(f"{k}:{v}" for k, v in sorted(por_nivel.items()))
    print(f"  • {tipo}: {len(archivos_grupo)} archivos ({pct:.1f}%) - [{niveles_str}]")

# Archivos no clasificados o problemáticos
archivos_analizados = {v['archivo'] for v in mejores_variantes}
archivos_faltantes = {a.name for a in archivos} - archivos_analizados
if archivos_faltantes:
    print(f"\n⚠️  Archivos sin clasificar ({len(archivos_faltantes)}):")
    for a in sorted(archivos_faltantes)[:10]:
        print(f"  • {a}")
    if len(archivos_faltantes) > 10:
        print(f"  ... y {len(archivos_faltantes) - 10} más")

print(f"\n{'='*80}")
print("ESQUEMA UNIFICADO DE COLUMNAS:")
print("─"*40)
print("OBLIGATORIAS:")
print("  • diagnostico    - Nombre/descripción de la causa")
print("  • total          - Total de casos")
print("\nOPCIONALES:")
print("  • codigo_diagnostico - Código de la causa (ej: CIE-10)")
print("  • porcentaje     - Porcentaje del total")
print("  • zona_urbana    - Casos en zona urbana/cabecera")
print("  • zona_rural     - Casos en zona rural/resto")
print("  • sexo_masculino - Casos hombres")
print("  • sexo_femenino  - Casos mujeres")
print("  • sexo_no_definido - Casos sin sexo definido")
print("  • subregion      - Nombre de la subregión")
print("  • municipio      - Nombre del municipio")
print("  • codigo_municipio - Código DANE del municipio")
print(f"\n{'='*80}")
print("TIPOS DE REPORTE:")
print("─"*40)
print("• CAUSAS       - Top causas de morbilidad (consulta/urgencias/hospitalización)")
print("• AGRUPACION22 - Agrupación por 22 grupos de edad (0-21)")
print("• OTRO         - Formatos no clasificados")
print(f"{'='*80}")