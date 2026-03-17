"""
UNIFICADOR DE DATOS DE MORBILIDAD
Consolida todos los archivos Excel en un único dataframe estandarizado
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURACIÓN DE COLUMNAS
# ============================================================================

COLUMNAS_CAUSAS = [
    'año', 'tipo_servicio', 'nivel_geografico', 'departamento',
    'subregion', 'codigo_municipio', 'municipio',
    'codigo_diagnostico', 'diagnostico', 'total', 'porcentaje',
    'zona_urbana', 'zona_rural', 'sexo_masculino', 'sexo_femenino',
    'sexo_no_definido', 'archivo_fuente'
]

# ============================================================================
# MAPEO SEMÁNTICO
# ============================================================================

SINONIMOS_COLUMNAS = {
    'codigo_diagnostico': [
        'código causa', 'cod_dx', 'codigo dx', 'codigo causa', 'cod causa', 
        'nro', 'código de causa', 'cod dx', 'codigo_causa', 'código dx'
    ],
    'codigo_municipio': [
        'código dane del municipio', 'cod_municipio', 'codigo municipio',
        'código de municipio', 'codigo dane', 'cod municipio', 'cod_mpio',
        'código de mpio', 'codigo_mpio'
    ],
    'diagnostico': [
        'causas', 'causa', 'diagnóstico', 'diagnostico', 'descripcion', 
        'descripción', 'dx', 'enfermedad', 'descripcion dx', 'descripción dx',
        'descripción del diagnóstico', 'descripción diagnóstico'
    ],
    'total': ['total', 'total general', 'n°', 'n', 'numero', 'cantidad', 'casos'],
    'porcentaje': ['%', 'distribución %', 'porcentaje', 'distribucion %', 'distribución', 'pct'],
    'zona_urbana': ['cabecera', 'urbana', 'urbano', 'zona_urbana', 'zona urbana'],
    'zona_rural': ['resto', 'rural', 'zona_rural', 'zona rural'],
    'sexo_masculino': [
        'hombre', 'masculino', 'hombres', 'sexo_hombre', 'sexo hombre',
        'masc', 'sexo_masculino'
    ],
    'sexo_femenino': [
        'mujer', 'femenino', 'mujeres', 'sexo_mujer', 'sexo mujer',
        'fem', 'sexo_femenino'
    ],
    'sexo_no_definido': [
        'no definido / no reportado', 'no definido/ no reportado', 
        'no definido no reportado', 'no reportado', 'no definido', 
        'indeterminado', 'sin definir', 'nd'
    ],
    'subregion': [
        'subregiones', 'subregion', 'subregión', 'nom_regional', 'region', 
        'regional', 'subregiones y municipios', 'nombre regional', 'región'
    ],
    'municipio': [
        'municipios y distritos', 'municipio', 'municipios', 'nom_mpio', 
        'nom_municipio', 'nombre municipio', 'mpio', 'distrito', 'nombre_municipio'
    ],
}

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def obtener_engine(archivo_path):
    """Retorna el engine correcto según la extensión del archivo"""
    return 'xlrd' if str(archivo_path).endswith('.xls') else 'openpyxl'

def extraer_año(nombre_archivo):
    """Extrae el año del nombre del archivo (número de 4 dígitos que empieza con 20)"""
    matches = re.findall(r'20[0-2][0-9]', nombre_archivo)
    return int(matches[-1]) if matches else None

def extraer_tipo_servicio(nombre_archivo):
    """Extrae el tipo de servicio del nombre del archivo"""
    nombre = nombre_archivo.lower()
    
    if any(x in nombre for x in ['consulta', 'cons', 'consultas']):
        return 'consulta'
    elif any(x in nombre for x in ['urgencia', 'urgen', 'urgencias']):
        return 'urgencias'
    elif any(x in nombre for x in ['hospital', 'hospi', 'egreso', 'hospitaliza']):
        return 'hospitalizacion'
    
    return 'no_especificado'

def normalizar_nombre_columna(col):
    """Normaliza nombres de columnas"""
    col = str(col).strip().lower()
    col = col.replace('unnamed: ', 'unnamed_')
    col = col.replace('\n', ' ').replace('\r', '')
    col = ' '.join(col.split())
    return col

def mapear_columna(col_original):
    """Mapea una columna original a su nombre canónico"""
    col_norm = normalizar_nombre_columna(col_original)
    
    if 'unnamed' in col_norm:
        return None
    
    # Intentar convertir a número (grupo de edad)
    try:
        num = float(col_norm)
        if num.is_integer() and 0 <= int(num) <= 21:
            return f'grupo_edad_{int(num)}'
        return None
    except:
        pass
    
    # Buscar en sinónimos
    for nombre_canonico, sinonimos in SINONIMOS_COLUMNAS.items():
        for sinonimo in sinonimos:
            if col_norm == sinonimo or (len(sinonimo) > 4 and sinonimo in col_norm):
                return nombre_canonico
    
    return None

def fusionar_headers_multilinea(archivo_path, hoja, fila_inicio, num_filas=2):
    """Fusiona headers que están en múltiples filas"""
    try:
        engine = obtener_engine(archivo_path)
        df_raw = pd.read_excel(archivo_path, sheet_name=hoja, header=None, engine=engine)
        
        if fila_inicio + num_filas > len(df_raw):
            return None, None
        
        filas = [df_raw.iloc[fila_inicio + i] for i in range(num_filas)]
        
        headers_fusionados = []
        for col_idx in range(len(filas[0])):
            partes = []
            for fila in filas:
                valor = str(fila.iloc[col_idx]).strip()
                if valor and valor.lower() not in ['nan', 'unnamed', '']:
                    partes.append(valor)
            
            if partes:
                headers_fusionados.append('_'.join(partes))
            else:
                headers_fusionados.append(f'Unnamed_{col_idx}')
        
        fila_datos = fila_inicio + num_filas
        return headers_fusionados, fila_datos
    except:
        return None, None

def detectar_tipo_reporte(columnas_mapeadas):
    """Detecta si es CAUSAS o AGRUPACION22"""
    tiene_grupos_edad = any('grupo_edad_' in str(c) for c in columnas_mapeadas.values() if c)
    tiene_diagnostico = 'diagnostico' in columnas_mapeadas.values()
    
    if tiene_grupos_edad:
        return 'AGRUPACION22'
    elif tiene_diagnostico:
        return 'CAUSAS'
    return 'OTRO'

def detectar_nivel_geografico(columnas_mapeadas):
    """Detecta el nivel geográfico"""
    valores = set(columnas_mapeadas.values())
    
    if 'municipio' in valores or 'codigo_municipio' in valores:
        return 'municipio'
    elif 'subregion' in valores:
        return 'subregion'
    return 'departamento'

# ============================================================================
# PROCESADOR PRINCIPAL
# ============================================================================

class UnificadorMorbilidad:
    """Procesa y unifica todos los archivos de morbilidad"""
    
    def __init__(self, carpeta='excels'):
        self.carpeta = Path(carpeta)
        self.archivos = sorted(
            list(self.carpeta.glob('*.xlsx')) + 
            list(self.carpeta.glob('*.xls'))
        )
        self.resultados_causas = []
        self.resultados_agrupacion22 = []
        self.errores = []
    
    def procesar_todos(self):
        """Procesa todos los archivos"""
        print(f"\n{'='*80}")
        print(f"UNIFICACIÓN DE DATOS DE MORBILIDAD")
        print(f"{'='*80}")
        print(f"Archivos a procesar: {len(self.archivos)}")
        
        for archivo_path in self.archivos:
            self._procesar_archivo(archivo_path)
        
        return self._consolidar()
    
    def _procesar_archivo(self, archivo_path):
        """Procesa un archivo individual"""
        nombre = archivo_path.name
        
        try:
            engine = obtener_engine(archivo_path)
            xl_file = pd.ExcelFile(archivo_path, engine=engine)
            
            # Obtener hoja principal
            hoja = xl_file.sheet_names[0]
            for h in xl_file.sheet_names:
                if 'datos' in h.lower():
                    hoja = h
                    break
            
            # Encontrar mejor configuración de headers
            mejor_config = self._encontrar_mejor_config(archivo_path, hoja, engine)
            
            if mejor_config is None:
                self.errores.append({'archivo': nombre, 'error': 'No se encontró configuración válida'})
                print(f"  ⚠️ {nombre}: Sin configuración válida")
                return
            
            # Cargar datos con la configuración encontrada
            df = self._cargar_datos(archivo_path, hoja, engine, mejor_config)
            
            if df is None or len(df) == 0:
                self.errores.append({'archivo': nombre, 'error': 'DataFrame vacío'})
                return
            
            # Extraer metadatos
            año = extraer_año(nombre)
            tipo_servicio = extraer_tipo_servicio(nombre)
            tipo_reporte = mejor_config['tipo_reporte']
            nivel_geo = mejor_config['nivel_geografico']
            
            # Estandarizar y agregar al resultado
            df_estandarizado = self._estandarizar_df(
                df, 
                mejor_config['mapeo'],
                tipo_reporte,
                año=año,
                tipo_servicio=tipo_servicio,
                nivel_geo=nivel_geo,
                archivo_fuente=nombre
            )
            
            if tipo_reporte == 'CAUSAS':
                self.resultados_causas.append(df_estandarizado)
            elif tipo_reporte == 'AGRUPACION22':
                self.resultados_agrupacion22.append(df_estandarizado)
            
            print(f"  ✓ {nombre}: {len(df_estandarizado)} filas ({tipo_reporte}, {nivel_geo}, {año})")
            
        except Exception as e:
            self.errores.append({'archivo': nombre, 'error': str(e)})
            print(f"  ✗ {nombre}: {type(e).__name__} - {str(e)[:50]}")
    
    def _encontrar_mejor_config(self, archivo_path, hoja, engine):
        """Encuentra la mejor configuración de headers para un archivo"""
        mejor_config = None
        mejor_score = 0
        
        # Probar headers simples (filas 0-9)
        for fila_header in range(10):
            try:
                df = pd.read_excel(archivo_path, sheet_name=hoja, header=fila_header, nrows=5, engine=engine)
                columnas = list(df.columns)
                
                mapeo = {col: mapear_columna(col) for col in columnas}
                columnas_validas = [v for v in mapeo.values() if v is not None]
                score = len(columnas_validas)
                
                if score > mejor_score:
                    tipo_reporte = detectar_tipo_reporte(mapeo)
                    nivel_geo = detectar_nivel_geografico(mapeo)
                    
                    mejor_score = score
                    mejor_config = {
                        'tipo': 'simple',
                        'fila_header': fila_header,
                        'fila_datos': fila_header + 1,
                        'mapeo': mapeo,
                        'score': score,
                        'tipo_reporte': tipo_reporte,
                        'nivel_geografico': nivel_geo
                    }
            except:
                pass
        
        # Probar headers multilineales
        for fila_inicio in range(9):
            headers, fila_datos = fusionar_headers_multilinea(archivo_path, hoja, fila_inicio, 2)
            if headers:
                mapeo = {col: mapear_columna(col) for col in headers}
                columnas_validas = [v for v in mapeo.values() if v is not None]
                score = len(columnas_validas) + 0.5  # Bonus pequeño para multilinea si empata
                
                if score > mejor_score:
                    tipo_reporte = detectar_tipo_reporte(mapeo)
                    nivel_geo = detectar_nivel_geografico(mapeo)
                    
                    mejor_score = score
                    mejor_config = {
                        'tipo': 'multilinea',
                        'fila_header': f'{fila_inicio}-{fila_inicio+1}',
                        'fila_datos': fila_datos,
                        'headers_fusionados': headers,
                        'mapeo': mapeo,
                        'score': score,
                        'tipo_reporte': tipo_reporte,
                        'nivel_geografico': nivel_geo
                    }
        
        return mejor_config if mejor_score >= 3 else None
    
    def _cargar_datos(self, archivo_path, hoja, engine, config):
        """Carga los datos según la configuración"""
        try:
            if config['tipo'] == 'simple':
                df = pd.read_excel(
                    archivo_path, 
                    sheet_name=hoja, 
                    header=config['fila_header'],
                    engine=engine
                )
            else:
                # Multilinea - cargar sin header y asignar
                df_raw = pd.read_excel(
                    archivo_path, 
                    sheet_name=hoja, 
                    header=None,
                    engine=engine
                )
                df = df_raw.iloc[config['fila_datos']:].copy()
                df.columns = config['headers_fusionados']
                df = df.reset_index(drop=True)
            
            # Limpiar filas vacías
            df = df.dropna(how='all')
            
            return df
        except Exception as e:
            return None
    
    def _estandarizar_df(self, df, mapeo, tipo_reporte, año, tipo_servicio, nivel_geo, archivo_fuente):
        """Estandariza el dataframe según el tipo de reporte"""
        
        # Crear dataframe con columnas renombradas
        columnas_renombradas = {}
        for col_original, col_canonico in mapeo.items():
            if col_canonico:
                columnas_renombradas[col_original] = col_canonico
        
        df_renamed = df.rename(columns=columnas_renombradas)
        
        if tipo_reporte == 'CAUSAS':
            return self._estandarizar_causas(df_renamed, año, tipo_servicio, nivel_geo, archivo_fuente)
        elif tipo_reporte == 'AGRUPACION22':
            return self._estandarizar_agrupacion22(df_renamed, año, tipo_servicio, nivel_geo, archivo_fuente)
        else:
            return pd.DataFrame()
    
    def _estandarizar_causas(self, df, año, tipo_servicio, nivel_geo, archivo_fuente):
        """Estandariza dataframe tipo CAUSAS"""
        # Resetear índice para asegurar alineación
        df = df.reset_index(drop=True)
        n_filas = len(df)
        
        if n_filas == 0:
            return pd.DataFrame()
        
        # Crear listas para cada columna
        data = {
            'año': [año] * n_filas,
            'tipo_servicio': [tipo_servicio] * n_filas,
            'nivel_geografico': [nivel_geo] * n_filas,
            'departamento': ['Antioquia'] * n_filas,
            'subregion': list(df['subregion']) if 'subregion' in df.columns else [None] * n_filas,
            'codigo_municipio': list(df['codigo_municipio'].astype(str)) if 'codigo_municipio' in df.columns else [None] * n_filas,
            'municipio': list(df['municipio']) if 'municipio' in df.columns else [None] * n_filas,
            'codigo_diagnostico': list(df['codigo_diagnostico'].astype(str)) if 'codigo_diagnostico' in df.columns else [None] * n_filas,
            'diagnostico': list(df['diagnostico']) if 'diagnostico' in df.columns else [None] * n_filas,
            'archivo_fuente': [archivo_fuente] * n_filas,
        }
        
        # Columnas numéricas
        for col in ['total', 'porcentaje', 'zona_urbana', 'zona_rural', 
                    'sexo_masculino', 'sexo_femenino', 'sexo_no_definido']:
            if col in df.columns:
                data[col] = list(pd.to_numeric(df[col], errors='coerce'))
            else:
                data[col] = [None] * n_filas
        
        df_final = pd.DataFrame(data)
        
        # Filtrar filas sin diagnóstico Y sin total (ambos vacíos)
        df_final = df_final[
            df_final['diagnostico'].notna() | df_final['total'].notna()
        ]
        
        # Limpiar valores de texto en diagnóstico
        if len(df_final) > 0 and df_final['diagnostico'].notna().any():
            df_final['diagnostico'] = df_final['diagnostico'].astype(str).str.strip().str.upper()
            # Filtrar headers de edad que no son diagnósticos reales
            df_final = df_final[~df_final['diagnostico'].str.match(r'^\d+\s*(A|AL?)\s*\d+\s*AÑOS?$', na=False)]
        
        return df_final
    
    def _estandarizar_agrupacion22(self, df, año, tipo_servicio, nivel_geo, archivo_fuente):
        """Estandariza dataframe tipo AGRUPACION22"""
        df = df.reset_index(drop=True)
        n_filas = len(df)
        
        if n_filas == 0:
            return pd.DataFrame()
        
        data = {
            'año': [año] * n_filas,
            'tipo_servicio': [tipo_servicio] * n_filas,
            'departamento': ['Antioquia'] * n_filas,
            'subregion': list(df['subregion']) if 'subregion' in df.columns else [None] * n_filas,
            'codigo_municipio': list(df['codigo_municipio'].astype(str)) if 'codigo_municipio' in df.columns else [None] * n_filas,
            'municipio': list(df['municipio']) if 'municipio' in df.columns else [None] * n_filas,
            'total': list(pd.to_numeric(df['total'], errors='coerce')) if 'total' in df.columns else [None] * n_filas,
            'archivo_fuente': [archivo_fuente] * n_filas,
        }
        
        # Grupos de edad (0-21)
        for i in range(22):
            col_edad = f'grupo_edad_{i}'
            if col_edad in df.columns:
                data[col_edad] = list(pd.to_numeric(df[col_edad], errors='coerce'))
            else:
                data[col_edad] = [None] * n_filas
        
        df_final = pd.DataFrame(data)
        
        # Filtrar filas sin total
        df_final = df_final[df_final['total'].notna()]
        
        return df_final
    
    def _consolidar(self):
        """Consolida todos los resultados"""
        print(f"\n{'='*80}")
        print("CONSOLIDACIÓN")
        print(f"{'='*80}")
        
        df_causas = None
        df_agrupacion22 = None
        
        # Consolidar CAUSAS
        if self.resultados_causas:
            df_causas = pd.concat(self.resultados_causas, ignore_index=True)
            # Convertir año a int donde sea posible
            df_causas['año'] = pd.to_numeric(df_causas['año'], errors='coerce').astype('Int64')
            años_validos = df_causas['año'].dropna().unique().tolist()
            print(f"\n✓ CAUSAS consolidado:")
            print(f"  Filas totales: {len(df_causas):,}")
            print(f"  Archivos: {df_causas['archivo_fuente'].nunique()}")
            print(f"  Años: {sorted([int(a) for a in años_validos])}")
            print(f"  Columnas: {list(df_causas.columns)}")
        
        # Consolidar AGRUPACION22
        if self.resultados_agrupacion22:
            # Filtrar dataframes vacíos
            dfs_validos = [df for df in self.resultados_agrupacion22 if len(df) > 0]
            if dfs_validos:
                df_agrupacion22 = pd.concat(dfs_validos, ignore_index=True)
                df_agrupacion22['año'] = pd.to_numeric(df_agrupacion22['año'], errors='coerce').astype('Int64')
                años_validos = df_agrupacion22['año'].dropna().unique().tolist()
                print(f"\n✓ AGRUPACION22 consolidado:")
                print(f"  Filas totales: {len(df_agrupacion22):,}")
                print(f"  Archivos: {df_agrupacion22['archivo_fuente'].nunique()}")
                print(f"  Años: {sorted([int(a) for a in años_validos])}")
        
        # Resumen de errores
        if self.errores:
            print(f"\n⚠️ Errores ({len(self.errores)}):")
            for err in self.errores[:10]:
                print(f"  • {err['archivo']}: {err['error'][:50]}")
        
        return df_causas, df_agrupacion22


# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

if __name__ == '__main__':
    
    # Procesar y unificar
    unificador = UnificadorMorbilidad('excels')
    df_causas, df_agrupacion22 = unificador.procesar_todos()
    
    # Guardar resultados
    print(f"\n{'='*80}")
    print("GUARDANDO ARCHIVOS")
    print(f"{'='*80}")
    
    if df_causas is not None and len(df_causas) > 0:
        # CSV
        df_causas.to_csv('morbilidad_causas_unificado.csv', index=False, encoding='utf-8-sig')
        print(f"\n✓ Guardado: morbilidad_causas_unificado.csv ({len(df_causas):,} filas)")
        
        # Excel (con manejo explícito)
        try:
            with pd.ExcelWriter('morbilidad_causas_unificado.xlsx', engine='openpyxl') as writer:
                df_causas.to_excel(writer, sheet_name='Datos', index=False)
            print(f"✓ Guardado: morbilidad_causas_unificado.xlsx")
        except Exception as e:
            print(f"⚠️ Error guardando Excel: {e}")
        
        # Mostrar resumen
        print(f"\n{'─'*40}")
        print("RESUMEN CAUSAS:")
        print(f"{'─'*40}")
        print(f"Por año:")
        print(df_causas.groupby('año').size().sort_index())
        print(f"\nPor tipo de servicio:")
        print(df_causas.groupby('tipo_servicio').size())
        print(f"\nPor nivel geográfico:")
        print(df_causas.groupby('nivel_geografico').size())
    
    if df_agrupacion22 is not None and len(df_agrupacion22) > 0:
        df_agrupacion22.to_csv('morbilidad_agrupacion22_unificado.csv', index=False, encoding='utf-8-sig')
        print(f"\n✓ Guardado: morbilidad_agrupacion22_unificado.csv ({len(df_agrupacion22):,} filas)")
        
        try:
            with pd.ExcelWriter('morbilidad_agrupacion22_unificado.xlsx', engine='openpyxl') as writer:
                df_agrupacion22.to_excel(writer, sheet_name='Datos', index=False)
            print(f"✓ Guardado: morbilidad_agrupacion22_unificado.xlsx")
        except Exception as e:
            print(f"⚠️ Error guardando Excel: {e}")
    
    print(f"\n{'='*80}")
    print("✓ PROCESO COMPLETADO")
    print(f"{'='*80}")
