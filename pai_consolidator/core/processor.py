"""
Módulo para procesar y consolidar archivos PAI de vacunación.
"""
import os
import glob
import traceback
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np

from pai_consolidator.core.utils import analizar_estructura_excel
from .utils import (
    extraer_nombre_municipio,
    extraer_info_ruta,
    listar_archivos_pai,
    encontrar_columnas_vacuna,
    identificar_columna_dosis,
    clasificar_grupo_etario,
    limpiar_texto,
    extraer_vereda_de_direccion,
    debe_excluir_archivo
)

class PaiProcessor:
    """
    Clase para procesar archivos PAI de vacunación.
    """
    
    def __init__(self, vacuna: str = "Fiebre amarilla", tipo_consolidado: str = "vacunacion"):
        """
        Inicializa el procesador de archivos PAI.
        
        Args:
            vacuna: Nombre de la vacuna a procesar (default: "Fiebre amarilla").
            tipo_consolidado: Tipo de consolidado a generar ("residencia", "vacunacion" o "ambos").
        """
        self.vacuna = vacuna
        self.tipo_consolidado = tipo_consolidado
        self.archivos_procesados = 0
        self.registros_totales = 0
        self.advertencias = []
    
    def procesar_archivo(self, ruta_archivo: str) -> pd.DataFrame:
        """
        Procesa un archivo PAI y extrae datos de la vacuna especificada.
        Optimizado para manejar estructuras jerárquicas de encabezados.
        
        Args:
            ruta_archivo: Ruta al archivo XLSM/XLSX.
            
        Returns:
            DataFrame con los datos procesados de la vacuna.
        """
        try:
            # Extraer información de la ruta y nombre del archivo
            info_ruta = extraer_info_ruta(ruta_archivo)
            municipio = info_ruta["municipio"]
            
            print(f"Procesando archivo: {os.path.basename(ruta_archivo)}")
            print(f"  - Municipio identificado: {municipio}")
            print(f"  - Año: {info_ruta['año'] or 'No identificado'}")
            print(f"  - Mes: {info_ruta['mes'] or 'No identificado'}")
            
            # Determinar el engine correcto según la extensión
            ext = os.path.splitext(ruta_archivo)[1].lower()
            if ext == '.xlsx' or ext == '.xlsm':
                engine = 'openpyxl'
            elif ext == '.xls':
                engine = 'xlrd'
            else:
                engine = 'openpyxl'  # Por defecto
            
            print(f"  - Usando engine: {engine} para extensión {ext}")
            
            # Identificar la hoja correcta
            try:
                xlsx = pd.ExcelFile(ruta_archivo, engine=engine)
                if "Registro Diario" in xlsx.sheet_names:
                    sheet_name = "Registro Diario"
                else:
                    # Buscar hojas que contengan "registro" o "diario"
                    hojas_candidatas = [hoja for hoja in xlsx.sheet_names 
                                    if "regist" in hoja.lower() or "diario" in hoja.lower()]
                    if hojas_candidatas:
                        sheet_name = hojas_candidatas[0]
                    else:
                        # Usar la primera hoja si no encontramos candidatas
                        sheet_name = xlsx.sheet_names[0]
                
                print(f"  - Usando hoja: {sheet_name}")
            except Exception as e:
                print(f"  - Error al verificar hojas: {e}")
                sheet_name = "Registro Diario"  # Valor por defecto
            
            # Analizar estructura del Excel para detectar encabezados jerárquicos
            print(f"Analizando estructura de {os.path.basename(ruta_archivo)}...")
            estructura = analizar_estructura_excel(ruta_archivo)
            
            # Mostrar información de la estructura detectada
            if estructura["contiene_fiebre_amarilla"]:
                print(f"  - ¡'Fiebre amarilla' encontrada en fila {estructura['fila_fiebre_amarilla'] + 1}, "
                    f"columna {estructura['columna_fiebre_amarilla'] + 1}!")
                if estructura["columnas_fiebre_amarilla"]:
                    subcategorias = [col['subcategoria'] for col in estructura['columnas_fiebre_amarilla']]
                    print(f"  - Subcategorías detectadas: {subcategorias}")
            else:
                print("  - No se detectó 'Fiebre amarilla' en el análisis de estructura")
            
            # ESTRATEGIA 1: Cargar con encabezados jerárquicos si detectamos la estructura
            if estructura["contiene_fiebre_amarilla"] and estructura["fila_fiebre_amarilla"] == 0:
                try:
                    print("  - Intentando cargar con encabezados jerárquicos [0,1]...")
                    df = pd.read_excel(
                        ruta_archivo,
                        sheet_name=sheet_name,
                        engine=engine,
                        header=[0, 1]  # Usar ambas filas como encabezado jerárquico
                    )
                    
                    # Buscar columnas de Fiebre amarilla en el MultiIndex
                    columnas_vacuna = []
                    for col in df.columns:
                        if isinstance(col, tuple) and len(col) >= 2:
                            # Verificar primer nivel (nombre de vacuna)
                            if "fiebre amarilla" in str(col[0]).lower():
                                columnas_vacuna.append(col)
                    
                    if columnas_vacuna:
                        print(f"  - Éxito! Encontradas {len(columnas_vacuna)} columnas de Fiebre amarilla: {columnas_vacuna}")
                        # Identificar columna de dosis
                        col_dosis = None
                        for col in columnas_vacuna:
                            if "dosis" in str(col[1]).lower():
                                col_dosis = col
                                break
                        
                        # Continuar con el procesamiento usando encabezados jerárquicos
                        modo_jerarquico = True
                    else:
                        print("  - No se encontraron columnas jerárquicas de Fiebre amarilla, intentando método alternativo")
                        modo_jerarquico = False
                        # Volvemos a cargar con método tradicional
                        df = pd.read_excel(
                            ruta_archivo,
                            sheet_name=sheet_name,
                            engine=engine,
                            header=1  # Usar solo la segunda fila
                        )
                        columnas_vacuna = []  # Reinicializar para búsqueda alternativa
                except Exception as e:
                    print(f"  - Error al cargar con encabezados jerárquicos: {e}")
                    modo_jerarquico = False
                    # Volvemos a método tradicional
                    df = pd.read_excel(
                        ruta_archivo,
                        sheet_name=sheet_name,
                        engine=engine,
                        header=1
                    )
                    columnas_vacuna = []
            else:
                # ESTRATEGIA 2: Cargar con método tradicional
                print("  - Usando método de carga tradicional (header=1)...")
                modo_jerarquico = False
                try:
                    df = pd.read_excel(
                        ruta_archivo,
                        sheet_name=sheet_name,
                        engine=engine,
                        header=1  # La segunda fila tiene los encabezados detallados
                    )
                    columnas_vacuna = []  # Inicializar para búsqueda posterior
                except Exception as e:
                    print(f"  - Error al cargar el archivo: {e}")
                    print("  - Intentando con parámetros alternativos...")
                    
                    # Intento con diferentes opciones de header
                    for header_pos in [0, 2, None]:
                        try:
                            df = pd.read_excel(
                                ruta_archivo,
                                sheet_name=sheet_name,
                                engine=engine,
                                header=header_pos
                            )
                            print(f"  - Éxito al cargar con header={header_pos}")
                            columnas_vacuna = []
                            break
                        except:
                            continue
                    
                    # Si ninguno funcionó, último intento con la primera hoja
                    if 'df' not in locals():
                        try:
                            df = pd.read_excel(
                                ruta_archivo,
                                sheet_name=0,
                                engine=engine,
                                header=None
                            )
                            print("  - Cargado con parámetros de emergencia (primera hoja, sin header)")
                            columnas_vacuna = []
                        except Exception as e2:
                            print(f"  - Error fatal al cargar el archivo: {e2}")
                            raise ValueError(f"No se pudo cargar el archivo {os.path.basename(ruta_archivo)}")
            
            # Si no tenemos columnas de vacuna aún, buscarlas
            if not columnas_vacuna:
                if estructura["contiene_fiebre_amarilla"] and estructura["columnas_fiebre_amarilla"]:
                    # Usar información de la estructura para encontrar columnas
                    print("  - Usando información de estructura para encontrar columnas...")
                    columnas_candidatas = []
                    for info_col in estructura["columnas_fiebre_amarilla"]:
                        indice = info_col["indice"]
                        subcategoria = info_col["subcategoria"]
                        if indice < len(df.columns):
                            col_name = df.columns[indice]
                            columnas_candidatas.append(col_name)
                            print(f"    * Encontrada columna '{col_name}' para subcategoría '{subcategoria}'")
                    
                    if columnas_candidatas:
                        columnas_vacuna = columnas_candidatas
                        
                        # Identificar posible columna de dosis
                        col_dosis = None
                        for info_col in estructura["columnas_fiebre_amarilla"]:
                            if "dosis" in info_col["subcategoria"].lower():
                                indice = info_col["indice"]
                                if indice < len(df.columns):
                                    col_dosis = df.columns[indice]
                                    break
                
                # Si aún no encontramos columnas, usar método tradicional mejorado
                if not columnas_vacuna:
                    print("  - Buscando columnas de vacuna con método tradicional...")
                    columnas_vacuna = encontrar_columnas_vacuna(df, self.vacuna)
                    col_dosis = None  # Reiniciamos para identificar después
            
            # Si todavía no tenemos columnas, intentar con distintos encabezados
            if not columnas_vacuna:
                print("  - No se encontraron columnas para la vacuna con métodos estándar")
                print("  - Intentando con variaciones de header...")
                
                # Probar diferentes filas como encabezado
                for header_row in range(0, 5):
                    try:
                        df_alt = pd.read_excel(
                            ruta_archivo,
                            sheet_name=sheet_name,
                            engine=engine,
                            header=header_row
                        )
                        
                        print(f"  - Probando con fila {header_row+1} como encabezado")
                        cols_alt = encontrar_columnas_vacuna(df_alt, self.vacuna)
                        
                        if cols_alt:
                            print(f"  - Éxito! Encontradas columnas usando fila {header_row+1} como encabezado")
                            df = df_alt
                            columnas_vacuna = cols_alt
                            modo_jerarquico = False
                            break
                    except Exception as e:
                        print(f"  - Error al intentar con fila {header_row+1}: {e}")
                
                # Si aún no tenemos éxito, buscar directamente en el contenido
                if not columnas_vacuna:
                    try:
                        print("  - Último intento: Buscando en contenido bruto...")
                        # Cargar sin encabezados
                        df_raw = pd.read_excel(
                            ruta_archivo,
                            sheet_name=sheet_name,
                            engine=engine,
                            header=None
                        )
                        
                        # Buscar "Fiebre amarilla" en cualquier celda
                        for i in range(min(20, len(df_raw))):
                            for j in range(min(20, len(df_raw.columns))):
                                try:
                                    valor = df_raw.iloc[i, j]
                                    if isinstance(valor, str) and self.vacuna.lower() in valor.lower():
                                        print(f"  - Encontrado '{self.vacuna}' en celda [{i+1}, {j+1}]: '{valor}'")
                                        
                                        # Intentar usar esta fila como encabezado o la siguiente
                                        for offset in [0, 1]:
                                            try:
                                                df_alt = pd.read_excel(
                                                    ruta_archivo,
                                                    sheet_name=sheet_name,
                                                    engine=engine,
                                                    header=i+offset
                                                )
                                                cols_alt = encontrar_columnas_vacuna(df_alt, self.vacuna)
                                                if cols_alt:
                                                    print(f"  - Éxito con fila {i+offset+1} como encabezado")
                                                    df = df_alt
                                                    columnas_vacuna = cols_alt
                                                    modo_jerarquico = False
                                                    break
                                            except:
                                                pass
                                except:
                                    continue
                                
                                if columnas_vacuna:
                                    break
                            
                            if columnas_vacuna:
                                break
                    except Exception as e:
                        print(f"  - Error en último intento: {e}")
            
            # Verificar si pudimos encontrar columnas de vacuna
            if not columnas_vacuna:
                self.advertencias.append(
                    f"Archivo {os.path.basename(ruta_archivo)}: "
                    f"no se encontraron columnas para la vacuna '{self.vacuna}'"
                )
                print(f"  - ERROR: No se encontraron columnas para la vacuna '{self.vacuna}' después de múltiples intentos")
                return pd.DataFrame()
            
            print(f"  - Columnas de vacuna encontradas: {columnas_vacuna}")
            
            # Si no hemos identificado la columna de dosis aún, hacerlo ahora
            if not col_dosis:
                if modo_jerarquico:
                    # En modo jerárquico, buscar por el segundo nivel "Dosis"
                    for col in columnas_vacuna:
                        if isinstance(col, tuple) and len(col) >= 2 and "dosis" in str(col[1]).lower():
                            col_dosis = col
                            break
                else:
                    # Modo tradicional
                    col_dosis = identificar_columna_dosis(df, columnas_vacuna)
            
            if col_dosis:
                print(f"  - Columna de dosis identificada: {col_dosis}")
            else:
                print("  - No se pudo identificar columna de dosis")
            
            # Columnas base deseadas - Ajustadas para la nueva estructura
            columnas_base_needed = [
                "Consecutivo", 
                "Fecha", 
                "Tipo de identificación", 
                "Número de identificación", 
                "Primer nombre", 
                "Primer apellido", 
                "AÑOS", 
                "MESES", 
                "DIAS", 
                "Sexo", 
                "Departamento de residencia", 
                "Municipio de residencia",
                "Comuna/Localidad",
                "Área",
                "Dirección"
            ]
            
            # Buscar columnas existentes que coincidan con los términos
            columnas_existentes = []
            for col in df.columns:
                # En modo jerárquico, los nombres son tuplas, verificar diferente
                if modo_jerarquico and isinstance(col, tuple):
                    col_str = " ".join([str(parte) for parte in col if pd.notna(parte)])
                else:
                    col_str = str(col)
                    
                col_lower = col_str.lower()
                if any(base.lower() in col_lower for base in columnas_base_needed):
                    columnas_existentes.append(col)
            
            # Columnas a extraer - combinamos existentes más las de vacuna
            columnas_seleccionadas = list(set(columnas_existentes + list(columnas_vacuna)))
            
            # Verificar que existen las columnas antes de filtrar
            columnas_validas = [col for col in columnas_seleccionadas if col in df.columns]
            try:
                df_filtrado = df[columnas_validas].copy()
            except Exception as e:
                print(f"  - Error al filtrar columnas: {str(e)}")
                # Último intento: tomar solo las columnas de vacuna que existen
                columnas_vacuna_validas = [col for col in columnas_vacuna if col in df.columns]
                if not columnas_vacuna_validas:
                    print("  - No se pueden filtrar las columnas de vacuna, no existen en el DataFrame")
                    return pd.DataFrame()
                    
                df_filtrado = df[columnas_vacuna_validas].copy()
                # Agregar columnas vacías para los campos básicos
                for campo in ["Fecha", "Departamento_Residencia", "Municipio_Residencia"]:
                    df_filtrado[campo] = None
            
            # Procesar la información según el modo (jerárquico o tradicional)
            if modo_jerarquico:
                # En modo jerárquico, los nombres de columnas son tuplas
                # Necesitamos encontrar y procesar las columnas de manera especial
                
                # Limpiar datos para columnas jerárquicas
                # Buscar columna de fecha
                fecha_col = None
                for col in df_filtrado.columns:
                    if isinstance(col, tuple) and any("fecha" in str(parte).lower() for parte in col):
                        fecha_col = col
                        break
                
                # Procesar fechas
                if fecha_col:
                    df_filtrado = df_filtrado[df_filtrado[fecha_col].notna() & (df_filtrado[fecha_col] != "fin")]
                    try:
                        df_filtrado["Fecha"] = pd.to_datetime(df_filtrado[fecha_col], format="%m/%d/%y", errors="coerce")
                    except Exception as e:
                        print(f"  - Error al convertir fechas: {e}")
                        try:
                            df_filtrado["Fecha"] = pd.to_datetime(df_filtrado[fecha_col], errors="coerce")
                        except:
                            df_filtrado["Fecha"] = pd.NaT
                else:
                    # Usar información de ruta si no hay columna de fecha
                    if info_ruta["año"] and info_ruta["mes"]:
                        fecha_str = f"{info_ruta['año']}-{info_ruta['mes']}-01"
                        df_filtrado["Fecha"] = pd.to_datetime(fecha_str)
                    else:
                        df_filtrado["Fecha"] = pd.NaT
                
                # Procesar información de vacunación para columnas jerárquicas
                if col_dosis:
                    df_filtrado["Vacunado"] = df_filtrado[col_dosis].notna() & (df_filtrado[col_dosis] != "fin")
                    df_filtrado["Tipo_Dosis"] = df_filtrado[col_dosis].apply(
                        lambda x: limpiar_texto(x) if pd.notna(x) and x != "fin" else None
                    )
                    
                    # Contadores por tipo de dosis
                    df_filtrado["Es_Primera_Dosis"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "PRIMERA" in str(x).upper() else 0)
                    df_filtrado["Es_Segunda_Dosis"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "SEGUNDA" in str(x).upper() else 0)
                    df_filtrado["Es_Refuerzo"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "REFUERZO" in str(x).upper() else 0)
                    df_filtrado["Es_Unica_Dosis"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "UNICA" in str(x).upper() else 0)
                else:
                    # Si no identificamos dosis, verificar cualquier dato en columnas de vacuna
                    df_filtrado["Vacunado"] = df_filtrado[columnas_vacuna].notna().any(axis=1)
                    df_filtrado["Tipo_Dosis"] = None
                    df_filtrado["Es_Primera_Dosis"] = 0
                    df_filtrado["Es_Segunda_Dosis"] = 0
                    df_filtrado["Es_Refuerzo"] = 0
                    df_filtrado["Es_Unica_Dosis"] = 0
                
                # Encontrar columnas relevantes para información de residencia
                col_depto = None
                col_muni = None
                col_local = None
                col_area = None
                
                for col in df_filtrado.columns:
                    if not isinstance(col, tuple):
                        continue
                        
                    col_str = " ".join([str(parte) for parte in col if pd.notna(parte)]).lower()
                    
                    if "departamento" in col_str and "residencia" in col_str:
                        col_depto = col
                    elif "municipio" in col_str and "residencia" in col_str:
                        col_muni = col
                    elif "comuna" in col_str or "localidad" in col_str:
                        col_local = col
                    elif "área" in col_str or "area" in col_str:
                        col_area = col
                
                # Establecer columnas de residencia
                df_filtrado["Departamento_Residencia"] = None if col_depto is None else df_filtrado[col_depto].apply(
                    lambda x: limpiar_texto(x) if pd.notna(x) else None)
                    
                df_filtrado["Municipio_Residencia"] = None if col_muni is None else df_filtrado[col_muni].apply(
                    lambda x: limpiar_texto(x) if pd.notna(x) else None)
                    
                df_filtrado["Localidad_Residencia"] = None if col_local is None else df_filtrado[col_local].apply(
                    lambda x: limpiar_texto(x) if pd.notna(x) else None)
                    
                df_filtrado["Area_Residencia"] = None if col_area is None else df_filtrado[col_area].apply(
                    lambda x: limpiar_texto(x) if pd.notna(x) else None)
                
                # Encontrar columna de edad para clasificación
                col_edad = None
                for col in df_filtrado.columns:
                    if not isinstance(col, tuple):
                        continue
                        
                    col_str = " ".join([str(parte) for parte in col if pd.notna(parte)]).lower()
                    if "años" in col_str or "edad" in col_str:
                        col_edad = col
                        break
                
                # Clasificar por grupo etario
                if col_edad:
                    try:
                        df_filtrado["Edad_Num"] = pd.to_numeric(df_filtrado[col_edad], errors="coerce")
                        df_filtrado["Grupo_Etario"] = df_filtrado["Edad_Num"].apply(clasificar_grupo_etario)
                    except Exception as e:
                        print(f"  - Error al clasificar por grupo etario: {e}")
                        df_filtrado["Grupo_Etario"] = "No especificado"
                else:
                    df_filtrado["Grupo_Etario"] = "No especificado"
                    
            else:
                # Procesamiento en modo tradicional (no jerárquico)
                # Limpiar datos
                # Buscar columna de fecha por coincidencia parcial
                fecha_cols = [col for col in df_filtrado.columns if "fecha" in str(col).lower()]
                if fecha_cols:
                    fecha_col = fecha_cols[0]
                    df_filtrado = df_filtrado[df_filtrado[fecha_col].notna() & (df_filtrado[fecha_col] != "fin")]
                    
                    # Convertir fechas con manejo de errores
                    try:
                        df_filtrado["Fecha"] = pd.to_datetime(
                            df_filtrado[fecha_col],
                            format="%m/%d/%y",
                            errors="coerce"
                        )
                    except Exception as e:
                        print(f"  - Error al convertir fechas con formato %m/%d/%y: {e}")
                        
                        # Intentar con otro formato común
                        try:
                            df_filtrado["Fecha"] = pd.to_datetime(
                                df_filtrado[fecha_col],
                                errors="coerce"
                            )
                            print("  - Fechas convertidas usando formato automático")
                        except Exception as e2:
                            print(f"  - Error al convertir fechas automáticamente: {e2}")
                else:
                    print(f"  - Advertencia: No se encontró columna de fecha")
                    # Crear una columna de fecha usando la información extraída
                    if info_ruta["año"] and info_ruta["mes"]:
                        fecha_str = f"{info_ruta['año']}-{info_ruta['mes']}-01"
                        df_filtrado["Fecha"] = pd.to_datetime(fecha_str)
                    else:
                        df_filtrado["Fecha"] = pd.NaT
                
                # Agregar columnas para lugar de vacunación
                df_filtrado["Municipio_Vacunacion"] = municipio
                df_filtrado["Año_Registro"] = info_ruta.get("año")
                df_filtrado["Mes_Registro"] = info_ruta.get("mes")
                
                # Procesar información de residencia con búsqueda flexible
                df_filtrado["Departamento_Residencia"] = None
                df_filtrado["Municipio_Residencia"] = None
                df_filtrado["Localidad_Residencia"] = None
                df_filtrado["Area_Residencia"] = None
                
                # Mapeo flexible para columnas similares
                campos_busqueda = {
                    "Departamento_Residencia": ["departamento", "depto", "dpto"],
                    "Municipio_Residencia": ["municipio", "ciudad", "muni"],
                    "Localidad_Residencia": ["comuna", "localidad", "barrio", "vereda"],
                    "Area_Residencia": ["area", "área", "zona", "urbana", "rural"]
                }
                
                # Buscar y mapear columnas que coincidan
                for campo_dest, terminos in campos_busqueda.items():
                    for col in df_filtrado.columns:
                        col_lower = str(col).lower()
                        if any(term in col_lower for term in terminos) and "residencia" in col_lower:
                            try:
                                df_filtrado[campo_dest] = df_filtrado[col].apply(
                                    lambda x: limpiar_texto(x) if pd.notna(x) else None
                                )
                                print(f"  - Mapeado '{col}' a '{campo_dest}'")
                                break
                            except Exception as e:
                                print(f"  - Error al mapear {col} a {campo_dest}: {e}")
                
                # Identificar veredas y áreas más específicamente
                if "Area_Residencia" in df_filtrado.columns:
                    df_filtrado["Tipo_Area"] = df_filtrado["Area_Residencia"].apply(
                        lambda x: "URBANA" if x and "URBANA" in str(x).upper() else 
                                "RURAL" if x and "RURAL" in str(x).upper() else "OTRA"
                    )
                
                # Extraer vereda del campo de localidad o dirección si está disponible
                vereda_extraida = False
                if "Localidad_Residencia" in df_filtrado.columns and df_filtrado["Localidad_Residencia"].notna().any():
                    df_filtrado["Vereda_Residencia"] = df_filtrado["Localidad_Residencia"]
                    vereda_extraida = True
                    
                if not vereda_extraida:
                    # Buscar columna de dirección
                    dir_cols = [col for col in df_filtrado.columns if "direcc" in str(col).lower()]
                    if dir_cols:
                        dir_col = dir_cols[0]
                        # Intentar extraer vereda de la dirección
                        df_filtrado["Vereda_Residencia"] = df_filtrado[dir_col].apply(
                            lambda x: extraer_vereda_de_direccion(x) if pd.notna(x) else None
                        )
                
                # Determinar si tiene aplicación de la vacuna
                if col_dosis:
                    df_filtrado["Vacunado"] = df_filtrado[col_dosis].notna() & (df_filtrado[col_dosis] != "fin")
                    df_filtrado["Tipo_Dosis"] = df_filtrado[col_dosis].apply(
                        lambda x: limpiar_texto(x) if pd.notna(x) and x != "fin" else None
                    )
                    
                    # Añadir contadores por tipo de dosis
                    df_filtrado["Es_Primera_Dosis"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "PRIMERA" in str(x).upper() else 0
                    )
                    df_filtrado["Es_Segunda_Dosis"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "SEGUNDA" in str(x).upper() else 0
                    )
                    df_filtrado["Es_Refuerzo"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "REFUERZO" in str(x).upper() else 0
                    )
                    df_filtrado["Es_Unica_Dosis"] = df_filtrado["Tipo_Dosis"].apply(
                        lambda x: 1 if x and "UNICA" in str(x).upper() else 0
                    )
                else:
                    # Si no se encontró columna de dosis, verificar datos en cualquier columna de la vacuna
                    df_filtrado["Vacunado"] = df_filtrado[columnas_vacuna].notna().any(axis=1)
                    df_filtrado["Tipo_Dosis"] = None
                    df_filtrado["Es_Primera_Dosis"] = 0
                    df_filtrado["Es_Segunda_Dosis"] = 0
                    df_filtrado["Es_Refuerzo"] = 0
                    df_filtrado["Es_Unica_Dosis"] = 0
                
                # Limpiar y normalizar todos los datos de texto
                cols_texto = ["Municipio_Vacunacion", "Departamento_Residencia", "Municipio_Residencia", 
                            "Localidad_Residencia", "Vereda_Residencia"]
                
                for col in cols_texto:
                    if col in df_filtrado.columns:
                        df_filtrado[col] = df_filtrado[col].apply(
                            lambda x: limpiar_texto(x) if pd.notna(x) else None
                        )
                
                # Clasificar por grupo etario con manejo seguro
                cols_edad = [col for col in df_filtrado.columns if "año" in str(col).lower() or "edad" in str(col).lower()]
                if cols_edad:
                    col_edad = cols_edad[0]
                    try:
                        # Convertir a numérico con manejo de errores
                        df_filtrado["Edad_Num"] = pd.to_numeric(df_filtrado[col_edad], errors="coerce")
                        df_filtrado["Grupo_Etario"] = df_filtrado["Edad_Num"].apply(clasificar_grupo_etario)
                    except Exception as e:
                        print(f"  - Error al calcular grupos etarios: {str(e)}")
                        df_filtrado["Grupo_Etario"] = "No especificado"
                else:
                    print("  - No se encontró columna de edad/años para clasificar grupos etarios")
                    df_filtrado["Grupo_Etario"] = "No especificado"
            
            # Verificar si se encontraron datos útiles (común para ambos modos)
            if len(df_filtrado) == 0:
                print(f"  - Advertencia: No se encontraron registros en {os.path.basename(ruta_archivo)}")
                return pd.DataFrame()
                
            num_vacunados = df_filtrado["Vacunado"].sum() if "Vacunado" in df_filtrado.columns else 0
            if num_vacunados == 0:
                print(f"  - Advertencia: No se encontraron registros de vacunación en {os.path.basename(ruta_archivo)}")
                
            # Actualizar contador de registros procesados
            self.archivos_procesados += 1
            self.registros_totales += len(df_filtrado)
            
            print(f"  - Procesamiento exitoso: {len(df_filtrado)} registros, {num_vacunados} vacunaciones")
            return df_filtrado
                
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            self.advertencias.append(f"Error al procesar {os.path.basename(ruta_archivo)}: {str(e)}")
            print(f"  - Error al procesar {os.path.basename(ruta_archivo)}: {str(e)}")
            print(f"  - Traceback: {error_traceback}")
            return pd.DataFrame()
    
    def consolidar_archivos(self, directorio: str, patron: str = "*.xls*", 
                          patrones_exclusion: List[str] = None,
                          procesar_directorio_exacto: bool = False) -> dict:
        """
        Consolida datos de todos los archivos PAI en un directorio.
        
        Args:
            directorio: Carpeta base donde están los registros.
            patron: Patrón para identificar archivos (default: "*.xls*").
            patrones_exclusion: Lista de patrones para excluir archivos (ej: ["COVID"]).
            procesar_directorio_exacto: Si es True, procesa solo archivos en el directorio exacto.
            
        Returns:
            Diccionario con DataFrames consolidados según el tipo seleccionado.
        """
        # Reiniciar contadores
        self.archivos_procesados = 0
        self.registros_totales = 0
        self.advertencias = []
        
        # Listar archivos dependiendo del modo
        if procesar_directorio_exacto:
            print(f"Procesando solo archivos en el directorio exacto: {directorio}")
            # Obtener archivos directamente sin buscar en subdirectorios
            archivos_brutos = glob.glob(os.path.join(directorio, patron))
            
            # Aplicar exclusiones
            archivos = []
            for archivo in archivos_brutos:
                if patrones_exclusion and debe_excluir_archivo(archivo, patrones_exclusion):
                    print(f"Excluyendo: {os.path.basename(archivo)}")
                else:
                    archivos.append(archivo)
        else:
            # Usar la función jerárquica
            archivos = listar_archivos_pai(directorio, patron, patrones_exclusion)
        
        if not archivos:
            self.advertencias.append(f"No se encontraron archivos que coincidan con {patron} en {directorio}")
            return {}
        
        print(f"Se encontraron {len(archivos)} archivos para procesar...")
        
        # Lista para almacenar DataFrames
        dfs = []
        
        # Procesar cada archivo
        for archivo in archivos:
            df = self.procesar_archivo(archivo)
            if not df.empty:
                dfs.append(df)
        
        # Combinar todos los DataFrames
        resultado = {}
        
        if dfs:
            df_combinado = pd.concat(dfs, ignore_index=True)
            print(f"Consolidación completada: {self.archivos_procesados} archivos procesados, "
                f"{self.registros_totales} registros totales")
            
            # Generar los diferentes tipos de consolidado
            if self.tipo_consolidado == "vacunacion" or self.tipo_consolidado == "ambos":
                # Consolidado por lugar de vacunación
                resultado["vacunacion"] = df_combinado.copy()
            
            if self.tipo_consolidado == "residencia" or self.tipo_consolidado == "ambos":
                # Consolidado por lugar de residencia
                df_por_residencia = df_combinado.copy()
                
                # Reordenar columnas para priorizar datos de residencia
                cols_residencia = [col for col in df_por_residencia.columns 
                                if "Residencia" in col or "Departamento_" in col or "Municipio_" in col]
                cols_resto = [col for col in df_por_residencia.columns if col not in cols_residencia]
                df_por_residencia = df_por_residencia[cols_residencia + cols_resto]
                
                resultado["residencia"] = df_por_residencia
            
            # Mostrar advertencias
            if self.advertencias:
                print("\nAdvertencias durante el procesamiento:")
                for adv in self.advertencias:
                    print(f"- {adv}")
            
            return resultado
        else:
            print("No se pudo procesar ningún archivo correctamente.")
            return {}