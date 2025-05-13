"""
Funciones de utilidad para el procesamiento de archivos PAI.
"""
import os
import re
import glob
from typing import List, Dict, Any, Optional, Union, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

def listar_archivos_pai(directorio_base: str, patron: str = "*.xls*", 
                        excluir_patrones: List[str] = None) -> List[str]:
    """
    Lista todos los archivos PAI en estructura de directorios por año y municipio.
    
    Args:
        directorio_base: Directorio base donde buscar.
        patron: Patrón de archivos a incluir (*.xlsx, *.xlsm, etc.)
        excluir_patrones: Lista de patrones a excluir (ej. ["COVID", "respaldo"])
        
    Returns:
        Lista de rutas a archivos PAI encontrados.
    """
    # Garantizar que siempre tengamos un patrón válido para archivos Excel
    if patron is None:
        patron = "*.xls*"  # Captura xlsx, xlsm, xls, etc.
        
    if excluir_patrones is None:
        excluir_patrones = ["COVID", "covid"]
    
    archivos_encontrados = []
    
    # Rutas a explorar: primero el propio directorio_base
    rutas_a_explorar = [directorio_base]
    
    # Añadir subdirectorios directos para búsqueda
    for item in os.listdir(directorio_base):
        ruta_item = os.path.join(directorio_base, item)
        if os.path.isdir(ruta_item):
            rutas_a_explorar.append(ruta_item)
    
    # Buscar estructura de año (REGISTROS_XXXX) y municipios
    for ruta in rutas_a_explorar:
        # Si parece ser un directorio de registros de año
        if "REGISTROS_" in ruta or ruta[-4:].isdigit():
            # Buscar carpetas de municipios dentro
            for municipio in os.listdir(ruta):
                ruta_municipio = os.path.join(ruta, municipio)
                if os.path.isdir(ruta_municipio):
                    # Buscar archivos Excel dentro de la carpeta del municipio
                    for archivo in glob.glob(os.path.join(ruta_municipio, patron)):
                        # Verificar si el archivo coincide con algún patrón de exclusión
                        if not any(excluir in archivo for excluir in excluir_patrones):
                            archivos_encontrados.append(archivo)
        
        # Buscar directamente en la ruta actual (podría ser una carpeta de municipio)
        for archivo in glob.glob(os.path.join(ruta, patron)):
            # Verificar si el archivo coincide con algún patrón de exclusión
            if not any(excluir in archivo for excluir in excluir_patrones):
                archivos_encontrados.append(archivo)
    
    return archivos_encontrados

def extraer_fecha_de_archivo(ruta_archivo: str) -> Dict[str, str]:
    """
    Extrae información de año y mes del nombre de archivo o ruta.
    
    Args:
        ruta_archivo: Ruta al archivo.
        
    Returns:
        Diccionario con 'año' y 'mes'
    """
    resultado = {
        "año": None,
        "mes": None
    }
    
    # Intentar extraer año de la ruta
    componentes = os.path.normpath(ruta_archivo).split(os.sep)
    for comp in componentes:
        # Buscar "REGISTROS_XXXX" o "20XX"
        if comp.startswith("REGISTROS_") and len(comp) >= 11:
            año_str = comp[-4:]
            if año_str.isdigit() and 2000 <= int(año_str) <= 2100:
                resultado["año"] = año_str
        elif comp.isdigit() and len(comp) == 4 and 2000 <= int(comp) <= 2100:
            resultado["año"] = comp
    
    # Extraer nombre del archivo
    nombre_archivo = os.path.basename(ruta_archivo).upper()
    
    # Intentar extraer año del nombre si no se encontró en la ruta
    if not resultado["año"]:
        # Buscar patrón "20XX" en el nombre
        match_año = re.search(r'(20\d{2})', nombre_archivo)
        if match_año:
            resultado["año"] = match_año.group(1)
    
    # Buscar mes en el nombre del archivo
    meses = {
        "ENERO": "01", "ENE": "01", "FEBRUARY": "02", "FEB": "02", "MARZO": "03", "MAR": "03",
        "ABRIL": "04", "ABR": "04", "MAYO": "05", "MAY": "05", "JUNIO": "06", "JUN": "06",
        "JULIO": "07", "JUL": "07", "AGOSTO": "08", "AGO": "08", "SEPTIEMBRE": "09", "SEP": "09",
        "OCTUBRE": "10", "OCT": "10", "NOVIEMBRE": "11", "NOV": "11", "DICIEMBRE": "12", "DIC": "12"
    }
    
    for mes_nombre, mes_num in meses.items():
        if mes_nombre in nombre_archivo:
            resultado["mes"] = mes_num
            break
    
    return resultado

def extraer_municipio_de_ruta(ruta_archivo: str) -> str:
    """
    Identifica el municipio desde la ruta del archivo.
    
    Args:
        ruta_archivo: Ruta completa al archivo.
        
    Returns:
        Nombre del municipio.
    """
    # Dividir la ruta en componentes
    componentes = os.path.normpath(ruta_archivo).split(os.sep)
    
    # Buscar el componente que parece ser un municipio
    # Típicamente sería la carpeta después de "REGISTROS_XXXX"
    for i, comp in enumerate(componentes[:-1]):  # Excluir el nombre del archivo
        if comp.startswith("REGISTROS_") and i + 1 < len(componentes) - 1:
            return componentes[i + 1].upper()
    
    # Si no se encontró con el patrón anterior, buscar un componente que no sea "REGISTROS_" y no parezca año
    for comp in componentes[:-1]:  # Excluir el nombre del archivo
        if (not comp.startswith("REGISTROS_") and 
            not comp.isdigit() and 
            comp.upper() == comp and  # Está en mayúsculas
            len(comp) > 2):  # No es muy corto
            return comp.upper()
    
    # Si no se puede determinar, extraer del nombre del archivo
    nombre_archivo = os.path.basename(ruta_archivo)
    # Buscar primeras letras hasta un separador
    match = re.match(r'^([A-Za-z]+)[_\s]', nombre_archivo)
    if match:
        return match.group(1).upper()
    
    # En último caso, devolver "DESCONOCIDO"
    return "DESCONOCIDO"

def analizar_estructura_excel(ruta_archivo: str, forzar_jerarquico: bool = True) -> Dict[str, Any]:
    """
    Analiza la estructura de un archivo Excel PAI para determinar encabezados.
    
    Args:
        ruta_archivo: Ruta al archivo Excel.
        forzar_jerarquico: Si True, fuerza la detección de estructura jerárquica.
        
    Returns:
        Diccionario con información de la estructura.
    """
    estructura = {
        "hojas": [],
        "hoja_seleccionada": None,
        "filas_encabezado": [0, 1],  # Por defecto usar las dos primeras filas
        "categorias_detectadas": {},
        "modo_jerarquico": forzar_jerarquico,  # Forzamos modo jerárquico si se solicita
        "error": None
    }
    
    try:
        # Determinar extensión para usar el engine correcto
        ext = os.path.splitext(ruta_archivo)[1].lower()
        engine = 'openpyxl' if ext in ['.xlsx', '.xlsm'] else 'xlrd'
        
        # Leer información del archivo
        try:
            excel_file = pd.ExcelFile(ruta_archivo, engine=engine)
            estructura["hojas"] = excel_file.sheet_names
            
            # Buscar específicamente la hoja "Registro Diario"
            hoja_objetivo = None
            if "Registro Diario" in excel_file.sheet_names:
                hoja_objetivo = "Registro Diario"
            else:
                # Buscar hojas similares si no existe la exacta
                for hoja in excel_file.sheet_names:
                    if "registro" in hoja.lower() and "diario" in hoja.lower():
                        hoja_objetivo = hoja
                        break
                
                # Si no encontramos ninguna, usar la primera hoja
                if not hoja_objetivo and excel_file.sheet_names:
                    hoja_objetivo = excel_file.sheet_names[0]
                    
            estructura["hoja_seleccionada"] = hoja_objetivo
        except Exception as e:
            estructura["error"] = f"Error al leer hojas: {str(e)}"
            return estructura
        
        if not hoja_objetivo:
            estructura["error"] = "No se encontraron hojas en el archivo"
            return estructura
        
        # Leer las primeras filas para análisis
        try:
            df_encabezados = pd.read_excel(
                ruta_archivo,
                sheet_name=hoja_objetivo,
                engine=engine,
                header=None,
                nrows=5  # Leer primeras 5 filas para análisis
            )
        except Exception as e:
            estructura["error"] = f"Error al leer encabezados: {str(e)}"
            return estructura
        
        # Si se fuerza el modo jerárquico, no realizamos detección automática
        if not forzar_jerarquico:
            # Detectar si tiene estructura jerárquica
            valores_unicos_fila0 = df_encabezados.iloc[0].dropna().unique()
            valores_unicos_fila1 = df_encabezados.iloc[1].dropna().unique()
            
            # Criterios para detectar jerarquía
            tiene_jerarquia = (
                len(valores_unicos_fila0) < len(df_encabezados.columns) * 0.5 and
                len(valores_unicos_fila1) > len(valores_unicos_fila0) * 1.5
            )
            
            estructura["modo_jerarquico"] = tiene_jerarquia
        
        # Analizar estructura de categorías
        categorias = {}
        ultimo_valor = None
        
        # Iterar por la primera fila para detectar categorías
        for i in range(len(df_encabezados.columns)):
            valor = df_encabezados.iloc[0, i]
            
            # Si hay un valor (no es NaN), es una nueva categoría
            if pd.notna(valor) and str(valor).strip():
                ultimo_valor = str(valor).strip()
                if ultimo_valor not in categorias:
                    categorias[ultimo_valor] = []
            
            # Si hay un último valor y hay un valor en la segunda fila, añadir subcategoría
            if ultimo_valor is not None and i < len(df_encabezados.columns):
                subcategoria = None
                if pd.notna(df_encabezados.iloc[1, i]):
                    subcategoria = str(df_encabezados.iloc[1, i]).strip()
                else:
                    # Si no hay subcategoría, usar una genérica basada en el índice
                    subcategoria = f"Columna_{i+1}"
                
                categorias[ultimo_valor].append({
                    "nombre": subcategoria,
                    "indice": i
                })
        
        estructura["categorias_detectadas"] = categorias
        return estructura
        
    except Exception as e:
        estructura["error"] = f"Error general: {str(e)}"
        return estructura

def leer_excel_con_estructura(ruta_archivo: str, estructura: Dict[str, Any] = None) -> Tuple[pd.DataFrame, bool]:
    """
    Lee un archivo Excel utilizando la información de estructura para manejar encabezados.
    
    Args:
        ruta_archivo: Ruta al archivo Excel.
        estructura: Información de estructura del archivo (opcional).
        
    Returns:
        Tuple con (DataFrame leído, es_jerarquico)
    """
    if estructura is None:
        estructura = analizar_estructura_excel(ruta_archivo, forzar_jerarquico=True)
    
    # Determinar extensión para usar el engine correcto
    ext = os.path.splitext(ruta_archivo)[1].lower()
    engine = 'openpyxl' if ext in ['.xlsx', '.xlsm'] else 'xlrd'
    
    hoja = estructura["hoja_seleccionada"]
    if not hoja and estructura["hojas"]:
        hoja = estructura["hojas"][0]
    
    # Intentar leer con encabezados jerárquicos si se detectó o forzó
    if estructura["modo_jerarquico"]:
        try:
            df = pd.read_excel(
                ruta_archivo,
                sheet_name=hoja,
                engine=engine,
                header=[0, 1]  # Siempre usar las dos primeras filas en modo jerárquico
            )
            return df, True
        except Exception as e:
            print(f"  - Error al leer con encabezados jerárquicos: {str(e)}")
            print("  - Intentando método alternativo...")
            pass
    
    # Método tradicional (encabezado en una sola fila)
    try:
        fila_encabezado = estructura["filas_encabezado"][0] if estructura["filas_encabezado"] else 1
        df = pd.read_excel(
            ruta_archivo,
            sheet_name=hoja,
            engine=engine,
            header=fila_encabezado
        )
        return df, False
    except Exception as e:
        # Último intento: leer sin encabezados
        try:
            df = pd.read_excel(
                ruta_archivo,
                sheet_name=hoja,
                engine=engine,
                header=None
            )
            return df, False
        except Exception:
            # Si todos los intentos fallan, lanzar la excepción original
            raise e

def limpiar_texto(texto: str) -> str:
    """
    Limpia y normaliza un texto.
    
    Args:
        texto: Texto a limpiar.
        
    Returns:
        Texto limpio y normalizado.
    """
    if not isinstance(texto, str):
        return str(texto)
    
    # Eliminar espacios adicionales
    texto = re.sub(r'\s+', ' ', texto).strip()
    
    # Convertir a mayúsculas
    return texto.upper()

def clasificar_grupo_etario(edad_anios):
    """
    Clasifica la edad en un grupo etario.
    
    Args:
        edad_anios: Edad en años (puede ser int, float, str o None).
        
    Returns:
        Grupo etario correspondiente.
    """
    if pd.isna(edad_anios):
        return "No especificado"
    
    # Intentar convertir a número si es cadena
    if isinstance(edad_anios, str):
        try:
            # Extraer solo dígitos y puntos para manejar decimales
            numeros = ''.join(c for c in edad_anios if c.isdigit() or c == '.')
            if numeros:
                edad_anios = float(numeros)
            else:
                return "No especificado"
        except ValueError:
            return "No especificado"
    
    # Verificar que sea un número válido
    if not isinstance(edad_anios, (int, float)) or pd.isna(edad_anios):
        return "No especificado"
        
    # Clasificar por grupo etario
    try:
        if edad_anios < 1:
            return "<1 año"
        elif edad_anios <= 5:
            return "1-5 años"
        elif edad_anios <= 10:
            return "6-10 años"
        elif edad_anios <= 18:
            return "11-18 años"
        elif edad_anios <= 60:
            return "19-60 años"
        else:
            return ">60 años"
    except Exception:
        return "No especificado"

def normalizar_nombres_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza los nombres de columnas para tener un formato estándar.
    Versión optimizada para manejar encabezados jerárquicos de Excel con 
    estructura específica de datos PAI.
    
    Args:
        df: DataFrame con columnas a normalizar.
        
    Returns:
        DataFrame con nombres de columnas normalizados.
    """
    nuevos_nombres = {}
    
    for col in df.columns:
        # Para columnas jerárquicas (tuplas)
        if isinstance(col, tuple):
            # Manejar caso especial de las primeras dos columnas sin encabezado en nivel 1
            if len(col) >= 2 and col[0] == 'Unnamed: 0_level_0' and col[1] == 'Consecutivo':
                nuevos_nombres[col] = 'Consecutivo'
                continue
            elif len(col) >= 2 and col[0] == 'Unnamed: 1_level_0' and 'Fecha de atención' in str(col[1]):
                nuevos_nombres[col] = 'Fecha_Atencion'
                continue
                
            # Obtener partes significativas
            partes = []
            for parte in col:
                if pd.notna(parte) and str(parte).strip():
                    # Ignorar partes como 'Unnamed: X_level_Y'
                    if not str(parte).startswith('Unnamed:'):
                        # Limpiar texto
                        parte_str = str(parte).strip()
                        # Reemplazar caracteres no alfanuméricos
                        parte_str = re.sub(r'[^\w ]', '_', parte_str)
                        # Convertir espacios a guiones bajos
                        parte_str = re.sub(r'\s+', '_', parte_str)
                        # Eliminar guiones bajos múltiples
                        parte_str = re.sub(r'_+', '_', parte_str)
                        # Eliminar guiones bajos al inicio/final
                        parte_str = parte_str.strip('_')
                        
                        if parte_str:
                            partes.append(parte_str)
            
            # Construir nombre nuevo
            if partes:
                # Unir partes con guiones bajos
                nuevo_nombre = '_'.join(partes)
            else:
                # Si no hay partes válidas, usar un nombre genérico
                idx = list(df.columns).index(col)
                nuevo_nombre = f"Columna_{idx}"
            
            nuevos_nombres[col] = nuevo_nombre
        
        # Para columnas simples (no tuplas)
        else:
            if pd.notna(col):
                nuevo_nombre = str(col).strip()
                # Limpiar texto
                nuevo_nombre = re.sub(r'[^\w ]', '_', nuevo_nombre)
                nuevo_nombre = re.sub(r'\s+', '_', nuevo_nombre)
                nuevo_nombre = re.sub(r'_+', '_', nuevo_nombre)
                nuevo_nombre = nuevo_nombre.strip('_')
                
                if not nuevo_nombre:
                    idx = list(df.columns).index(col)
                    nuevo_nombre = f"Columna_{idx}"
                
                nuevos_nombres[col] = nuevo_nombre
            else:
                idx = list(df.columns).index(col)
                nuevos_nombres[col] = f"Columna_{idx}"
    
    # Verificar y resolver nombres duplicados
    nombres_usados = set()
    for col, nuevo_nombre in list(nuevos_nombres.items()):
        if nuevo_nombre in nombres_usados:
            # Si ya existe, añadir un sufijo numérico basado en la posición
            idx = list(df.columns).index(col)
            nuevos_nombres[col] = f"{nuevo_nombre}_{idx}"
        nombres_usados.add(nuevos_nombres[col])
    
    # Aplicar renombrado
    df_normalizado = df.rename(columns=nuevos_nombres)
    
    # Verificar que todas las columnas se hayan normalizado
    columnas_problemáticas = [col for col in df_normalizado.columns if isinstance(col, tuple)]
    
    return df_normalizado

def validar_normalizacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida que todas las columnas se hayan normalizado correctamente.
    Aplica correcciones adicionales a columnas que siguen siendo tuplas.
    
    Args:
        df: DataFrame a validar.
        
    Returns:
        DataFrame con todas las columnas correctamente normalizadas.
    """
    # Identificar columnas que siguen siendo tuplas
    columnas_problematicas = [col for col in df.columns if isinstance(col, tuple)]
    
    if columnas_problematicas:
        # Crear diccionario para renombrar
        nuevos_nombres = {}
        
        for col in columnas_problematicas:
            # Crear nombre basado en contenido y posición
            idx = list(df.columns).index(col)
            
            # Unir partes no nulas
            partes = []
            for parte in col:
                if pd.notna(parte) and str(parte).strip():
                    parte_str = str(parte).strip()
                    # Convertir caracteres no alfanuméricos a guiones bajos
                    parte_str = re.sub(r'[^\w]', '_', parte_str)
                    # Eliminar guiones bajos múltiples
                    parte_str = re.sub(r'_+', '_', parte_str)
                    # Eliminar guiones bajos al inicio/final
                    parte_str = parte_str.strip('_')
                    
                    if parte_str:
                        partes.append(parte_str)
            
            if partes:
                nuevo_nombre = f"{'_'.join(partes)}_{idx}"
            else:
                nuevo_nombre = f"Columna_{idx}"
            
            nuevos_nombres[col] = nuevo_nombre
            print(f"  - Corrigiendo columna problemática: {col} -> {nuevo_nombre}")
        
        # Aplicar renombrado
        df = df.rename(columns=nuevos_nombres)
    
    return df