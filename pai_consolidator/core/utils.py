"""
Funciones de utilidad para el procesamiento de archivos PAI.
"""
import os
import re
import glob
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

def extraer_nombre_municipio(ruta_archivo: str) -> str:
    """
    Extrae el nombre del municipio del nombre del archivo.
    
    Args:
        ruta_archivo: Ruta completa al archivo.
        
    Returns:
        Nombre del municipio extraído del nombre del archivo.
    """
    nombre_archivo = os.path.basename(ruta_archivo)
    # Intenta extraer el nombre del municipio usando varios patrones
    
    # Patrón 1: MUNICIPIO_REGISTRO_...
    match = re.match(r'^([A-Za-z]+)_', nombre_archivo)
    if match:
        return match.group(1).upper()
    
    # Patrón 2: REGISTRO_MUNICIPIO_...
    match = re.match(r'^REGISTRO_([A-Za-z]+)_', nombre_archivo)
    if match:
        return match.group(1).upper()
    
    # Si no se puede extraer, usa el nombre sin extensión
    return os.path.splitext(nombre_archivo)[0].upper()

def extraer_info_ruta(ruta_archivo: str) -> dict:
    """
    Extrae información de la ruta completa al archivo.
    
    Args:
        ruta_archivo: Ruta completa al archivo.
        
    Returns:
        Diccionario con información extraída (municipio, año, mes).
    """
    # Dividir la ruta en componentes
    componentes = os.path.normpath(ruta_archivo).split(os.sep)
    resultado = {
        "municipio": None,
        "año": None,
        "mes": None
    }
    
    # Extraer nombre del archivo sin extensión
    nombre_archivo = os.path.splitext(os.path.basename(ruta_archivo))[0].upper()
    
    # Buscar información en los componentes de la ruta
    for i, comp in enumerate(componentes):
        # Buscar patrón de año en nombres de carpeta
        if comp.startswith("REGISTROS_") and len(comp) >= 11:
            año_str = comp[-4:]
            if año_str.isdigit():
                resultado["año"] = año_str
                # El municipio debería ser el siguiente componente después del año
                if i + 1 < len(componentes) - 1:  # Asegurarse que no es el último (que sería el archivo)
                    resultado["municipio"] = componentes[i + 1].upper()
    
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
    
    # Si no encontramos el municipio por la estructura, intentar buscar de otra forma
    # (aunque esto no debería ser necesario según la nueva información)
    if not resultado["municipio"]:
        # Buscar si alguna carpeta padre podría ser un municipio
        for i in range(len(componentes) - 2, -1, -1):  # Excluir el archivo mismo
            comp = componentes[i]
            # Si no parece ser una carpeta especial (registro, año, etc.)
            if not comp.startswith("REGISTROS_") and not comp[-4:].isdigit() and comp.upper() == comp:
                resultado["municipio"] = comp.upper()
                break
    
    return resultado

def listar_archivos_pai(directorio_base: str, patron: str = "*.xlsm") -> list:
    """
    Lista todos los archivos PAI en estructura de directorios por año y municipio.
    
    Args:
        directorio_base: Directorio base con las carpetas de años (REGISTROS_XXXX).
        patron: Patrón de archivo a buscar.
        
    Returns:
        Lista de rutas a archivos PAI encontrados.
    """
    archivos_encontrados = []
    
    # Buscar carpetas de años (REGISTROS_XXXX)
    for item in os.listdir(directorio_base):
        ruta_item = os.path.join(directorio_base, item)
        
        # Si es un directorio y parece ser una carpeta de registros por año
        if os.path.isdir(ruta_item) and (item.startswith("REGISTROS_") or item[-4:].isdigit()):
            # Buscar carpetas de municipios dentro
            for municipio in os.listdir(ruta_item):
                ruta_municipio = os.path.join(ruta_item, municipio)
                
                # Si es un directorio, asumimos que es una carpeta de municipio
                if os.path.isdir(ruta_municipio):
                    # Buscar archivos Excel dentro de la carpeta del municipio
                    for archivo in glob.glob(os.path.join(ruta_municipio, patron)):
                        archivos_encontrados.append(archivo)
    
    # También buscar archivos en el directorio base (por si acaso)
    for archivo in glob.glob(os.path.join(directorio_base, patron)):
        archivos_encontrados.append(archivo)
    
    return archivos_encontrados

def encontrar_columnas_vacuna(df: pd.DataFrame, vacuna: str) -> List[str]:
    """
    Encuentra todas las columnas relacionadas con una vacuna específica.
    
    Args:
        df: DataFrame con los datos del PAI.
        vacuna: Nombre de la vacuna a buscar.
        
    Returns:
        Lista de nombres de columnas relacionadas con la vacuna.
    """
    return [col for col in df.columns if vacuna.lower() in str(col).lower()]

def identificar_columna_dosis(df: pd.DataFrame, columnas_vacuna: List[str]) -> Optional[str]:
    """
    Identifica la columna que contiene la información de dosis.
    
    Args:
        df: DataFrame con los datos del PAI.
        columnas_vacuna: Lista de columnas relacionadas con la vacuna.
        
    Returns:
        Nombre de la columna de dosis o None si no se encuentra.
    """
    # Intentar encontrar en la primera fila
    for col in columnas_vacuna:
        if df[col].iloc[0] and "dosis" in str(df[col].iloc[0]).lower():
            return col
    
    # Buscar en todos los valores únicos
    for col in columnas_vacuna:
        valores = df[col].dropna().unique()
        if any("dosis" in str(v).lower() for v in valores):
            return col
    
    # Si hay una columna que específicamente se llama "Dosis"
    for col in columnas_vacuna:
        if "dosis" in col.lower():
            return col
    
    return None

def clasificar_grupo_etario(edad_anios: int) -> str:
    """
    Clasifica la edad en un grupo etario.
    
    Args:
        edad_anios: Edad en años.
        
    Returns:
        Grupo etario correspondiente.
    """
    if pd.isna(edad_anios):
        return "No especificado"
    
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

def extraer_vereda_de_direccion(direccion: str) -> str:
    """
    Intenta extraer el nombre de la vereda de una dirección.
    
    Args:
        direccion: Texto de dirección.
        
    Returns:
        Nombre de vereda extraído o None si no se identifica.
    """
    # Lista de palabras clave que pueden indicar una vereda en direcciones rurales
    palabras_clave = ["VEREDA", "VDA", "VER.", "CASERIO", "CORREGIMIENTO", "FINCA"]
    
    if not isinstance(direccion, str):
        return None
        
    direccion = direccion.upper()
    
    # Buscar palabras clave en la dirección
    for palabra in palabras_clave:
        if palabra in direccion:
            # Obtener el texto después de la palabra clave hasta el siguiente separador
            idx = direccion.find(palabra) + len(palabra)
            
            # Obtener el resto de la dirección después de la palabra clave
            resto = direccion[idx:].strip()
            
            # Buscar el primer separador (coma, punto, guion, etc.)
            for sep in [',', '.', '-', ';', '(']:
                sep_idx = resto.find(sep)
                if sep_idx > 0:
                    return resto[:sep_idx].strip()
            
            # Si no hay separador, tomar como máximo las primeras 30 letras
            return resto[:30].strip() if resto else None
    
    return None