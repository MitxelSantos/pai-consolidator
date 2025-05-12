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
    
    # Buscar mes en el nombre del archivo - más robusto
    nombre_archivo_upper = nombre_archivo.upper()
    
    # Diccionario con todas las posibles formas de meses
    meses = {
        # Nombres completos
        "ENERO": "01", "FEBRUARY": "02", "FEBRERO": "02", "MARZO": "03", "MARCH": "03",
        "ABRIL": "04", "APRIL": "04", "MAYO": "05", "MAY": "05", "JUNIO": "06", "JUNE": "06",
        "JULIO": "07", "JULY": "07", "AGOSTO": "08", "AUGUST": "08", 
        "SEPTIEMBRE": "09", "SEPTEMBER": "09", "OCTUBRE": "10", "OCTOBER": "10", 
        "NOVIEMBRE": "11", "NOVEMBER": "11", "DICIEMBRE": "12", "DECEMBER": "12",
        
        # Abreviaciones
        "ENE": "01", "JAN": "01", "FEB": "02", "MAR": "03", "ABR": "04", "APR": "04",
        "MAY": "05", "JUN": "06", "JUL": "07", "AGO": "08", "AUG": "08", "SEP": "09",
        "OCT": "10", "NOV": "11", "DIC": "12", "DEC": "12",
        
        # Números
        "01": "01", "02": "02", "03": "03", "04": "04", "05": "05", "06": "06",
        "07": "07", "08": "08", "09": "09", "1": "01", "2": "02", "3": "03",
        "4": "04", "5": "05", "6": "06", "7": "07", "8": "08", "9": "09",
        "10": "10", "11": "11", "12": "12"
    }
    
    mes_encontrado = False
    for mes_nombre, mes_num in meses.items():
        if mes_nombre in nombre_archivo_upper:
            resultado["mes"] = mes_num
            mes_encontrado = True
            break
    
    # Intentar extracción por patrón regex de fecha
    if not mes_encontrado:
        # Buscar patrones como: AAAA-MM, MM-AAAA, DD-MM-AAAA, etc.
        patrones_fecha = [
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})',  # DD/MM/AAAA o DD-MM-AAAA
            r'(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})'   # AAAA/MM/DD o AAAA-MM-DD
        ]
        
        for patron in patrones_fecha:
            match = re.search(patron, nombre_archivo)
            if match:
                # Según el patrón, el mes podría estar en diferentes posiciones
                if len(match.group(3)) == 4 or len(match.group(1)) == 4:
                    # Formato DD/MM/AAAA o AAAA/MM/DD
                    mes = match.group(2).zfill(2)
                else:
                    # Asumimos que es MM en otro formato
                    mes = match.group(1).zfill(2)
                    
                if mes in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
                    resultado["mes"] = mes
                    mes_encontrado = True
                    break
    
    # Si no encontramos el municipio por la estructura, intentar buscar de otra forma
    if not resultado["municipio"]:
        # Buscar si alguna carpeta padre podría ser un municipio
        for i in range(len(componentes) - 2, -1, -1):  # Excluir el archivo mismo
            comp = componentes[i]
            # Si no parece ser una carpeta especial (registro, año, etc.)
            if not comp.startswith("REGISTROS_") and not comp[-4:].isdigit() and comp.upper() == comp:
                resultado["municipio"] = comp.upper()
                break
    
    return resultado

def debe_excluir_archivo(ruta_archivo: str, patrones_exclusion: List[str] = None) -> bool:
    """
    Determina si un archivo debe ser excluido basado en patrones en su nombre.
    
    Args:
        ruta_archivo: Ruta al archivo a verificar.
        patrones_exclusion: Lista de patrones que, si se encuentran en el nombre del archivo,
                            provocarán que sea excluido.
        
    Returns:
        True si el archivo debe ser excluido, False en caso contrario.
    """
    if not patrones_exclusion:
        return False
        
    nombre_archivo = os.path.basename(ruta_archivo).upper()
    
    for patron in patrones_exclusion:
        if patron.upper() in nombre_archivo:
            return True
            
    return False

def listar_archivos_pai(directorio_base: str, patron: str = "*.xls*", patrones_exclusion: List[str] = None) -> list:
    """
    Lista todos los archivos PAI en estructura de directorios por año y municipio.
    
    Args:
        directorio_base: Directorio base con las carpetas de años (REGISTROS_XXXX).
        patron: Patrón de archivo a buscar (default: "*.xls*" para capturar .xlsx, .xlsm, .xls).
        patrones_exclusion: Lista de patrones para excluir archivos (ej: ["COVID"]).
        
    Returns:
        Lista de rutas a archivos PAI encontrados.
    """
    if patrones_exclusion is None:
        patrones_exclusion = ["COVID"]  # Por defecto excluir archivos con COVID en el nombre
        
    archivos_encontrados = []
    archivos_excluidos = []
    extensiones_encontradas = set()
    
    print(f"Buscando archivos en: {directorio_base}")
    print(f"Patrón de búsqueda: {patron}")
    print(f"Se excluirán archivos que contengan: {patrones_exclusion}")
    
    # Intenta listar el contenido del directorio base
    try:
        items_directorio = os.listdir(directorio_base)
        print(f"Encontrados {len(items_directorio)} elementos en el directorio base")
    except Exception as e:
        print(f"Error al listar directorio base: {e}")
        return []
    
    # Buscar carpetas de años (REGISTROS_XXXX)
    for item in items_directorio:
        ruta_item = os.path.join(directorio_base, item)
        
        # Si es un directorio y parece ser una carpeta de registros por año
        if os.path.isdir(ruta_item):
            if item.startswith("REGISTROS_") or item[-4:].isdigit():
                print(f"Procesando carpeta de año: {item}")
                
                # Buscar carpetas de municipios dentro
                try:
                    municipios = os.listdir(ruta_item)
                    print(f"  Encontrados {len(municipios)} municipios")
                except Exception as e:
                    print(f"  Error al listar municipios en {item}: {e}")
                    continue
                
                for municipio in municipios:
                    ruta_municipio = os.path.join(ruta_item, municipio)
                    
                    # Si es un directorio, asumimos que es una carpeta de municipio
                    if os.path.isdir(ruta_municipio):
                        print(f"    Procesando municipio: {municipio}")
                        
                        # Buscar archivos Excel dentro de la carpeta del municipio
                        archivos = glob.glob(os.path.join(ruta_municipio, patron))
                        
                        # Registrar las extensiones encontradas para análisis
                        for archivo in archivos:
                            ext = os.path.splitext(archivo)[1].lower()
                            extensiones_encontradas.add(ext)
                        
                        print(f"      Encontrados {len(archivos)} archivos con patrón {patron}")
                        
                        # Procesar cada archivo encontrado
                        for archivo in archivos:
                            # Verificar si debe ser excluido
                            if debe_excluir_archivo(archivo, patrones_exclusion):
                                archivos_excluidos.append(archivo)
                                print(f"        Excluyendo: {os.path.basename(archivo)}")
                            else:
                                archivos_encontrados.append(archivo)
                                print(f"        Incluido: {os.path.basename(archivo)}")
                    else:
                        print(f"    Omitiendo {municipio}: No es un directorio")
            else:
                print(f"  Omitiendo carpeta: {item} - No parece ser una carpeta de año")
                
                # A pesar de no parecer carpeta de año, verificamos si hay archivos Excel directamente
                archivos_directos = glob.glob(os.path.join(ruta_item, patron))
                if archivos_directos:
                    print(f"    Sin embargo, contiene {len(archivos_directos)} archivos Excel")
                    for archivo in archivos_directos:
                        # Verificar si debe ser excluido
                        if debe_excluir_archivo(archivo, patrones_exclusion):
                            archivos_excluidos.append(archivo)
                        else:
                            archivos_encontrados.append(archivo)
                            ext = os.path.splitext(archivo)[1].lower()
                            extensiones_encontradas.add(ext)
    
    # También buscar archivos en el directorio base (por si acaso)
    archivos_base = glob.glob(os.path.join(directorio_base, patron))
    if archivos_base:
        print(f"Encontrados {len(archivos_base)} archivos en el directorio base")
        for archivo in archivos_base:
            # Verificar si debe ser excluido
            if debe_excluir_archivo(archivo, patrones_exclusion):
                archivos_excluidos.append(archivo)
            else:
                archivos_encontrados.append(archivo)
                ext = os.path.splitext(archivo)[1].lower()
                extensiones_encontradas.add(ext)
    
    print(f"\nResumen de búsqueda:")
    print(f"- Total de archivos encontrados: {len(archivos_encontrados)}")
    print(f"- Total de archivos excluidos: {len(archivos_excluidos)}")
    print(f"- Extensiones encontradas: {', '.join(extensiones_encontradas)}")
    
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
    resultado = []
    vacuna_lower = vacuna.lower().strip()
    
    # Términos a excluir (falsos positivos)
    terminos_excluidos = ["fallecido", "muerto", "observacion", "comentario", "nota"]
    
    # Mapeo específico para fiebre amarilla
    variantes_fiebre_amarilla = [
        "fa", "f.a.", "f. amarilla", "fieb.amar", "f amarilla", 
        "amarilla", "antiamarílica", "antiamarilica"
    ]
    
    # Términos relacionados con la administración de vacunas
    terminos_vacunacion = [
        "dosis", "lote", "jeringa", "fecha aplicacion", "aplicada", 
        "biologico", "vacuna", "inmunizacion", "inmunización"
    ]
    
    # Definir términos de búsqueda según la vacuna
    buscar_terminos = [vacuna_lower]
    if vacuna_lower == "fiebre amarilla":
        buscar_terminos.extend(variantes_fiebre_amarilla)
    
    # Estrategia 1: Buscar coincidencias directas en nombres de columnas
    for col in df.columns:
        col_lower = str(col).lower().strip()
        
        # Verificar si contiene el nombre de la vacuna pero no términos excluidos
        if (any(term in col_lower for term in buscar_terminos) and 
            not any(exc in col_lower for exc in terminos_excluidos)):
            resultado.append(col)
    
    # Si no encontramos nada, intentar estrategias adicionales
    if not resultado:
        # Estrategia 2: Buscar columnas adyacentes
        columnas_list = list(df.columns)
        for i, col in enumerate(columnas_list):
            col_lower = str(col).lower().strip()
            
            # Si esta columna tiene un término de vacuna
            if any(term in col_lower for term in buscar_terminos):
                # Verificar columnas cercanas (3 antes y 3 después)
                rango_inicio = max(0, i - 3)
                rango_fin = min(len(columnas_list), i + 4)
                
                for j in range(rango_inicio, rango_fin):
                    if j == i:  # Saltar la columna actual
                        continue
                    
                    col_cercana = str(columnas_list[j]).lower().strip()
                    # Si la columna cercana tiene términos de vacunación
                    if any(term in col_cercana for term in terminos_vacunacion):
                        resultado.append(columnas_list[j])
        
        # Estrategia 3: Verificar contenido de las columnas
        # Buscar columnas que contengan valores relacionados con aplicación de vacunas
        for col in df.columns:
            try:
                valores = df[col].dropna().astype(str).str.lower().unique()
                if len(valores) < 50:  # Solo columnas con pocos valores únicos
                    # Términos que indicarían que es una columna de vacuna
                    terminos_valores = ['dosis', 'primera', 'segunda', 'refuerzo', 'unica']
                    
                    if any(any(term in str(val) for term in terminos_valores) for val in valores):
                        # Buscar si alguna columna cercana menciona la vacuna
                        idx = list(df.columns).index(col)
                        rango_inicio = max(0, idx - 5)
                        rango_fin = min(len(df.columns), idx + 6)
                        
                        hay_mencion_vacuna = False
                        for i in range(rango_inicio, rango_fin):
                            col_cercana = str(df.columns[i]).lower()
                            if any(term in col_cercana for term in buscar_terminos):
                                hay_mencion_vacuna = True
                                break
                                
                        if hay_mencion_vacuna and col not in resultado:
                            resultado.append(col)
            except:
                continue
    
    # Estrategia 4: Filtrar y priorizar
    if len(resultado) > 0:
        # Verificar si alguna columna parece ser de dosis
        dosis_cols = [col for col in resultado if 'dosis' in str(col).lower()]
        if dosis_cols:
            # Priorizar columnas que mencionan dosis
            return dosis_cols
    
    return resultado

def identificar_columna_dosis(df: pd.DataFrame, columnas_vacuna: List[str]) -> Optional[str]:
    """
    Identifica la columna que contiene la información de dosis.
    
    Args:
        df: DataFrame con los datos del PAI.
        columnas_vacuna: Lista de columnas relacionadas con la vacuna.
        
    Returns:
        Nombre de la columna de dosis o None si no se encuentra.
    """
    # Términos que indican dosis en los nombres de columna
    terminos_dosis = ["dosis", "tipo de dosis", "tipo dosis"]
    
    # Buscar en nombres de columnas primero
    for col in columnas_vacuna:
        if any(term in str(col).lower() for term in terminos_dosis):
            return col
    
    # Si no encontramos en nombres, buscar en contenido
    for col in columnas_vacuna:
        valores = df[col].dropna().astype(str).unique()
        if any(any(term in str(v).lower() for term in ["primera", "segunda", "refuerzo", "unica"]) 
              for v in valores[:30]):
            return col
    
    return None

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

def analizar_estructura_excel(ruta_archivo: str) -> dict:
    """
    Analiza la estructura de un archivo Excel PAI para detectar encabezados y ubicación de datos.
    Específicamente diseñado para la estructura de PAI con encabezados jerárquicos.
    """
    estructura = {
        "hojas": [],
        "contiene_fiebre_amarilla": False,
        "fila_fiebre_amarilla": None,
        "columna_fiebre_amarilla": None,
        "columnas_fiebre_amarilla": [],
        "encabezados_nivel1": {},
        "encabezados_nivel2": {}
    }
    
    try:
        # Detectar extensión para usar el engine correcto
        ext = os.path.splitext(ruta_archivo)[1].lower()
        engine = 'openpyxl' if ext in ['.xlsx', '.xlsm'] else 'xlrd'
        
        # Leer información del archivo
        excel_file = pd.ExcelFile(ruta_archivo, engine=engine)
        estructura["hojas"] = excel_file.sheet_names
        
        # Buscar hoja "Registro Diario" o similar
        hoja_objetivo = None
        for hoja in excel_file.sheet_names:
            if "registro" in hoja.lower() or "diario" in hoja.lower():
                hoja_objetivo = hoja
                break
        
        if not hoja_objetivo and len(excel_file.sheet_names) > 0:
            hoja_objetivo = excel_file.sheet_names[0]
        
        if not hoja_objetivo:
            return estructura
        
        # Leer las primeras 3 filas para análisis
        df_encabezados = pd.read_excel(
            ruta_archivo,
            sheet_name=hoja_objetivo,
            engine=engine,
            header=None,
            nrows=3
        )
        
        # Guardar encabezados nivel 1 y 2
        estructura["encabezados_nivel1"] = df_encabezados.iloc[0].to_dict()
        estructura["encabezados_nivel2"] = df_encabezados.iloc[1].to_dict()
        
        # Buscar "Fiebre amarilla" en la primera fila
        fiebre_amarilla_encontrada = False
        col_inicio_fa = None
        for j, valor in enumerate(df_encabezados.iloc[0]):
            if pd.notna(valor) and "fiebre amarilla" in str(valor).lower():
                fiebre_amarilla_encontrada = True
                col_inicio_fa = j
                estructura["contiene_fiebre_amarilla"] = True
                estructura["fila_fiebre_amarilla"] = 0  # Fila 1 (índice 0)
                estructura["columna_fiebre_amarilla"] = j
                break
        
        # Si encontramos Fiebre Amarilla, identificar todas sus columnas
        if fiebre_amarilla_encontrada and col_inicio_fa is not None:
            # Determinar hasta dónde llegan las columnas de Fiebre Amarilla
            # (hasta que encontremos otro valor no nulo en la fila 1)
            col_fin_fa = col_inicio_fa + 1
            while col_fin_fa < len(df_encabezados.columns):
                if pd.notna(df_encabezados.iloc[0, col_fin_fa]):
                    break
                col_fin_fa += 1
            
            # Recopilar todas las columnas de Fiebre Amarilla con sus subcategorías
            for j in range(col_inicio_fa, col_fin_fa):
                if j < len(df_encabezados.columns):
                    subcategoria = df_encabezados.iloc[1, j]
                    if pd.notna(subcategoria):
                        estructura["columnas_fiebre_amarilla"].append({
                            "indice": j,
                            "subcategoria": str(subcategoria)
                        })
        
        return estructura
    
    except Exception as e:
        print(f"Error al analizar estructura: {str(e)}")
        return estructura
    
def extraer_columnas_fiebre_amarilla(df: pd.DataFrame, estructura: dict) -> List[str]:
    """
    Extrae las columnas específicas relacionadas con Fiebre Amarilla basadas
    en el análisis de estructura.
    """
    columnas = []
    
    # Si se encontró Fiebre Amarilla en la estructura
    if estructura["contiene_fiebre_amarilla"] and estructura["columnas_fiebre_amarilla"]:
        # Verificar si el dataframe ya tiene encabezados normalizados
        if estructura["fila_fiebre_amarilla"] == 0:  # Los encabezados ya están normalizados
            # Encontrar columnas con términos clave para Fiebre Amarilla
            for info_col in estructura["columnas_fiebre_amarilla"]:
                indice = info_col["indice"]
                if indice < len(df.columns):
                    columnas.append(df.columns[indice])
    
    # Si no encontramos columnas con el método estructurado, usar búsqueda por texto
    if not columnas:
        # Buscar en nombres de columnas
        for col in df.columns:
            col_lower = str(col).lower()
            if "fiebre" in col_lower or "amarilla" in col_lower or "fa" == col_lower or "f.a." in col_lower:
                columnas.append(col)
    
    return columnas