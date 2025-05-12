"""
Módulo para procesar y consolidar archivos PAI de vacunación.
"""
import os
import glob
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np
from .utils import (
    extraer_nombre_municipio,
    extraer_info_ruta,
    listar_archivos_pai,
    encontrar_columnas_vacuna,
    identificar_columna_dosis,
    clasificar_grupo_etario,
    limpiar_texto,
    extraer_vereda_de_direccion
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
        
        Args:
            ruta_archivo: Ruta al archivo XLSM.
            
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
            
            # Cargar la hoja de Registro Diario
            df = pd.read_excel(
                ruta_archivo,
                sheet_name="Registro Diario",
                engine="openpyxl",
                header=1  # La segunda fila tiene los encabezados reales
            )
            
            # Columnas base que siempre queremos extraer
            columnas_base = [
                "Consecutivo", 
                "Fecha de atención formato de fecha en números (día/mes/año)*", 
                "Tipo de identificación*", 
                "Número de identificación*", 
                "Primer nombre*", 
                "Primer apellido*", 
                "AÑOS", 
                "MESES", 
                "DIAS", 
                "Sexo*", 
                "Departamento de residencia*", 
                "Municipio de residencia*", 
                "Comuna/Localidad",
                "Área*",
                "Dirección con nomenclatura"
            ]
            
            # Verificar que las columnas base existen
            columnas_existentes = [col for col in columnas_base if col in df.columns]
            if len(columnas_existentes) != len(columnas_base):
                columnas_faltantes = set(columnas_base) - set(columnas_existentes)
                self.advertencias.append(
                    f"Archivo {os.path.basename(ruta_archivo)}: "
                    f"faltan columnas necesarias: {columnas_faltantes}"
                )
            
            # Encontrar columnas de la vacuna
            columnas_vacuna = encontrar_columnas_vacuna(df, self.vacuna)
            if not columnas_vacuna:
                self.advertencias.append(
                    f"Archivo {os.path.basename(ruta_archivo)}: "
                    f"no se encontraron columnas para la vacuna '{self.vacuna}'"
                )
                return pd.DataFrame()
            
            # Columnas a extraer
            columnas_seleccionadas = columnas_existentes + columnas_vacuna
            
            # Filtrar solo las columnas necesarias
            df_filtrado = df[columnas_seleccionadas].copy()
            
            # Limpiar datos
            # Eliminar filas con valores no válidos en la fecha
            fecha_col = "Fecha de atención formato de fecha en números (día/mes/año)*"
            df_filtrado = df_filtrado[df_filtrado[fecha_col].notna() & (df_filtrado[fecha_col] != "fin")]
            
            # Convertir fechas
            df_filtrado["Fecha"] = pd.to_datetime(
                df_filtrado[fecha_col],
                format="%m/%d/%y",
                errors="coerce"
            )
            
            # Identificar columna de dosis
            col_dosis = identificar_columna_dosis(df_filtrado, columnas_vacuna)
            
            # Agregar columnas para lugar de vacunación
            df_filtrado["Municipio_Vacunacion"] = municipio
            df_filtrado["Año_Registro"] = info_ruta.get("año")
            df_filtrado["Mes_Registro"] = info_ruta.get("mes")
            
            # Procesar información de residencia
            df_filtrado["Departamento_Residencia"] = None
            df_filtrado["Municipio_Residencia"] = None
            df_filtrado["Localidad_Residencia"] = None
            df_filtrado["Area_Residencia"] = None
            
            # Mapear columnas de residencia
            col_mapping = {
                "Departamento de residencia*": "Departamento_Residencia",
                "Municipio de residencia*": "Municipio_Residencia",
                "Comuna/Localidad": "Localidad_Residencia",
                "Área*": "Area_Residencia"
            }
            
            # Copiar valores normalizados
            for col_orig, col_nuevo in col_mapping.items():
                if col_orig in df_filtrado.columns:
                    df_filtrado[col_nuevo] = df_filtrado[col_orig].apply(
                        lambda x: limpiar_texto(x) if pd.notna(x) else None
                    )
            
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
                
            if not vereda_extraida and "Dirección con nomenclatura" in df_filtrado.columns:
                # Intentar extraer vereda de la dirección
                df_filtrado["Vereda_Residencia"] = df_filtrado["Dirección con nomenclatura"].apply(
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
            
            # Clasificar por grupo etario
            if "AÑOS" in df_filtrado.columns:
                df_filtrado["Grupo_Etario"] = df_filtrado["AÑOS"].apply(clasificar_grupo_etario)
            
            # Actualizar contador de registros procesados
            self.archivos_procesados += 1
            self.registros_totales += len(df_filtrado)
            
            return df_filtrado
            
        except Exception as e:
            self.advertencias.append(f"Error al procesar {os.path.basename(ruta_archivo)}: {str(e)}")
            return pd.DataFrame()
    
    def consolidar_archivos(self, directorio: str, patron: str = "*.xlsm") -> dict:
        """
        Consolida datos de todos los archivos PAI en un directorio.
        
        Args:
            directorio: Carpeta base donde están los registros (contiene carpetas REGISTROS_XXXX).
            patron: Patrón para identificar archivos (default: "*.xlsm").
            
        Returns:
            Diccionario con DataFrames consolidados según el tipo seleccionado.
        """
        # Reiniciar contadores
        self.archivos_procesados = 0
        self.registros_totales = 0
        self.advertencias = []
        
        # Listar archivos usando la nueva función que maneja la estructura jerárquica
        archivos = listar_archivos_pai(directorio, patron)
        
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