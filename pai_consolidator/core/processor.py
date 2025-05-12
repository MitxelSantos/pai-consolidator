"""
Módulo para procesar y consolidar archivos PAI de vacunación.
"""
import os
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple, Optional, Set
from datetime import datetime
import traceback
from .utils import (
    listar_archivos_pai,
    extraer_fecha_de_archivo,
    extraer_municipio_de_ruta,
    analizar_estructura_excel,
    leer_excel_con_estructura,
    clasificar_grupo_etario,
    limpiar_texto,
    normalizar_nombres_columnas
)

class PaiProcessor:
    """
    Clase para procesar archivos PAI de vacunación.
    """
    
    def __init__(self, modo_detallado: bool = False, ignorar_errores: bool = False):
        """
        Inicializa el procesador de archivos PAI.
        
        Args:
            modo_detallado: Si True, muestra información detallada durante el procesamiento.
            ignorar_errores: Si True, continúa procesando aunque haya archivos con errores.
        """
        self.modo_detallado = modo_detallado
        self.ignorar_errores = ignorar_errores
        self.archivos_procesados = 0
        self.registros_totales = 0
        self.advertencias = []
        self.datos_consolidados = None
        self.info_archivos = []
    
    def _agregar_info_archivo(self, ruta: str, registros: int, advertencias: List[str]):
        """
        Agrega información sobre un archivo procesado.
        
        Args:
            ruta: Ruta del archivo.
            registros: Número de registros procesados.
            advertencias: Lista de advertencias durante el procesamiento.
        """
        self.info_archivos.append({
            "ruta": ruta,
            "nombre": os.path.basename(ruta),
            "registros": registros,
            "advertencias": advertencias,
            "fecha_proceso": datetime.now()
        })
    
    def procesar_archivo(self, ruta_archivo: str) -> pd.DataFrame:
        """
        Procesa un archivo PAI y extrae todos los datos.
        
        Args:
            ruta_archivo: Ruta al archivo XLSM/XLSX.
            
        Returns:
            DataFrame con los datos procesados.
        """
        advertencias_archivo = []
        try:
            # Extraer información básica del archivo
            municipio = extraer_municipio_de_ruta(ruta_archivo)
            info_fecha = extraer_fecha_de_archivo(ruta_archivo)
            
            if self.modo_detallado:
                print(f"Procesando archivo: {os.path.basename(ruta_archivo)}")
                print(f"  - Municipio identificado: {municipio}")
                print(f"  - Año: {info_fecha['año'] or 'No identificado'}")
                print(f"  - Mes: {info_fecha['mes'] or 'No identificado'}")
            
            # Analizar estructura del archivo
            estructura = analizar_estructura_excel(ruta_archivo, forzar_jerarquico=True)
            
            if estructura["error"]:
                advertencias_archivo.append(f"Error al analizar estructura: {estructura['error']}")
                if self.modo_detallado:
                    print(f"  - {advertencias_archivo[-1]}")
            
            if self.modo_detallado:
                if estructura["modo_jerarquico"]:
                    print(f"  - Archivo con estructura jerárquica detectada")
                    print(f"  - Categorías principales: {list(estructura['categorias_detectadas'].keys())}")
                else:
                    print(f"  - Archivo con estructura plana (no jerárquica)")
                    print(f"  - Encabezado en fila {estructura['filas_encabezado'][0] + 1}")
            
            # Leer el archivo con la estructura adecuada
            df, es_jerarquico = leer_excel_con_estructura(ruta_archivo, estructura)
            
            if self.modo_detallado:
                print(f"  - Archivo leído exitosamente: {len(df)} filas, {len(df.columns)} columnas")
            
            # Normalizar nombres de columnas (especialmente para encabezados jerárquicos)
            df = normalizar_nombres_columnas(df)
            
            if self.modo_detallado:
                print(f"  - Nombres de columnas normalizados")
                
            # Añadir columnas de información adicional
            df["Municipio_Vacunacion"] = municipio
            df["Año_Registro"] = info_fecha.get("año")
            df["Mes_Registro"] = info_fecha.get("mes")
            df["Archivo_Origen"] = os.path.basename(ruta_archivo)
            
            # Intentar detectar y limpiar información clave
            # 1. Fecha de atención/aplicación
            columnas_fecha = []
            for col in df.columns:
                # Comprobar si la columna es una tupla (encabezados jerárquicos) o string
                if isinstance(col, tuple):
                    # Para encabezados jerárquicos, verificar ambos niveles de encabezado
                    col_str = " ".join([str(parte) for parte in col if pd.notna(parte)])
                    if "fecha" in col_str.lower() and "atencion" in col_str.lower():
                        columnas_fecha.append(col)
                else:
                    # Para encabezados simples (strings)
                    if "fecha" in str(col).lower() and "atencion" in str(col).lower():
                        columnas_fecha.append(col)

            if columnas_fecha:
                col_fecha = columnas_fecha[0]
                df = df[df[col_fecha].notna()]  # Eliminar filas sin fecha
                try:
                    df["Fecha"] = pd.to_datetime(df[col_fecha], errors="coerce")
                except Exception as e:
                    advertencias_archivo.append(f"Error al convertir fechas: {str(e)}")
                    if self.modo_detallado:
                        print(f"  - {advertencias_archivo[-1]}")
            else:
                # Si no hay columna de fecha, usar la fecha del archivo
                if info_fecha["año"] and info_fecha["mes"]:
                    fecha_str = f"{info_fecha['año']}-{info_fecha['mes']}-01"
                    df["Fecha"] = pd.to_datetime(fecha_str)
                else:
                    df["Fecha"] = pd.NaT
            
            # 2. Datos de identificación personal
            columnas_id = {
                "Tipo_Identificacion": ["tipo", "identificacion"],
                "Numero_Identificacion": ["numero", "identificacion", "cedula"],
                "Primer_Nombre": ["primer", "nombre"],
                "Primer_Apellido": ["primer", "apellido"],
                "Sexo": ["sexo", "genero"]
            }
            
            for col_norm, términos in columnas_id.items():
                for col in df.columns:
                    # Comprobar si es una tupla o string
                    if isinstance(col, tuple):
                        col_str = " ".join([str(parte) for parte in col if pd.notna(parte)]).lower()
                    else:
                        col_str = str(col).lower()
                        
                    if all(term in col_str for term in términos):
                        df[col_norm] = df[col]
                        break
            
            # 3. Datos de residencia
            columnas_residencia = {
                "Departamento_Residencia": ["departamento", "residencia"],
                "Municipio_Residencia": ["municipio", "residencia"],
                "Localidad_Residencia": ["comuna", "localidad", "barrio"]
            }
            
            for col_norm, términos in columnas_residencia.items():
                for col in df.columns:
                    # Comprobar si es una tupla o string
                    if isinstance(col, tuple):
                        col_str = " ".join([str(parte) for parte in col if pd.notna(parte)]).lower()
                    else:
                        col_str = str(col).lower()
                        
                    if all(term in col_str for term in términos):
                        df[col_norm] = df[col].apply(lambda x: limpiar_texto(x) if pd.notna(x) else None)
                        break
            
            # 4. Clasificar por grupo etario
            columnas_edad = []
            for col in df.columns:
                # Comprobar si es una tupla o string
                if isinstance(col, tuple):
                    col_str = " ".join([str(parte) for parte in col if pd.notna(parte)]).lower()
                else:
                    col_str = str(col).lower()
                    
                if "año" in col_str or "edad" in col_str:
                    columnas_edad.append(col)
                    
            if columnas_edad:
                col_edad = columnas_edad[0]
                try:
                    df["Edad_Num"] = pd.to_numeric(df[col_edad], errors="coerce")
                    df["Grupo_Etario"] = df["Edad_Num"].apply(clasificar_grupo_etario)
                except Exception as e:
                    advertencias_archivo.append(f"Error al calcular grupos etarios: {str(e)}")
                    if self.modo_detallado:
                        print(f"  - {advertencias_archivo[-1]}")
                    df["Grupo_Etario"] = "No especificado"
            else:
                df["Grupo_Etario"] = "No especificado"
            
            # Actualizar contador de registros
            registros = len(df)
            self.archivos_procesados += 1
            self.registros_totales += registros
            
            # Agregar información del archivo procesado
            self._agregar_info_archivo(ruta_archivo, registros, advertencias_archivo)
            
            if self.modo_detallado:
                print(f"  - Procesamiento exitoso: {registros} registros")
            
            return df
            
        except Exception as e:
            error_traceback = traceback.format_exc()
            error_msg = f"Error al procesar {os.path.basename(ruta_archivo)}: {str(e)}"
            advertencias_archivo.append(error_msg)
            self.advertencias.append(error_msg)
            
            self._agregar_info_archivo(ruta_archivo, 0, advertencias_archivo)
            
            if self.modo_detallado:
                print(f"  - {error_msg}")
                print(f"  - Traceback: {error_traceback}")
            
            return pd.DataFrame()
    
    def consolidar_archivos(self, directorio: str, patron: str = "*.xls*", 
                           excluir_patrones: List[str] = None) -> pd.DataFrame:
        """
        Consolida todos los datos de archivos PAI en un directorio.
        
        Args:
            directorio: Carpeta base donde están los registros.
            patron: Patrón para identificar archivos (default: "*.xls*").
            excluir_patrones: Lista de patrones a excluir (ej. ["COVID", "respaldo"])
            
        Returns:
            DataFrame consolidado con todos los datos.
        """
        # Reiniciar contadores
        self.archivos_procesados = 0
        self.registros_totales = 0
        self.advertencias = []
        self.info_archivos = []
        
        # Listar archivos
        if excluir_patrones is None:
            excluir_patrones = ["COVID", "covid", "respaldo", "backup"]
            
        archivos = listar_archivos_pai(directorio, patron, excluir_patrones)
        
        if not archivos:
            self.advertencias.append(f"No se encontraron archivos que coincidan con {patron} en {directorio}")
            print(f"No se encontraron archivos que coincidan con {patron} en {directorio}")
            return pd.DataFrame()
        
        print(f"Se encontraron {len(archivos)} archivos para procesar...")
        
        # Lista para almacenar DataFrames
        dfs = []
        
        # Procesar cada archivo
        for i, archivo in enumerate(archivos, 1):
            print(f"\nProcesando archivo {i}/{len(archivos)}: {os.path.basename(archivo)}")
            try:
                df = self.procesar_archivo(archivo)
                if not df.empty:
                    dfs.append(df)
                elif not self.ignorar_errores:
                    print(f"  - El archivo no produjo datos válidos")
                    if not self.ignorar_errores:
                        print("  - Deteniendo procesamiento. Use --ignorar-errores para continuar.")
                        break
            except Exception as e:
                error_msg = f"Error al procesar {os.path.basename(archivo)}: {str(e)}"
                self.advertencias.append(error_msg)
                print(f"  - {error_msg}")
                if not self.ignorar_errores:
                    print("  - Deteniendo procesamiento. Use --ignorar-errores para continuar.")
                    break
        
        # Combinar todos los DataFrames
        if dfs:
            print(f"\nCombinando {len(dfs)} archivos procesados...")
            df_combinado = pd.concat(dfs, ignore_index=True)
            print(f"Consolidación completada: {len(df_combinado)} registros totales")
            
            # Guardar resultados para uso posterior
            self.datos_consolidados = df_combinado
            
            # Mostrar advertencias
            if self.advertencias:
                print("\nAdvertencias durante el procesamiento:")
                for i, adv in enumerate(self.advertencias[:10], 1):
                    print(f"{i}. {adv}")
                
                if len(self.advertencias) > 10:
                    print(f"... y {len(self.advertencias) - 10} advertencias más")
            
            return df_combinado
        else:
            print("No se pudo procesar ningún archivo correctamente.")
            return pd.DataFrame()
    
    def filtrar_por_vacuna(self, vacuna: str = "Fiebre amarilla", 
                      tipo_consolidado: str = "vacunacion") -> dict:
        """
        Filtra los datos consolidados por una vacuna específica.
        
        Args:
            vacuna: Nombre de la vacuna a filtrar.
            tipo_consolidado: Tipo de consolidado ("residencia", "vacunacion", "ambos").
            
        Returns:
            Diccionario con DataFrames filtrados para la vacuna específica.
        """
        if self.datos_consolidados is None or self.datos_consolidados.empty:
            print("No hay datos consolidados para filtrar. Ejecute consolidar_archivos primero.")
            return {}
        
        df = self.datos_consolidados.copy()
        
        # Identificar columnas relacionadas con la vacuna
        vacuna_lower = vacuna.lower()
        columnas_vacuna = []
        
        for col in df.columns:
            # Comprobar si es una tupla o string
            if isinstance(col, tuple):
                col_str = " ".join([str(parte) for parte in col if pd.notna(parte)]).lower()
            else:
                col_str = str(col).lower()
                
            if vacuna_lower in col_str:
                columnas_vacuna.append(col)
        
        if not columnas_vacuna:
            print(f"No se encontraron columnas relacionadas con '{vacuna}' en los datos consolidados.")
            return {}
        
        print(f"Se encontraron {len(columnas_vacuna)} columnas relacionadas con '{vacuna}':")
        for col in columnas_vacuna[:10]:
            print(f"  - {col}")
        if len(columnas_vacuna) > 10:
            print(f"  ... y {len(columnas_vacuna) - 10} más")
        
        # Filtrar registros que tienen datos en alguna de estas columnas
        tiene_datos = df[columnas_vacuna].notna().any(axis=1)
        df_filtrado = df[tiene_datos].copy()
        
        # Intentar identificar columnas de dosis
        columnas_dosis = []
        for col in columnas_vacuna:
            if isinstance(col, tuple):
                col_str = " ".join([str(parte) for parte in col if pd.notna(parte)]).lower()
            else:
                col_str = str(col).lower()
                
            if "dosis" in col_str:
                columnas_dosis.append(col)
        
        if columnas_dosis:
            col_dosis = columnas_dosis[0]
            print(f"Columna de dosis identificada: {col_dosis}")
            
            # Marcar si está vacunado
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
            print("No se identificó columna específica de dosis")
            # Usar cualquier dato en columnas de vacuna como indicador
            df_filtrado["Vacunado"] = df_filtrado[columnas_vacuna].notna().any(axis=1)
            df_filtrado["Tipo_Dosis"] = None
            df_filtrado["Es_Primera_Dosis"] = 0
            df_filtrado["Es_Segunda_Dosis"] = 0
            df_filtrado["Es_Refuerzo"] = 0
            df_filtrado["Es_Unica_Dosis"] = 0
        
        # Preparar resultado según tipo de consolidado
        resultado = {}
        
        if tipo_consolidado == "vacunacion" or tipo_consolidado == "ambos":
            # Consolidado por lugar de vacunación
            df_vacunacion = df_filtrado.copy()
            # Ordenar columnas para priorizar datos de vacunación
            cols_vacunacion = ["Municipio_Vacunacion", "Año_Registro", "Mes_Registro"]
            cols_resto = [col for col in df_vacunacion.columns if col not in cols_vacunacion]
            df_vacunacion = df_vacunacion[cols_vacunacion + cols_resto]
            resultado["vacunacion"] = df_vacunacion
            
        if tipo_consolidado == "residencia" or tipo_consolidado == "ambos":
            # Consolidado por lugar de residencia
            df_residencia = df_filtrado.copy()
            # Ordenar columnas para priorizar datos de residencia
            cols_residencia = [col for col in df_residencia.columns 
                            if "Residencia" in col or "Departamento_" in col or "Municipio_" in col]
            cols_resto = [col for col in df_residencia.columns if col not in cols_residencia]
            df_residencia = df_residencia[cols_residencia + cols_resto]
            resultado["residencia"] = df_residencia
        
        return resultado
    
    def generar_estadisticas(self, df: pd.DataFrame, tipo: str = "vacunacion") -> Dict[str, Any]:
        """
        Genera estadísticas para los datos filtrados.
        
        Args:
            df: DataFrame con datos filtrados.
            tipo: Tipo de consolidado ("residencia" o "vacunacion").
            
        Returns:
            Diccionario con estadísticas.
        """
        if df.empty:
            return {
                "total_registros": 0,
                "mensaje": "No hay datos para generar estadísticas"
            }
        
        estadisticas = {
            "total_registros": len(df),
            "registros_por_año": {},
            "registros_por_mes": {},
            "distribucion_grupo_etario": {},
            "tipo_consolidado": tipo
        }
        
        # Estadísticas por año
        if "Año_Registro" in df.columns:
            for año, grupo in df.groupby("Año_Registro"):
                if pd.notna(año):
                    estadisticas["registros_por_año"][año] = len(grupo)
        
        # Estadísticas por mes
        if "Mes_Registro" in df.columns:
            for mes, grupo in df.groupby("Mes_Registro"):
                if pd.notna(mes):
                    estadisticas["registros_por_mes"][mes] = len(grupo)
        
        # Estadísticas por grupo etario
        if "Grupo_Etario" in df.columns:
            for grupo, conteo in df["Grupo_Etario"].value_counts().items():
                estadisticas["distribucion_grupo_etario"][grupo] = int(conteo)
        
        # Estadísticas específicas según tipo de consolidado
        if tipo == "vacunacion":
            # Estadísticas por municipio de vacunación
            estadisticas["municipios_vacunacion"] = {}
            if "Municipio_Vacunacion" in df.columns:
                for muni, conteo in df["Municipio_Vacunacion"].value_counts().items():
                    if pd.notna(muni):
                        estadisticas["municipios_vacunacion"][muni] = int(conteo)
                estadisticas["total_municipios"] = len(estadisticas["municipios_vacunacion"])
        
        elif tipo == "residencia":
            # Estadísticas por departamento y municipio de residencia
            estadisticas["departamentos_residencia"] = {}
            estadisticas["municipios_residencia"] = {}
            
            if "Departamento_Residencia" in df.columns:
                for depto, conteo in df["Departamento_Residencia"].value_counts().items():
                    if pd.notna(depto):
                        estadisticas["departamentos_residencia"][depto] = int(conteo)
                estadisticas["total_departamentos"] = len(estadisticas["departamentos_residencia"])
            
            if "Municipio_Residencia" in df.columns:
                for muni, conteo in df["Municipio_Residencia"].value_counts().items():
                    if pd.notna(muni):
                        estadisticas["municipios_residencia"][muni] = int(conteo)
                estadisticas["total_municipios_residencia"] = len(estadisticas["municipios_residencia"])
        
        # Estadísticas de vacunación
        if "Vacunado" in df.columns:
            total_vacunados = df["Vacunado"].sum()
            estadisticas["total_vacunados"] = int(total_vacunados)
            
            # Desglose por tipo de dosis
            dosis_cols = {
                "Es_Primera_Dosis": "Primera dosis",
                "Es_Segunda_Dosis": "Segunda dosis",
                "Es_Refuerzo": "Refuerzo",
                "Es_Unica_Dosis": "Dosis única"
            }
            
            estadisticas["tipos_dosis"] = {}
            for col, nombre in dosis_cols.items():
                if col in df.columns:
                    total = df[col].sum()
                    if total > 0:
                        porcentaje = total/total_vacunados*100 if total_vacunados > 0 else 0
                        estadisticas["tipos_dosis"][nombre] = {
                            "total": int(total),
                            "porcentaje": round(porcentaje, 1)
                        }
        
        return estadisticas