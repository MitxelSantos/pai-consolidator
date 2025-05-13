"""
Interfaz de línea de comandos para el consolidador de datos PAI.
"""
import os
import argparse
import sys
import json
from datetime import datetime
import pandas as pd
from .core.processor import PaiProcessor

def main():
    """
    Punto de entrada principal para la línea de comandos.
    """
    parser = argparse.ArgumentParser(
        description="Consolidador de datos PAI para vacunación",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--directorio", "-d",
        required=True,
        help="Directorio con los archivos PAI (.xlsm, .xlsx)"
    )
    
    parser.add_argument(
        "--salida", "-o",
        default="data\\output",
        help="Directorio donde guardar el archivo consolidado"
    )
    
    parser.add_argument(
        "--vacuna", "-v",
        default="Fiebre amarilla",
        help="Vacuna a analizar (ej: 'Fiebre amarilla', 'Polio')"
    )
    
    parser.add_argument(
        "--patron", "-p",
        help="Patrón personalizado para buscar archivos (por defecto: todos los formatos Excel)",
        default=None
    )
    
    parser.add_argument(
        "--excluir", "-x",
        default="COVID,covid,respaldo,backup",
        help="Patrones a excluir separados por coma"
    )
    
    parser.add_argument(
        "--año", "-a",
        help="Filtrar archivos por año específico (ej: 2025)"
    )
    
    parser.add_argument(
        "--mes", "-m",
        help="Filtrar archivos por mes específico (ej: 04 para abril)"
    )
    
    parser.add_argument(
        "--tipo-consolidado", "-t",
        choices=["residencia", "vacunacion", "ambos"],
        default="ambos",
        help="Tipo de consolidado: por lugar de residencia de la persona o por lugar de vacunación"
    )
    
    parser.add_argument(
        "--modo", "-M",
        choices=["todo", "consolidar", "filtrar"],
        default="todo",
        help="Modo de operación: consolidar todos los datos, filtrar datos existentes o ambos"
    )
    
    parser.add_argument(
        "--archivo-consolidado", "-ac",
        help="Ruta a un archivo consolidado previamente generado (para modo 'filtrar')"
    )
    
    parser.add_argument(
        "--detalles", "-i",
        action="store_true",
        help="Mostrar información detallada de cada archivo procesado"
    )
    
    parser.add_argument(
        "--ignorar-errores", "-e",
        action="store_true",
        help="Continuar procesando archivos aunque alguno falle"
    )
    
    parser.add_argument(
        "--estadisticas", "-s",
        action="store_true",
        help="Generar archivo JSON con estadísticas"
    )

    # Añadir este argumento después de los argumentos existentes en la función main()
    parser.add_argument(
        "--paralelo", "-P",
        action="store_true",
        help="Usar procesamiento paralelo para mejorar rendimiento con múltiples archivos"
    )
    
    args = parser.parse_args()
    
    # Verificar que el directorio de entrada existe (si es requerido)
    if args.modo in ["todo", "consolidar"] and not os.path.isdir(args.directorio):
        print(f"Error: El directorio {args.directorio} no existe")
        sys.exit(1)
    
    # Verificar archivo consolidado (si es requerido)
    if args.modo == "filtrar" and not args.archivo_consolidado:
        print("Error: Se requiere especificar --archivo-consolidado en modo 'filtrar'")
        sys.exit(1)
    
    if args.modo == "filtrar" and not os.path.isfile(args.archivo_consolidado):
        print(f"Error: El archivo consolidado {args.archivo_consolidado} no existe")
        sys.exit(1)
    
    # Crear directorio de salida si no existe
    os.makedirs(args.salida, exist_ok=True)
    
    print("\n= PAI Consolidator =")
    print(f"Modo: {args.modo}")

    if args.paralelo:
        print("Procesamiento: Paralelo (multi-core)")
    else:
        print("Procesamiento: Secuencial (single-core)")
    
    if args.modo in ["todo", "consolidar"]:
        print(f"Directorio de entrada: {args.directorio}")
        print(f"Patrón de archivos: {args.patron}")
        print(f"Patrones a excluir: {args.excluir}")
    
    if args.modo in ["todo", "filtrar"]:
        print(f"Vacuna a analizar: {args.vacuna}")
        print(f"Tipo de consolidado: {args.tipo_consolidado}")
    
    print(f"Directorio de salida: {args.salida}")
    
    if args.año:
        print(f"Filtro por año: {args.año}")
    if args.mes:
        print(f"Filtro por mes: {args.mes}")
    
    print("-" * 60)
    
    # Crear procesador
    processor = PaiProcessor(
        modo_detallado=args.detalles,
        ignorar_errores=args.ignorar_errores
    )
    
    # Ejecutar según el modo
    if args.modo in ["todo", "consolidar"]:
        # Consolidar datos
        print("\nFase 1: Consolidando todos los datos...")
        excluir_patrones = args.excluir.split(",") if args.excluir else None
        
        df_consolidado = processor.consolidar_archivos(
            args.directorio,
            args.patron,
            excluir_patrones,
            usar_paralelo=args.paralelo
        )
        
        if df_consolidado.empty:
            print("\nNo se pudieron consolidar los datos.")
            sys.exit(1)
        
        # Guardar consolidado general
        fecha_actual = datetime.now().strftime("%Y%m%d")
        ruta_consolidado = os.path.join(args.salida, f"Consolidado_General_{fecha_actual}.xlsx")
        
        try:
            df_consolidado.to_excel(ruta_consolidado, index=False)
            print(f"\nArchivo consolidado general guardado en: {ruta_consolidado}")
        except Exception as e:
            print(f"Error al guardar archivo consolidado general: {str(e)}")
            # Intentar guardar en CSV
            ruta_csv = os.path.join(args.salida, f"Consolidado_General_{fecha_actual}.csv")
            df_consolidado.to_csv(ruta_csv, index=False)
            print(f"Se ha guardado una versión CSV en: {ruta_csv}")
    
    if args.modo == "filtrar":
        # Cargar datos consolidados previamente
        print(f"\nCargando datos consolidados desde: {args.archivo_consolidado}")
        try:
            extension = os.path.splitext(args.archivo_consolidado)[1].lower()
            if extension == ".csv":
                df_consolidado = pd.read_csv(args.archivo_consolidado)
            else:
                df_consolidado = pd.read_excel(args.archivo_consolidado)
            
            processor.datos_consolidados = df_consolidado
            print(f"Datos cargados: {len(df_consolidado)} registros")
        except Exception as e:
            print(f"Error al cargar archivo consolidado: {str(e)}")
            sys.exit(1)
    
    if args.modo in ["todo", "filtrar"]:
        # Filtrar por vacuna específica
        print(f"\nFase 2: Filtrando datos para vacuna '{args.vacuna}'...")
        resultado_filtrado = processor.filtrar_por_vacuna(
            args.vacuna,
            args.tipo_consolidado
        )
        
        if not resultado_filtrado:
            print(f"\nNo se encontraron datos para la vacuna '{args.vacuna}'.")
            sys.exit(1)
        
        # Guardar resultados filtrados
        fecha_actual = datetime.now().strftime("%Y%m%d")
        
        for tipo, df in resultado_filtrado.items():
            # Filtrar por año y mes si se especifican
            if args.año or args.mes:
                print(f"\nAplicando filtros: Año={args.año or 'Todos'}, Mes={args.mes or 'Todos'}")
                df_original = df.copy()
                
                if args.año and "Año_Registro" in df.columns:
                    df = df[df["Año_Registro"] == args.año]
                
                if args.mes and "Mes_Registro" in df.columns:
                    df = df[df["Mes_Registro"] == args.mes]
                
                print(f"Filtrado: {len(df)} de {len(df_original)} registros")
                
                if df.empty:
                    print(f"Advertencia: No hay datos para el filtro año={args.año}, mes={args.mes}")
                    continue
            
            # Nombre de archivo
            nombre_archivo = f"Consolidado_{args.vacuna.replace(' ', '_')}_{tipo}_{fecha_actual}.xlsx"
            ruta_salida = os.path.join(args.salida, nombre_archivo)
            
            # Generar archivo Excel más completo
            try:
                # Crear un escritor de Excel con formato mejorado
                with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
                    # Hoja principal de datos
                    df.to_excel(writer, sheet_name='Datos', index=False)
                    
                    # Generar estadísticas
                    estadisticas = processor.generar_estadisticas(df, tipo)
                    
                    # Hoja de resumen
                    resumen = []
                    resumen.append(["Fecha de generación", datetime.now().strftime("%Y-%m-%d %H:%M")])
                    resumen.append(["Vacuna analizada", args.vacuna])
                    resumen.append(["Tipo de consolidado", tipo])
                    resumen.append(["Total de registros", len(df)])
                    
                    if tipo == "vacunacion":
                        n_municipios = estadisticas.get("total_municipios", 0)
                        resumen.append(["Municipios de vacunación", n_municipios])
                        # Incluir conteo por municipio
                        resumen.append([])
                        resumen.append(["Distribución por municipio de vacunación", ""])
                        for municipio, count in sorted(estadisticas.get("municipios_vacunacion", {}).items(), 
                                                      key=lambda x: x[1], reverse=True)[:20]:
                            resumen.append([municipio, count])
                    else:
                        n_deptos = estadisticas.get("total_departamentos", 0)
                        n_municipios = estadisticas.get("total_municipios_residencia", 0)
                        resumen.append(["Departamentos de residencia", n_deptos])
                        resumen.append(["Municipios de residencia", n_municipios])
                        # Incluir conteo por departamento
                        resumen.append([])
                        resumen.append(["Distribución por departamento de residencia", ""])
                        for depto, count in sorted(estadisticas.get("departamentos_residencia", {}).items(), 
                                                 key=lambda x: x[1], reverse=True)[:20]:
                            resumen.append([depto, count])
                    
                    if "total_vacunados" in estadisticas:
                        total_vacunados = estadisticas["total_vacunados"]
                        resumen.append([])
                        resumen.append(["Total de vacunaciones", total_vacunados])
                        
                        # Desglose por tipo de dosis
                        if "tipos_dosis" in estadisticas:
                            resumen.append([])
                            resumen.append(["Distribución por tipo de dosis", ""])
                            for nombre, info in estadisticas["tipos_dosis"].items():
                                resumen.append([nombre, f"{info['total']} ({info['porcentaje']}%)"])
                    
                    # Distribución por grupo etario
                    if "distribucion_grupo_etario" in estadisticas:
                        resumen.append([])
                        resumen.append(["Distribución por grupo etario", ""])
                        for grupo, count in sorted(estadisticas["distribucion_grupo_etario"].items()):
                            porcentaje = count/len(df)*100
                            resumen.append([grupo, f"{count} ({porcentaje:.1f}%)"])
                    
                    # Escribir resumen
                    pd.DataFrame(resumen).to_excel(writer, sheet_name='Resumen', index=False, header=False)
                    
                    # Hoja de metadatos
                    metadatos = []
                    metadatos.append(["Archivos procesados", processor.archivos_procesados])
                    metadatos.append(["Archivos con advertencias", len(processor.advertencias)])
                    metadatos.append(["Total de registros procesados", processor.registros_totales])
                    metadatos.append(["Patrón utilizado", args.patron])
                    
                    if args.modo in ["todo", "consolidar"]:
                        metadatos.append(["Directorio procesado", os.path.abspath(args.directorio)])
                    
                    # Detalles técnicos de columnas
                    metadatos.append([])
                    metadatos.append(["Columnas en el archivo", len(df.columns)])
                    
                    # Listado de columnas con datos no vacíos
                    metadatos.append([])
                    metadatos.append(["Columnas con datos (no vacías)", "Porcentaje de celdas con datos"])
                    for col in df.columns:
                        no_vacios = df[col].notna().sum()
                        porcentaje = no_vacios / len(df) * 100 if len(df) > 0 else 0
                        if porcentaje > 0:
                            metadatos.append([col, f"{porcentaje:.1f}%"])
                    
                    # Escribir metadatos
                    pd.DataFrame(metadatos).to_excel(writer, sheet_name='Metadatos', index=False, header=False)
                
                print(f"Archivo consolidado por {tipo} guardado en: {ruta_salida}")
                
                # Generar archivo de estadísticas en JSON si se solicita
                if args.estadisticas:
                    ruta_json = os.path.join(args.salida, f"Estadisticas_{args.vacuna.replace(' ', '_')}_{tipo}_{fecha_actual}.json")
                    with open(ruta_json, 'w', encoding='utf-8') as f:
                        json.dump(estadisticas, f, ensure_ascii=False, indent=4)
                    print(f"Archivo de estadísticas guardado en: {ruta_json}")
                
            except Exception as e:
                print(f"Error al guardar archivo Excel: {str(e)}")
                # Intentar guardar en CSV como alternativa
                ruta_csv = os.path.join(args.salida, f"Consolidado_{args.vacuna.replace(' ', '_')}_{tipo}_{fecha_actual}.csv")
                df.to_csv(ruta_csv, index=False)
                print(f"Se ha guardado una versión CSV alternativa en: {ruta_csv}")
            
            # Mostrar resumen para este tipo
            print(f"\nResumen de datos consolidados por {tipo}:")
            print(f"- Total de registros: {len(df)}")
            
            if tipo == "vacunacion":
                n_municipios = estadisticas.get("total_municipios", 0)
                print(f"- Municipios de vacunación: {n_municipios}")
                
                # Mostrar top 5 municipios
                top_municipios = sorted(estadisticas.get("municipios_vacunacion", {}).items(), 
                                       key=lambda x: x[1], reverse=True)[:5]
                if top_municipios:
                    print("  Top 5 municipios por cantidad de registros:")
                    for muni, count in top_municipios:
                        print(f"    * {muni}: {count} registros")
            else:
                n_deptos = estadisticas.get("total_departamentos", 0)
                n_municipios = estadisticas.get("total_municipios_residencia", 0)
                
                print(f"- Departamentos de residencia: {n_deptos}")
                print(f"- Municipios de residencia: {n_municipios}")
                
                # Mostrar top 5 departamentos
                top_deptos = sorted(estadisticas.get("departamentos_residencia", {}).items(), 
                                   key=lambda x: x[1], reverse=True)[:5]
                if top_deptos:
                    print("  Top 5 departamentos por cantidad de registros:")
                    for depto, count in top_deptos:
                        print(f"    * {depto}: {count} registros")
            
            if "total_vacunados" in estadisticas:
                total_vacunados = estadisticas["total_vacunados"]
                print(f"- Total de vacunaciones: {total_vacunados}")
                
                # Desglose por tipo de dosis
                if "tipos_dosis" in estadisticas:
                    for nombre, info in estadisticas["tipos_dosis"].items():
                        print(f"  - {nombre}: {info['total']} ({info['porcentaje']}%)")
            
            # Mostrar distribución por grupo etario
            if "distribucion_grupo_etario" in estadisticas:
                print("- Distribución por grupo etario:")
                for grupo, count in sorted(estadisticas["distribucion_grupo_etario"].items()):
                    porcentaje = count/len(df)*100
                    print(f"  - {grupo}: {count} ({porcentaje:.1f}%)")
    
    # Mostrar advertencias si las hay
    if processor.advertencias:
        print("\nAdvertencias durante el procesamiento:")
        for i, adv in enumerate(processor.advertencias[:10], 1):
            print(f"{i}. {adv}")
        
        if len(processor.advertencias) > 10:
            print(f"... y {len(processor.advertencias) - 10} advertencias más")
    
    print("\nProceso completado con éxito.")

if __name__ == "__main__":
    main()