"""
Interfaz de línea de comandos para el consolidador de datos PAI.
"""
import os
import argparse
import sys
from datetime import datetime
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
        help="Directorio con los archivos PAI (.xlsm, .xlsx, etc.)"
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
        default="*.xls*",
        help="Patrón para buscar archivos (default: '*.xls*' captura .xlsx, .xlsm, etc.)"
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
        default="vacunacion",
        help="Tipo de consolidado: por lugar de residencia de la persona o por lugar de vacunación"
    )
    
    parser.add_argument(
        "--excluir", "-e",
        nargs="+",  # Permite múltiples valores
        default=["COVID"],
        help="Patrones para excluir archivos (por defecto: COVID)"
    )
    
    parser.add_argument(
        "--directorio-exacto",
        action="store_true",
        help="Procesar solo los archivos en el directorio especificado, sin buscar en subdirectorios"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mostrar información detallada durante el procesamiento"
    )
    
    args = parser.parse_args()
    
    # Verificar que el directorio de entrada existe
    if not os.path.isdir(args.directorio):
        print(f"Error: El directorio {args.directorio} no existe")
        sys.exit(1)
    
    # Crear directorio de salida si no existe
    os.makedirs(args.salida, exist_ok=True)
    
    print(f"Iniciando consolidación de datos para {args.vacuna}")
    print(f"Directorio de datos: {args.directorio}")
    print(f"Patrón de búsqueda: {args.patron}")
    if args.excluir:
        print(f"Excluyendo archivos que contengan: {', '.join(args.excluir)}")
    print(f"Tipo de consolidado: {args.tipo_consolidado}")
    if args.directorio_exacto:
        print("Modo: Procesando solo el directorio especificado (sin subdirectorios)")
    
    # Procesar archivos
    processor = PaiProcessor(vacuna=args.vacuna, tipo_consolidado=args.tipo_consolidado)
    resultado_consolidado = processor.consolidar_archivos(
        args.directorio, 
        args.patron,
        patrones_exclusion=args.excluir,
        procesar_directorio_exacto=args.directorio_exacto
    )
    
    if not resultado_consolidado:
        print("No se pudieron consolidar los datos.")
        sys.exit(1)
    
    # Guardar resultados
    fecha_actual = datetime.now().strftime("%Y%m%d")
    
    for tipo, df in resultado_consolidado.items():
        # Filtrar por año y mes si se especifican
        if args.año or args.mes:
            print(f"Aplicando filtros: Año={args.año or 'Todos'}, Mes={args.mes or 'Todos'}")
            df_consolidado_original = df.copy()
            
            if args.año and "Año_Registro" in df.columns:
                df = df[df["Año_Registro"] == args.año]
            
            if args.mes and "Mes_Registro" in df.columns:
                df = df[df["Mes_Registro"] == args.mes]
            
            print(f"Filtrado: {len(df)} de {len(df_consolidado_original)} registros")
            
            if df.empty:
                print(f"Advertencia: No hay datos para el filtro año={args.año}, mes={args.mes}")
                continue
        
        nombre_archivo = f"Consolidado_{args.vacuna.replace(' ', '_')}_{tipo}_{fecha_actual}.xlsx"
        ruta_salida = os.path.join(args.salida, nombre_archivo)
        
        df.to_excel(ruta_salida, index=False)
        print(f"Archivo consolidado por {tipo} guardado en: {ruta_salida}")
        
        # Mostrar resumen para este tipo
        print(f"\nResumen de datos consolidados por {tipo}:")
        print(f"- Total de registros: {len(df)}")
        
        if tipo == "vacunacion":
            municipios_vacunacion = df["Municipio_Vacunacion"].nunique()
            print(f"- Municipios de vacunación: {municipios_vacunacion}")
            if municipios_vacunacion <= 10:  # Si son pocos, mostrarlos
                top_municipios = df["Municipio_Vacunacion"].value_counts().head(10)
                print("  Top municipios de vacunación:")
                for mun, count in top_municipios.items():
                    print(f"    - {mun}: {count} registros")
        else:
            dpto_count = df["Departamento_Residencia"].nunique()
            muni_count = df["Municipio_Residencia"].nunique()
            print(f"- Departamentos de residencia: {dpto_count}")
            print(f"- Municipios de residencia: {muni_count}")
            
            # Mostrar top municipios
            if muni_count <= 10:
                top_municipios = df["Municipio_Residencia"].value_counts().head(10)
                print("  Top municipios de residencia:")
                for mun, count in top_municipios.items():
                    print(f"    - {mun}: {count} registros")
        
        if "Vacunado" in df.columns:
            total_vacunados = df["Vacunado"].sum()
            print(f"- Total de vacunaciones: {total_vacunados}")
            
            # Desglose por tipo de dosis
            dosis_cols = {
                "Es_Primera_Dosis": "Primera dosis",
                "Es_Segunda_Dosis": "Segunda dosis",
                "Es_Refuerzo": "Refuerzo",
                "Es_Unica_Dosis": "Dosis única"
            }
            
            for col, nombre in dosis_cols.items():
                if col in df.columns:
                    total = df[col].sum()
                    if total > 0:
                        porcentaje = total/total_vacunados*100 if total_vacunados > 0 else 0
                        print(f"  - {nombre}: {total} ({porcentaje:.1f}%)")
    
    print("\nProceso completado con éxito.")

if __name__ == "__main__":
    main()