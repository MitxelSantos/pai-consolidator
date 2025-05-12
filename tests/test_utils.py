"""
Tests para las funciones de utilidad.
"""
import os
import pandas as pd
import pytest
from pai_consolidator.core.utils import (
    extraer_nombre_municipio,
    encontrar_columnas_vacuna,
    identificar_columna_dosis,
    clasificar_grupo_etario,
    limpiar_texto,
    extraer_info_ruta,
    extraer_vereda_de_direccion
)

def test_extraer_nombre_municipio():
    """Prueba la extracción del nombre del municipio."""
    assert extraer_nombre_municipio("CASABIANCA_REGISTRO.xlsm") == "CASABIANCA"
    assert extraer_nombre_municipio("REGISTRO_IBAGUE_ABRIL.xlsm") == "IBAGUE"
    assert extraer_nombre_municipio("archivo_sin_patron.xlsm") == "ARCHIVO_SIN_PATRON"

def test_extraer_info_ruta():
    """Prueba la extracción de información de la ruta."""
    # Ruta con estructura completa
    ruta = os.path.join("REGISTROS_2025", "IBAGUE", "PAI_ABRIL.xlsm")
    info = extraer_info_ruta(ruta)
    assert info["municipio"] == "IBAGUE"
    assert info["año"] == "2025"
    assert info["mes"] == "04"
    
    # Ruta sin año pero con municipio
    ruta = os.path.join("DATOS", "CASABIANCA", "PAI_ABRIL.xlsm")
    info = extraer_info_ruta(ruta)
    assert info["municipio"] == "CASABIANCA"
    assert info["año"] is None
    assert info["mes"] == "04"

def test_encontrar_columnas_vacuna():
    """Prueba la búsqueda de columnas de vacuna."""
    df = pd.DataFrame({
        "A": [1, 2, 3],
        "Fiebre amarilla": [4, 5, 6],
        "Fiebre amarilla Dosis": [7, 8, 9],
        "Otra columna": [10, 11, 12]
    })
    
    columnas = encontrar_columnas_vacuna(df, "Fiebre amarilla")
    assert len(columnas) == 2
    assert "Fiebre amarilla" in columnas
    assert "Fiebre amarilla Dosis" in columnas

def test_identificar_columna_dosis():
    """Prueba la identificación de la columna de dosis."""
    df = pd.DataFrame({
        "Vacuna": ["Nombre", "Valor", "Otro"],
        "Dosis": ["Dosis", "Primera dosis", "Segunda dosis"],
        "Lote": ["Lote", "123", "456"]
    })
    
    assert identificar_columna_dosis(df, ["Vacuna", "Dosis", "Lote"]) == "Dosis"
    assert identificar_columna_dosis(df, ["Vacuna", "Lote"]) is None

def test_clasificar_grupo_etario():
    """Prueba la clasificación por grupo etario."""
    assert clasificar_grupo_etario(0) == "<1 año"
    assert clasificar_grupo_etario(3) == "1-5 años"
    assert clasificar_grupo_etario(7) == "6-10 años"
    assert clasificar_grupo_etario(15) == "11-18 años"
    assert clasificar_grupo_etario(30) == "19-60 años"
    assert clasificar_grupo_etario(70) == ">60 años"

def test_limpiar_texto():
    """Prueba la limpieza de texto."""
    assert limpiar_texto("  texto con  espacios  ") == "TEXTO CON ESPACIOS"
    assert limpiar_texto("texto\ncon\nsaltos") == "TEXTO CON SALTOS"
    assert limpiar_texto(123) == "123"

def test_extraer_vereda_de_direccion():
    """Prueba la extracción de vereda de una dirección."""
    assert extraer_vereda_de_direccion("VEREDA LA PALMA, CASA 5") == "LA PALMA"
    assert extraer_vereda_de_direccion("VDA EL CARMEN - FINCA LOS NARANJOS") == "EL CARMEN"
    assert extraer_vereda_de_direccion("CARRERA 5 #10-15") is None
    assert extraer_vereda_de_direccion("CORREGIMIENTO SAN BERNARDO") == "SAN BERNARDO"
    assert extraer_vereda_de_direccion(123) is None