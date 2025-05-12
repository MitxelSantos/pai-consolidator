"""
Tests para el procesador de archivos PAI.
"""
import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from pai_consolidator.core.processor import PaiProcessor

@pytest.fixture
def mock_excel():
    """Mock para simular la lectura de un archivo Excel."""
    mock_df = pd.DataFrame({
        "Consecutivo": [1, 2, 3],
        "Fecha de atención formato de fecha en números (día/mes/año)*": ["4/10/25", "4/11/25", "4/12/25"],
        "Tipo de identificación*": ["CC", "TI", "CC"],
        "Número de identificación*": ["12345", "67890", "54321"],
        "Primer nombre*": ["JUAN", "MARIA", "PEDRO"],
        "Primer apellido*": ["PEREZ", "GOMEZ", "RODRIGUEZ"],
        "AÑOS": [30, 12, 45],
        "MESES": [0, 6, 3],
        "DIAS": [0, 15, 20],
        "Sexo*": ["HOMBRE", "MUJER", "HOMBRE"],
        "Departamento de residencia*": ["TOLIMA", "TOLIMA", "TOLIMA"],
        "Municipio de residencia*": ["IBAGUE", "CASABIANCA", "IBAGUE"],
        "Área*": ["URBANA", "RURAL", "URBANA"],
        "Fiebre amarilla ": [None, None, None],
        "Dosis": ["Única", "Única", None],
        "Lote": ["123", "456", None]
    })
    return mock_df

def test_procesar_archivo(mock_excel):
    """Prueba el procesamiento de un archivo PAI."""
    with patch("pandas.read_excel", return_value=mock_excel), \
         patch("pai_consolidator.core.processor.extraer_info_ruta") as mock_info:
        
        mock_info.return_value = {
            "municipio": "TEST_MUNICIPIO",
            "año": "2025",
            "mes": "04"
        }
        
        processor = PaiProcessor(vacuna="Fiebre amarilla")
        df = processor.procesar_archivo("TEST_MUNICIPIO.xlsm")
        
        assert not df.empty
        assert "Municipio_Vacunacion" in df.columns
        assert "Vacunado" in df.columns
        assert df["Municipio_Vacunacion"].iloc[0] == "TEST_MUNICIPIO"
        assert df["Vacunado"].sum() == 2  # Dos registros con dosis
        assert "Año_Registro" in df.columns
        assert df["Año_Registro"].iloc[0] == "2025"

def test_consolidar_archivos():
    """Prueba la consolidación de archivos PAI."""
    with patch("pai_consolidator.core.processor.listar_archivos_pai", 
              return_value=["archivo1.xlsm", "archivo2.xlsm"]), \
         patch.object(PaiProcessor, "procesar_archivo") as mock_procesar:
        
        # Crear dos DataFrames de ejemplo
        df1 = pd.DataFrame({
            "Municipio_Vacunacion": ["MUNICIPIO1", "MUNICIPIO1"],
            "Departamento_Residencia": ["TOLIMA", "TOLIMA"],
            "Municipio_Residencia": ["IBAGUE", "IBAGUE"],
            "Vacunado": [True, True],
            "Fecha": pd.to_datetime(["2025-04-10", "2025-04-11"])
        })
        
        df2 = pd.DataFrame({
            "Municipio_Vacunacion": ["MUNICIPIO2", "MUNICIPIO2"],
            "Departamento_Residencia": ["TOLIMA", "CALDAS"],
            "Municipio_Residencia": ["CASABIANCA", "MANIZALES"],
            "Vacunado": [True, False],
            "Fecha": pd.to_datetime(["2025-04-12", "2025-04-13"])
        })
        
        # Configurar el mock para devolver los DataFrames de ejemplo
        mock_procesar.side_effect = [df1, df2]
        
        # Ejecutar la función a probar - Consolidado por vacunación
        processor = PaiProcessor(tipo_consolidado="vacunacion")
        resultado = processor.consolidar_archivos("directorio\\prueba")
        
        # Verificar resultados
        assert "vacunacion" in resultado
        assert len(resultado["vacunacion"]) == 4
        assert resultado["vacunacion"]["Municipio_Vacunacion"].nunique() == 2
        assert resultado["vacunacion"]["Vacunado"].sum() == 3
        
        # Ejecutar la función a probar - Consolidado por residencia
        processor = PaiProcessor(tipo_consolidado="residencia")
        resultado = processor.consolidar_archivos("directorio\\prueba")
        
        # Verificar resultados
        assert "residencia" in resultado
        assert len(resultado["residencia"]) == 4
        assert resultado["residencia"]["Departamento_Residencia"].nunique() == 2
        assert resultado["residencia"]["Municipio_Residencia"].nunique() == 3
        
        # Ejecutar la función a probar - Ambos consolidados
        processor = PaiProcessor(tipo_consolidado="ambos")
        resultado = processor.consolidar_archivos("directorio\\prueba")
        
        # Verificar resultados
        assert "vacunacion" in resultado
        assert "residencia" in resultado
        assert len(resultado["vacunacion"]) == 4
        assert len(resultado["residencia"]) == 4