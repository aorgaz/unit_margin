"""Configuration settings for unit margin processing.

This module defines:
- Target units and years for processing
- File paths for data sources (ESIOS, OMIE)
- Market configurations with their data sources and price mappings
"""

import os

# Filter Configuration
TARGET_UNITS = ['GUIG', 'GUIB', 'MLTG', 'MLTB', 'SLTG', 'SLTB', 'TJEG', 'TJEB']
TARGET_YEARS = [2020, 2021, 2022, 2023, 2024, 2025]

# Performance Configuration
MAX_WORKERS = 1  # Number of parallel workers for day processing (set to 1 for sequential)

# Base Paths (Adjust as needed or assume mounted)
# Using raw string for Windows paths
#BASE_PATH_SHAREPOINT = r"C:\Sharepoint\Enel Spa\ZZZ_Transfer - Documentos\DATA"
BASE_PATH_SHAREPOINT = r"C:\Users\albor\git\unit_margin\DATA"

PATH_ESIOS_I90 = os.path.join(BASE_PATH_SHAREPOINT, "ESIOS", "i90", "Raw")
PATH_ESIOS_IND = os.path.join(BASE_PATH_SHAREPOINT, "ESIOS", "Ind", "Precios")
PATH_OMIE = os.path.join(BASE_PATH_SHAREPOINT, "OMIE", "zip")

# Market Configurations
# Structure: Market -> {Source, Type, File/Sheet/ID, Notes}

MARKET_CONFIG = [
    { 
        "market": "Bilaterales",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA27", # Sheet name
        "price_id": None
    },
    { 
        "market": "PDBC",
        "source": "omie",
        "type": "Energy",
        "data_id": "pdbc", # folder/file prefix
        "price_source": "omie",
        "price_id": "marginalpdbc"
    },
    { 
        "market": "PDBF",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA26",
        "price_id": None
    },
    { 
        "market": "Restricciones tecnicas Subir",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA03",
        "filters": {"Sentido": "Subir"},
        "price_source": "i90",
        "price_id": "I90DIA09"
    },
    { 
        "market": "Restricciones tecnicas Bajar",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA03",
        "filters": {"Sentido": "Bajar"},
        "price_source": "i90",
        "price_id": "I90DIA09"
    },
    { 
        "market": "PDVP",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA01",
        "price_id": None
    },
    { 
        "market": "PDVD",
        "source": "omie",
        "type": "Energy",
        "data_id": "pdvd",
        "price_id": None
    },
    {
        "market": "PIBCI",
        "source": "omie",
        "type": "Energy",
        "data_id": "pibci",
        "price_source": "indicator",
        "price_id": { # Map session to indicator
            1: 612, 2: 613, 3: 614, 4: 615, 5: 616, 6: 617, 7: 618
        }
    },
    { 
        "market": "MIC",
        "source": "omie",
        "type": "EnergyPrice", # Special case
        "data_id": "trades",
        "price_id": None
    },
    { 
        "market": "Banda Subir",
        "source": "i90",
        "type": "Power",
        "data_id": "I90DIA05",
        "filters": {"Sentido": "Subir"},
        "price_source": "indicator",
        "price_id": "BANDA_SUBIR_RULE"
    },
    { 
        "market": "Banda Bajar",
        "source": "i90",
        "type": "Power",
        "data_id": "I90DIA05",
        "filters": {"Sentido": "Bajar"},
        "price_source": "indicator",
        "price_id": 634
    },
    { 
        "market": "aFRR Subir",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA37",
        "filters": {"Sentido": "Subir"},
        "price_source": "indicator",
        "price_id": 682
    },
    { 
        "market": "aFRR Bajar",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA37",
        "filters": {"Sentido": "Bajar"},
        "price_source": "indicator",
        "price_id": 683
    },
    { 
        "market": "mFRR Subir",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA07",
        "filters": {"Sentido": "Subir"},
        "price_source": "indicator",
        "price_id": "MFRR_SUBIR_RULE"
    },
    { 
        "market": "mFRR Bajar",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA07",
        "filters": {"Sentido": "Bajar"},
        "price_source": "indicator",
        "price_id": "MFRR_BAJAR_RULE"
    },
    { 
        "market": "RR Subir",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA06",
        "quantity_filters": {"Sentido": "Subir", "Redespacho": "RR"},
        "price_source": "i90",
        "price_id": "I90DIA11",
        "price_filters": {"Sentido": "Subir", "Tipo": "RR"}
    },
    { 
        "market": "RR Bajar",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA06",
        "quantity_filters": {"Sentido": "Bajar", "Redespacho": "RR"},
        "price_source": "i90",
        "price_id": "I90DIA11",
        "price_filters": {"Sentido": "Bajar", "Tipo": "RR"}
    },
    { 
        "market": "Restricciones TR Subir",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA08",
        "filters": {"Sentido": "Subir", "Redespacho": "Restricciones Técnicas"},
        "price_source": "i90",
        "price_id": "I90DIA10"
    },
    { 
        "market": "Restricciones TR Bajar",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA08",
        "filters": {"Sentido": "Bajar", "Redespacho": "Restricciones Técnicas"},
        "price_source": "i90",
        "price_id": "I90DIA10"
    },
    { 
        "market": "P48",
        "source": "i90",
        "type": "Energy",
        "data_id": "I90DIA02",
        "price_id": None
    }
]
