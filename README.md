# Script de Margen Unitario

Este repositorio contiene un script en Python para procesar ficheros de mercados energéticos (REE/ESIOS y OMIE) y calcular el margen horario/cuartohorario.

## Estructura
- `src/`: Código fuente.
  - `config.py`: Configuración de rutas, mercados y filtros.
  - `engine.py`: Lógica principal de procesamiento.
  - `readers/`: Módulos de lectura de ficheros (I90, OMIE, ESIOS).
- `docs/`: Documentación detallada (`methodology.md`).
- `logs/`: Logs de ejecución.
- `output/`: Ficheros CSV resultantes.

## Verificación de Rutas
Antes de ejecutar, verifique `src/config.py` y ajuste `BASE_PATH_SHAREPOINT` si sus datos están en otra ubicación.

## Ejecución
Para ejecutar el script para los años configurados:
```bash
python src/main.py
```

Para años específicos:
```bash
python src/main.py --years 2024 2025
```

El script generará un log en `logs/` con el detalle del proceso y el tiempo total.
El fichero final se guardará en `output/`.
