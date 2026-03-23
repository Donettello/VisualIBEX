# 📈 VisualIbex: Real-Time Data Pipeline & BI Dashboard
Este proyecto implementa una solución End-to-End de Business Intelligence para el seguimiento del IBEX 35, automatizando todo el flujo desde la extracción de datos hasta la visualización avanzada.

## 🛠️ Arquitectura Técnica
* **Data Source**: Extracción mediante Web Scrapping/API con **Python**.
* **Edge Computing**: Script automatizado corriendo en una **Raspberry Pi** (24/7).
* **Database**: Servidor **MariaDB** para el almacenamiento de históricos y metadatos.
* **Networking**: Túnel seguro vía **ngrok** para permitir la conexión remota desde la nube.
* **Visualización**: Dashboard en **Power BI** con lógica **DAX** avanzada (Análisis de Velas, Heatmaps sectoriales...)

## 🚀 Características Principales
* **Actualización Automática**: Los datos viajan de la Raspberry a Power BI con un solo clic.
* **Análisis Técnico**: Gráfico de velas para detectar tendencias.
* **Market Heatmap**: Treemap dinámico para identificar sectores ganadores/perdedores por sesión.
* **Filtros Interactivos**: Segmentación por sector, empresa y rangos de fechas.

## 📁 Estructura del Repositorio
* `/codigo`: Código Python de extracción de información y carga de datos a MariaDB.
* `/pbix`: Ubicación archivo `.pbix` para su revisión técnica. Puede verse un breve resumen en [este enlace](https://www.google.com). (En breve)
