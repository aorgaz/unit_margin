Necesito un script de python que procese ficheros de REE (red eléctrica) y Omie. El objetivo es tener el margen horario/cuartohorario ((energía ó potencia) x precio) para las distintas unidades de programación y de oferta del sistema eléctrico español.

Los datos de energía ó potencia y precios están segmentados por mercados y se encuentran en tres tipos de ficheros: hojas de los i90 de esios (por unidades de programación), indicadores de esios y ficheros de omie (por unidades de oferta).

En la siguiente tabla se detallan las fuentes de información de cada mercado y cada tipo de datos (energía ó potencia y precio)

mercado | fuente | tipo | fichero/hoja/indicador | notas adicionales

Bilaterales | i90 | Energía | I90DIA27 |
PDBC | omie | Energía | pdbc_aaaammdd.v |
PDBC | omie | Precio | marginalpdbc_aaaammdd.v |
PDBF | i90 | Energía | I90DIA26 |
Restricciones técnicas | i90 | Energía | I90DIA03 |
Restricciones técnicas | i90 | Precio | I90DIA09 |
PDVP | i90 | Energía | I90DIA01
PDVD | omie | Energía | pdvd_aaaammdd.v |
PIBC ss | omie | Energía | pibci_aaaammddss.v |
PIBC ss1 | indicador | Precio | 612 | Sesión 1
PIBC ss2 | indicador | Precio | 613 | Sesión 2
PIBC ss3 | indicador | Precio | 614 | Sesión 3
PIBC ss4 | indicador | Precio | 615 | Sesión 4
PIBC ss5 | indicador | Precio | 616 | Sesión 5
PIBC ss6 | indicador | Precio | 617 | Sesión 6
PIBC ss7 | indicador | Precio | 618 | Sesión 7
MIC | omie | Energía y precio | trades_aaaammdd.v | mención especial abajo
Banda bajar/subir | i90 | Potencia | I90DIA05 | filtrar por 'Bajar'/'Subir' en columna Sentido ; bajar=subir hasta 20-11-2024
Banda bajar | indicador | Precio | 634 |
Banda subir | indicador | Precio | 634 y 2130 | 634 hasta 20-11-2024 y 2130 desde 20-11-2024
aFRR bajar/subir | i90 | Energía | I90DIA37 | filtrar por 'Bajar'/'Subir' en columna Sentido
aFRR bajar | indicador | Precio | 683 |
aFRR subir | indicador | Precio | 682 |
mFRR bajar/subir | i90 | Energía | I90DIA07 | filtrar por 'Bajar'/'Subir' en columna Sentido
mFRR bajar | indicador | Precio | 676 | hasta 10-12-2024
mFRR subir | indicador | Precio | 677 | hasta 10-12-2024
mFRR bajar | indicador | Precio | 2197 | desde 10-12-2024
mFRR subir | indicador | Precio | 2197 | desde 10-12-2024
RR bajar/subir | i90 | Energía | I90DIA06 | filtrar por 'Bajar'/'Subir' en columna Sentido y por 'RR' en columna Redespacho
RR bajar/subir | i90 | Precio | I90DIA11 | filtrar por 'Bajar'/'Subir' en columna Sentido y por 'RR' en columna Redespacho
Restricciones técnicas tiempo real | i90 | Energía | I90DIA08 |
Restricciones técnicas tiempo real | i90 | Precio | I90DIA10 |
P48 | i90 | Energía | I90DIA02 |

Hay mercados en los que no hay precio. En esos casos dejar el precio en blanco dejarlo en blanco.

Abajo se hace una explicación detallada de cada fichero:

Los ficheros i90 están dentro de ficheros zip en la siguiente ruta:
C:\Sharepoint\Enel Spa\ZZZ_Transfer - Documentos\DATA\ESIOS\i90\Raw\i90_[aaaa]\I90DIA_[aaaammdd].zip\I90DIA_[aaaammdd].xls

La estructura de archivos se presenta en el documento adjunto. Aquí es importante destacar que no aparece la columna unidad de programación en cada hoja, pero está presente. Para que veas un ejemplo, se adjuntan

Habría que filtrar por la hoja correspondiente, determinar cuántas filas hay que saltarse, ver si está por horas o por cuartos de hora y extraer y procesar la tabla con las fechas agregando el resto de columnas que no se necesite a no ser que se especifique lo contrario.

Los ficheros de indicadores de esios están:
"C:\Sharepoint\Enel Spa\ZZZ_Transfer - Documentos\DATA\ESIOS\Ind\Precios\682\682_2025_6.csv"

Los ficheros de omie están en:
C:\Sharepoint\Enel Spa\ZZZ_Transfer - Documentos\DATA\OMIE\zip\pdbc\pdbc_[aaaamm].zip\pdbc_[aaaammdd].[v]
C:\Sharepoint\Enel Spa\ZZZ_Transfer - Documentos\DATA\OMIE\zip\pdvd\pdvd_[aaaamm].zip\pdvd_[aaaammdd].[v]
C:\Sharepoint\Enel Spa\ZZZ_Transfer - Documentos\DATA\OMIE\zip\pibci\pibci_[aaaamm].zip\pibci_[aaaammddss].[v]
C:\Sharepoint\Enel Spa\ZZZ_Transfer - Documentos\DATA\OMIE\zip\trades\trades_[aaaamm].zip\trades_[aaaammdd].[v]

Aquí [ss] corresponde al número de sesión del intradiario y [v] es la versión del fichero (siempre habrá que coger la última para cada fichero)

Adjunto también imágenes con especificaciones de los ficheros

En el caso del MIC, el fichero trades tiene la información de energía y precios al mismo tiempo. Como son trades, habrá que filtrar por unidad de compra por un lado y de venta por otro para sacar el ingreso total. En este caso, aunque salga a nivel segundo habría que quedarse a nivel hora o minuto según los datos sean horarios o cuartohorarios.

Hay que prestar especial detalle a las fechas incluyendo DST. Necesito dos columnas de fecha: Datetime Madrid y Datetime UTC+1. Según el fichero o el periodo temporal la información puede venir de forma horaria o cuartohoraria. Habrá que deducirlo según corresponda. Siempre las horas de los ficheros hacen referencia a Madrid. Además, hay datos horarios y cuartohorarios, habrá que procesarlo con cuidado y finalmente dar la posibilidad de tener los datos en periodo original (horario/cuartohorario) o transformado a horario.

Como la información viene por unidad de programación o de oferta según el fichero que se lee, necesito también hacer un merge entre unidades de programación y unidades de oferta, quedándome con los que sean idénticos.

Estaría bien saber qué porcentaje de unidades tienen un nombre idéntico entre unidades de programación y de oferta.

Por último, hay que guardar los datos finales en un fichero csv bien organizados

Para verificar la información de una forma más rápida quiero filtrar por las siguientes unidades: [GUIG, GUIB, MLTG, MLTB, SLTG, SLTB, TJEG, TJEB]
También quiero filtrar la información para los un determinado rango de fechas especificado por yyyy. 
Todo esto creo que lo suyo es hacerlo previamente para que sea más rápido, no al final.

Instrucciones del código:
-Código en inglés sin clases
-Con descripciones de lo que hace cada función
-No quiero emojis
-Quiero un código bien comentado pero no en exceso
-Quiero que el script vaya lo más rápido posible porque supone leer y escribir muchísimos datos
-Necesito que se escriba un log (con nombre la fecha (año mes segundo hora minuto segundo)) en el que se vea el proceso con seguimiento del tiempo
-Necesito que al final del script se vea el tiempo que ha durado en total el proceso

Dame en primer lugar una estructura para construir el repositorio donde voy a guardar el código, y decirme dónde guardar la documentación que te he adjuntado y también explicar tener un documento explicativo que se vaya actualizando en cada paso a modo de explicación del script (documentación) donde además se ponga un poco en contexto todo y se explique bien conceptualmente de dónde se sacan los datos.

Genera la estructura del código e implementa la parte de procesamiento de ficheros i90. En este punto, déjame tener directorios de datos dummy para que pueda probar el código.

Implementa la parte de procesamiento de ficheros de omie

Implementa la parte de procesamiento de ficheros de precios


----

Documentación

Cómo obtener los ficheros de documentación

ESIOS
Ficheros i90
https://www.esios.ree.es/es/documentacion > EXPORTAR EXCEL > Buscar por "Intercambio de Información con el OS (Mercados de Producción)" > Mercados de Producción. Parte 11 Ficheros I3DIA, IMES e I90DIA con información agregada v[XX].pdf I90DIA

OMIE
https://www.omie.es/es/file-access-list > https://www.omie.es/es/publicaciones/64 > Buscar por fichero