# Fixtures exclusivamente sintéticos

Todos los identificadores y valores de synthetic_bdg2 son inventados para pruebas
de software. No son mediciones BDG2 ni resultados del TFM. La disposición de carpetas
imita la publicada y las cabeceras mínimas siguen la documentación oficial.
Incluyen cero, ausencia, NaN literal, negativo, extremo, duplicado, decimales con
ceros y texto con comillas/espacios para comprobar conservación sin reglas Silver.

Los CSV vacíos o malformados se crean únicamente en tmp_path durante pytest.
Las salidas pytest son temporales; la demostración CLI se guarda exclusivamente
en data/lakehouse/synthetic/. Los originales BDG2 irán en data/raw/bdg2/.
