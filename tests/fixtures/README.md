# Fixtures exclusivamente sintéticos

Todos los identificadores y valores de synthetic_bdg2 son inventados para pruebas
de software. No son mediciones BDG2 ni resultados del TFM. La disposición de carpetas
imita la publicada y las cabeceras mínimas siguen la documentación oficial.
Incluyen cero, ausencia, NaN literal, negativo, extremo, duplicado, decimales con
ceros y texto con comillas/espacios para comprobar conservación sin reglas Silver.

Los CSV vacíos o malformados se crean únicamente en tmp_path durante pytest.
Las salidas pytest son temporales; la demostración CLI se guarda exclusivamente
en data/lakehouse/synthetic/. Los originales BDG2 se conservan en data/raw/bdg2/, fuera de Git.

La configuración sintética incluye electricidad (4 registros), metadatos (2) y
meteorología (3). El CSV weather conserva timestamp y site_id como texto e incluye
cero, ausencia, -1.0, NaN y 12.50 para comprobar que Bronze no interpreta ni corrige
las mediciones. No implica ninguna equivalencia con unidades o códigos reales BDG2.
