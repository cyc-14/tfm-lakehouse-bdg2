# Guía técnica: hitos 1 y 2

## Alcance y responsabilidades

Implementación local de la base Spark/Delta y de tres fuentes Bronze
(electricidad, metadatos y meteorología opcional). Carlos dispone
de módulos separados para configuración, sesión, ingesta, ejecución y pruebas.
Las decisiones de Irantzu sobre selección de edificios, calidad, tipos analíticos,
integración, indicadores y validación del caso real quedan pendientes de acuerdo conjunto.

El contrato Bronze no selecciona edificios ni fija un esquema analítico. La estructura
de carpetas futuras de processing, quality y analytics se creará cuando haya código
verificado que alojar. No se han añadido módulos vacíos.

## Arquitectura implementada

```text
Archivos CSV originales inalterados (raw_root)
  -> validación UTF-8/CSV completa, cabecera, conteo independiente y SHA-256
  -> Spark: columnas originales string, sin inferencia
  -> validación/materialización de todas las fuentes configuradas
  -> Delta Bronze: snapshot completo por fuente + trazabilidad
  -> recuperación de cada versión escrita y comprobación de tipos y conteos
  -> publicación JSON atómica del conjunto de versiones verificadas
```

Se separan los CSV originales de su representación tabular Delta. Electricidad
permanece en formato ancho; los nombres originales de edificios se conservan.
No se normalizan fechas ni se convierten a UTC. Las fechas UTC de los informes
describen la ejecución del software, nunca las mediciones de origen.

## Estructura

```text
config/
  bronze.example.toml          # entradas reales locales, no versionadas
  bronze.synthetic.toml        # demostración de software
src/tfm_lakehouse/
  config.py                   # configuración y separación de rutas
  spark.py                    # Spark local y extensiones Delta
  environment.py              # escritura/lectura efectiva de verificación
  cli.py, __main__.py          # comandos
  ingestion/
    csv_source.py             # inspección y lectura CSV
    bronze.py                 # snapshots, trazabilidad e informes
    snapshot.py               # consulta de publicaciones completas
scripts/check_environment.py  # alternativa al comando check-environment
tests/
  test_config.py, test_environment.py
  ingestion/
    test_sources.py, test_bronze.py, test_bronze_safety.py, test_real_data.py
  fixtures/synthetic_bdg2/data/{meters/raw,metadata,weather}/
docs/
  hitos-1-2.md
data/                         # generado/local; excluido de Git
  raw/bdg2/data/...            # originales reales inalterados
  lakehouse/
    synthetic/{bronze,_runs}/
    real/{bronze,_runs}/       # cargas con fuentes reales
  checks/                     # resultados de verificación locales
```

## Instalación y versiones

La instalación se realiza dentro de Ubuntu/WSL, no con Python de Windows.
Java 17 debe estar disponible mediante PATH o JAVA_HOME. No se modifica el Java
global ni se actualizan Python, Spark o Delta.

Sustituir la ruta de ejemplo por la ubicación del repositorio dentro de Ubuntu/WSL.

```bash
cd "/ruta/al/repositorio/tfm-lakehouse"
export PATH="$HOME/.local/bin:$PATH"
# Si no existe el entorno:
uv venv --python 3.11.17
uv pip install --python .venv/bin/python -r requirements.lock -e .
uv pip check --python .venv/bin/python
.venv/bin/python --version
java -version
uv --version
```

`requirements.txt` mantiene los dos pines originales. `pyproject.toml` declara
el paquete y los comandos; `requirements-dev.txt` añade pytest 8.4.2.
`requirements.lock` fija todas las dependencias Python del entorno comprobado
(incluidas las transitivas originales), y el backend de construcción se fija
en pyproject. No se necesita pip dentro de .venv: uv puede instalar directamente.

La [matriz oficial de Delta](https://docs.delta.io/releases/) contempla Delta 3.3.x
con Spark 3.5.x. El comando check-environment permite registrar la comprobación
local en `data/checks/environment/<nombre-nuevo>.json`; estos resultados generados
no se versionan.

## Spark y Maven

`spark_session()` abre una sesión local exclusiva y la cierra al salir. Por defecto:
`local[2]`, dos particiones de shuffle y de snapshots Delta, UI desactivada.
Rechaza un master distribuido y cualquier sesión o contexto Spark ya existente.

### Propiedad de recursos y fallos de arranque (AUD-05)

El gestor exige un contexto exclusivo: comprueba los recursos antes de construir
el builder o modificar el entorno y repite la comprobación bajo los bloqueos
reentrantes de PySpark 3.5.7. El orden es siempre `SparkSession.Builder._lock`
y después `SparkContext._lock`; cada adquisición tiene un límite de diez
segundos. Estos bloqueos se liberan antes del bloque del usuario y también
cuando falla el arranque. No se introduce un registro global adicional.
La protección se establece antes de adquirirlos. Se verifica la profundidad de
recursión del hilo mediante `RLock._recursion_count()` de CPython 3.11, evitando
perder una adquisición si se cancela antes de guardar un indicador y conservando
las adquisiciones previas del llamador. Si esa capacidad no está disponible,
el gestor falla antes de adquirir bloqueos o modificar el entorno.

La identidad del contexto propio se conserva aunque falle la construcción
posterior de la sesión. La limpieza solo actúa si el contexto activo sigue
siendo ese mismo objeto. Si otro componente lo sustituye, no se detiene ni se
borra el nuevo contexto. Tampoco se llama a `stop()` sobre una sesión devuelta
que pertenezca a otro contexto. Una sesión global ajena no se borra como parte
de la limpieza de un contexto propio.

El entorno se restaura tanto en operación normal como ante errores, incluido
un `PYSPARK_PYTHON` inicialmente ausente o vacío. La primera causa se conserva;
los errores secundarios de cierre se añaden como notas. La primera cancelación
(`KeyboardInterrupt` o `SystemExit`, conservando identidad y código) controla
la propagación, manteniendo un error ordinario anterior como causa.

Son APIs internas verificadas para la versión fijada, no un contrato general de
PySpark entre versiones. La coordinación cubre los caminos normales de creación
que utilizan esos bloqueos; no aísla constructores de sesiones directos,
manipulaciones manuales de singleton ni componentes que se acoplen al contexto
propio durante el bloque. El entorno del proceso es global. No se admite
compartir deliberadamente este gestor con otros propietarios de Spark. Un fallo
de `stop()`, una terminación forzosa o cancelaciones repetidas pueden impedir
completar la limpieza; el gestor comunica el fallo sin detener recursos ajenos.

Se configuran expresamente:

- `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`.
- `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`.
- `configure_spark_with_delta_pip`, que para la versión instalada selecciona
  `io.delta:delta-spark_2.12:3.3.3`.
- Workers con `sys.executable` a través de la configuración Spark y de
  `PYSPARK_PYTHON` acotado a la sesión; su valor anterior se restaura.

La [guía oficial](https://docs.delta.io/quick-start/) explica el helper Python;
en este proyecto se usa la versión 3.3.3 instalada, sin copiar los números de versión
de los ejemplos de la documentación actual. Se comprobó el código del helper instalado.

En la primera ejecución se descargaron de Maven Central:
`delta-spark_2.12:3.3.3`, `delta-storage:3.3.3` y `antlr4-runtime:4.9.3`.
Ivy guarda sus JAR y metadatos en `~/.ivy2/`; `spark.ivy_dir` permite elegir
otra caché. La siguiente ejecución reutilizó los tres artefactos.
Un entorno aislado necesita preparar esa caché previamente; instalar delta-spark
en Python por sí solo no proporciona los JAR de ejecución.

### Separación de la caché Ivy (AUD-08)

El validador compartido no importa PySpark, no crea directorios y no prueba
permisos mediante escrituras. Antes del builder y del entorno, comprueba la ruta
explícita o el default `Path.home() / ".ivy2"`, y transmite su ruta canónica a
`spark.jars.ivy`. Es una decisión explícita: deja de delegar la selección al
home de Java. En el entorno local habitual coincide con la caché anterior; si
ambos homes difieren, debe indicarse una caché segura mediante `ivy_dir`.

Se protegen `data/raw`, `data/lakehouse` y `data/checks` del repositorio identificado
desde el paquete y su manifiesto, con independencia del directorio de invocación.
Bronze añade sus raíces personalizadas. El argumento opcional y solo por nombre
`protected_roots` de `spark_session()` permite ampliar esa protección sin
sustituir las raíces convencionales. Fuera de un checkout identificable es
obligatorio declarar raíces; el gestor no puede descubrir fuentes o expedientes
externos arbitrarios y el llamador debe declararlos.

Se rechazan solapamientos canónicos en ambos sentidos, componentes que sean
archivos, enlaces colgantes, ciclos y errores de inspección. Se aceptan cachés
externas, directorios seguros futuros y alias existentes de la raíz cuyo destino
sea seguro. Dentro de `cache` y `jars` se rechazan enlaces, también profundos,
para impedir escrituras indirectas hacia fuentes o expedientes. No se rechaza
un vecino solo por compartir un prefijo textual. Las rutas TOML siguen siendo
relativas al TOML y las de CLI/API al directorio de invocación; `../.cache/ivy`
sigue siendo una opción válida.

El perfil local rechaza overrides Ivy explícitos en argumentos del lanzador,
opciones JVM o `spark-defaults.conf`, archivos de propiedades alternativos,
escapes/continuaciones activos en los defaults, archivos `spark-env.sh` o
`java-opts` personalizados
y gateways proporcionados externamente. También rechaza `@argfiles` en opciones
JVM, argumentos del driver o defaults, cuyo contenido no está auditado.
No interpreta shell ni XML de Ivy.
Opciones ordinarias ajenas a Ivy siguen admitidas. Esta comprobación presupone
la distribución instalada y sus scripts de lanzamiento sin alteraciones; no
certifica lanzadores externos arbitrarios.

La validación es puntual: no elimina TOCTOU ni aísla el filesystem. Enlaces duros,
bind mounts, cambios posteriores de rutas o configuración y manipulación
deliberada por procesos con permisos quedan fuera de la garantía. Una caché
existente se inspecciona en lectura; una resolución Maven ordinaria puede
actualizar sus propios metadatos. Para pruebas sin descargas ni cambios en la
caché histórica, se pueden usar copias temporales de JAR locales mediante
`spark.jars`, sin `spark.jars.packages`. Eso verifica Spark/Delta local, pero
no la resolución Maven habitual.

```bash
.venv/bin/python -m tfm_lakehouse check-environment --report data/checks/environment/check-001.json
# Alternativa equivalente:
.venv/bin/python scripts/check_environment.py --report data/checks/environment/check-002.json
```

El criterio de aceptación incluye: tres filas conocidas, esquema y valores exactos
(con cero y null), archivos Parquet y registro JSON en _delta_log. La tabla vive
en un directorio temporal y se elimina al terminar. No es un benchmark.

### Protección del informe de entorno

Ambas interfaces comparten la misma validación y publicación. Si se solicita
`--report`, el directorio de invocación debe ser la raíz del proyecto, identificada
por `pyproject.toml` y el paquete `src/tfm_lakehouse`. Se admite únicamente un
archivo `.json` nuevo, hijo directo de `data/checks/environment/`; las rutas
relativas y absolutas equivalentes son válidas. Se rechazan `..`, destinos
existentes —incluidos enlaces duros—, enlaces simbólicos y colgantes, componentes
antecesores enlazados y destinos fuera de esa ubicación. La validación y las
capacidades necesarias se comprueban antes de iniciar Spark, sin crear archivos.
Sin `--report`, no se exige la raíz del proyecto y se conserva la salida por consola.

La nueva ubicación es un cambio deliberado de compatibilidad y queda reservada
exclusivamente para informes de entorno. El antiguo `data/checks/environment.json`
y cualquier evidencia histórica permanecen intactos; no se migran ni se eliminan.
La comprobación de entorno no recibe un TOML ni descubre raíces externas
arbitrarias: no debe configurarse esta ubicación reservada como origen de datos
o almacenamiento de tablas.

Tras completar la comprobación y cerrar Spark, el publicador vuelve a validar
el destino. Recorre los directorios mediante descriptores con `O_NOFOLLOW` y
crea únicamente los directorios reservados que falten. Serializa el JSON completo
antes de escribirlo en un temporal exclusivo y publica el nombre final con
`os.link`, que rechaza un destino creado entretanto. No utiliza `replace` como
alternativa a esa exclusión. Los archivos nuevos tienen permisos `0600` y los
directorios nuevos `0700`; los permisos existentes no se alteran.

Un fallo anterior a la publicación no deja un JSON final parcial. Se intenta
retirar exclusivamente el temporal propio; un fallo de limpieza puede dejar
ese temporal o directorios vacíos y se comunica sin borrar archivos ajenos.
Si falla la limpieza después de publicar, el JSON íntegro se conserva aunque
el comando comunique el fallo. Las primitivas requeridas son de Linux/WSL;
un filesystem que no permita el enlace exclusivo hace fallar la publicación.

Estas protecciones reducen carreras de enlaces y sobrescrituras accidentales.
No impiden que otro proceso con permisos renombre directorios abiertos o manipule
deliberadamente el temporal. Tampoco garantizan durabilidad frente a un apagado:
la publicación visible completa no equivale a sincronización persistente del
filesystem. La ubicación y protección de la caché Ivy son un contrato distinto.

## AUD-09: riesgo residual de dependencias JVM

**Estado provisional:** `AUD-09 — Evaluado; tratamiento propuesto; aceptación del riesgo residual pendiente.`

Registro documental del 31/10/2026, sobre la revisión base
`50455ce02d3c7a8c26232a965a302bc4679f240b`. La autorización para documentarlo
no acepta el riesgo ni cierra AUD-09. Los autores deben decidir expresamente.
El [registro principal de hitos](milestones.md) remite a esta justificación.

### Evidencia reutilizada y alcance

La auditoría estática anterior inventarió 255 archivos JAR: 252 distribuidos con
PySpark y tres de Delta/ANTLR en Ivy. Se reutiliza ese inventario y se contrastan
los tres artefactos afectados, sus metadatos internos y hashes, sin repetir el
inventario completo. La integridad de los 255 JAR se verifica mediante hashes;
esto no implica una nueva auditoría de todas sus dependencias.

El entorno observado es Python 3.11.17, PySpark 3.5.7, Delta 3.3.3 y Java
17.0.20.1. Los pines de `requirements.lock` y `pyproject.toml` fijan las
dependencias Python directas; no constituyen un bloqueo criptográfico completo
de todos los JAR transitivos. Los hashes son evidencia puntual, no un control
automático de seguridad de cada arranque ni una firma del proveedor.

Los tres archivos siguientes están bajo
`.venv/lib/python3.11/site-packages/pyspark/jars/`:

| ID | Artefacto y versión del componente | Evidencia local de presencia |
| --- | --- | --- |
| C1 | `commons-compress-1.23.0.jar`: Commons Compress 1.23.0 | `META-INF/maven/org.apache.commons/commons-compress/pom.properties`, manifiesto y clases DUMP/Pack200 |
| C2 | `hadoop-client-runtime-3.3.4.jar`: Commons Compress 1.21 reubicado | Su propio `pom.properties` de Commons Compress y clases bajo `org/apache/hadoop/shaded/org/apache/commons/compress/` |
| Z1 | `zookeeper-3.6.3.jar`: ZooKeeper 3.6.3 | `META-INF/maven/org.apache.zookeeper/zookeeper/pom.properties`, manifiesto y clases de servidor/quorum/observadores |

SHA-256 contrastados con el inventario anterior:

```text
C1  c267f17160e9ef662b4d78b7f29dca7c82b15c5cff2cb6a9865ef4ab3dd5b787
C2  9377a68071137ae5f5c8cdc2ed20d6f904a1df4d06df26b47ed8872a1b0d8d47
Z1  ac44de96d37a80d463c1e01aaa2ea86b19490fa17a37438d14491456c0e4a911
```

El lanzador ofrece esos JAR en el classpath estándar. Esto no prueba que se
cargaran las clases afectadas ni que se ejecutara una función vulnerable. No se
inició una JVM ni se midió dinámicamente `CodeSource`. Las 452 pruebas funcionales
de AUD-06/AUD-07 son un antecedente separado, no una prueba de seguridad ni una
nueva ejecución atribuida a esta revisión documental.

### Matriz de vulnerabilidades y condiciones

| CVE | Rango afectado y copias locales | Función, entrada y consecuencia | Primera corrección publicada |
| --- | --- | --- | --- |
| CVE-2024-25710 | 1.3–1.25.0; C1 y C2 | Lector DUMP con entrada corrupta: bucle y denegación de servicio | 1.26.0 |
| CVE-2024-26308 | ≥1.21 y <1.26.0; C1 y C2 | Desempaquetado Pack200 malformado: agotamiento de memoria | 1.26.0 |
| CVE-2023-44981 | Anteriores a 3.7.0; 3.7.0–3.7.1; 3.8.0–3.8.2; 3.9.0; Z1 | Quorum accesible con `quorum.auth.enableSasl=true` e identidad sin parte de instancia: bypass de autorización y acceso de lectura/escritura al árbol | 3.7.2, 3.8.3 o 3.9.1 |
| CVE-2023-42503, comprobación complementaria | ≥1.22 y <1.24.0; C1 afectado, C2 fuera del rango | Lectura TAR con tiempos PAX malformados: consumo excesivo de CPU | 1.24.0 |
| CVE-2024-23944, comprobación complementaria | 3.6.0–3.7.2; 3.8.0–3.8.3; 3.9.0–3.9.1; Z1 | Servidor y observador persistente sobre padre accesible: notificaciones sin ACL suficiente revelan rutas descendientes, no contenido | 3.8.4 o 3.9.2 |

Fuentes oficiales consultadas el 31/10/2026:
[Apache Commons Compress](https://commons.apache.org/proper/commons-compress/security.html),
[ZooKeeper: CVE-2023-44981](https://zookeeper.apache.org/security/#cve-2023-44981) y
[ZooKeeper: CVE-2024-23944](https://zookeeper.apache.org/security/#cve-2024-23944).
Las versiones de la última columna identifican la primera corrección de cada
aviso, no una combinación de actualización recomendada ni una garantía global.

### Alcanzabilidad, incertidumbres y riesgo residual

La presencia de código afectado está confirmada. La auditoría estática no
identificó una cadena desde el flujo CSV/Delta implementado hasta las funciones
vulnerables. No se intentó explotación y no se afirma imposibilidad absoluta.

- **C1/C2, DUMP y Pack200:** el proyecto selecciona lectores CSV explícitos y no
  llama a esos parsers. Java 17 no elimina el código Pack200 propio de Commons
  Compress. La restricción propuesta es no introducir ese procesamiento.
- **C1, TAR:** Spark y Hive sí disponen de utilidades TAR; el flujo del proyecto
  no solicita extracción mediante ellas. Su disponibilidad impide describirlo
  como un formato técnicamente bloqueado. La restricción propuesta es no leer
  TAR no confiable con funciones afectadas. C2 no se clasifica como afectado por
  este aviso concreto; sigue afectado por los dos anteriores.
- **Z1, quorum:** no hay configuración del proyecto que inicie servidores o
  habilite autenticación SASL entre peers. Esto no equivale a recomendar quitar
  autenticación de un despliegue existente.
- **Z1, observadores persistentes:** no se encontró uso de `addWatch` ni un
  servicio ZooKeeper en el flujo. Esta condición no depende de SASL de quorum;
  introducir un cliente o servidor, incluso externo al proyecto, exige revisar
  también este aviso.

Para todas las filas, el riesgo residual consiste en conservar código vulnerable
disponible y en que un cambio de entradas, configuración o funcionalidad active
una ruta hoy no identificada. Los controles y compromisos de los apartados
siguientes se aplican a las cinco filas, con las restricciones de formato y
servicio indicadas arriba. No se asigna una probabilidad numérica.

El análisis de referencias y configuración no es un grafo exhaustivo frente a
reflexión, plugins o modificaciones del lanzador. No se autenticaron firmas
externas de los artefactos ni se auditó toda la red del host. La ausencia de una
JVM relevante en la instantánea previa no acredita ausencia permanente de
servicios. Estas limitaciones se mantienen; los dos avisos complementarios no
identifican por sí solos una nueva ruta explotable en el perfil actual.

### Controles existentes y compromisos operativos propuestos

**Comprobado en la implementación:** el gestor exige master local; configura
Delta y desactiva la UI. Bronze valida el contenido CSV y usa `.csv()` con
comprobación posterior de la fuente local exacta. Se aplican las protecciones
de rutas y arranque Ivy ya documentadas. H03.2/H03.3 utilizan lectores Python
sin JVM. Ninguno de estos controles corrige los bytes vulnerables.

**Condiciones de la propuesta, no una lista técnicamente impuesta por el código:**

1. Mantener ejecución local controlada y procesamiento por lotes; no exponer
   servicios deliberadamente a redes externas.
2. Usar lectura CSV explícita y fuentes verificadas para la tarea autorizada.
   Un SHA-256 coincidente identifica bytes; no demuestra inocuidad del contenido.
3. No añadir extracción DUMP o Pack200 ni lectura TAR no confiable mediante
   funciones afectadas. El proyecto no implementa una prohibición universal de
   formatos o de opciones adicionales de Spark.
4. No introducir servidores ZooKeeper gestionados por el proyecto ni servicios
   HA basados en quorum; revisar cualquier nueva conexión de coordinación.
5. Mantener los pines y artefactos inventariados; controlar acceso a entradas,
   configuraciones y classpath. Ese control de acceso es una obligación de los
   operadores, no una ACL nueva aplicada por este incremento.

`local[2]` y la UI desactivada no garantizan aislamiento de red. Las restricciones
anteriores tampoco autorizan automáticamente descargas, ejecuciones reales o
modificaciones de dependencias.

### Disparadores de reevaluación previa

Reevaluar aplicabilidad y compatibilidad **antes** de cualquiera de estos cambios:

- Actualizar Java, Python, Spark, Delta o Hadoop.
- Modificar el classpath, sus JAR, dependencias o plugins.
- Incorporar formatos comprimidos adicionales o extracción de archivos.
- Procesar entradas de fuentes no confiables o cambiar la verificación de fuentes.
- Pasar de ejecución local a distribuida.
- Introducir ZooKeeper, HA u otros mecanismos de coordinación, propios o externos.
- Exponer servicios a redes externas.
- Conocer un nuevo aviso que afecte al flujo utilizado o cambiar el modelo de amenazas.

Si se identifica una ruta relevante o una incertidumbre material, suspender la
propuesta de cierre y delimitar una evaluación específica. No actualizar JAR
aislados: sustituir C1 no elimina C2. Una corrección requiere estudiar también
Hadoop y repetir las verificaciones pertinentes de arranque, contextos, Ivy,
Delta, publicación Bronze, recuperación y reproducibilidad, con autorización.

### Propuesta de decisión para los autores — pendiente

**Riesgos cuya aceptación se propone:** presencia de las versiones afectadas y
sus impactos condicionados, manteniendo exclusivamente el perfil descrito.
La justificación es la ausencia de rutas identificadas desde ese flujo, no la
seguridad intrínseca de las bibliotecas ni el éxito de pruebas funcionales.

**Condiciones y límites:** conservar las restricciones operativas anteriores,
revisar los disparadores antes de cambiar el sistema y registrar cualquier
incumplimiento. La propuesta no se extiende a clústeres, servicios públicos,
formatos adicionales o fuentes no confiables. No se propone una actualización
inmediata de toda la plataforma basada solo en el inventario.

**Responsables propuestos:** Carlos mantiene el inventario y coordina la
reevaluación técnica; Irantzu revisa el alcance y procedencia de las fuentes.
La aceptación exige una decisión expresa conjunta de Carlos e Irantzu.

| Campo de decisión | Estado |
| --- | --- |
| Decisión de los autores | Pendiente |
| Revisión y perfil expresamente aceptados | Pendiente de ratificación |
| Fecha de aceptación | No otorgada |
| Aprobación de Carlos e Irantzu | Pendiente |
| Obligaciones futuras | Mantener restricciones y reevaluar antes de los disparadores, si se acepta |

La autorización de este texto y su eventual versionado no sustituyen esa
decisión. **AUD-09 permanece evaluado, con tratamiento propuesto y aceptación
del riesgo residual pendiente; no se declara cerrado.**

## Configuración Bronze

Usar `--config` explícitamente. Todas las rutas relativas se resuelven respecto
al TOML; las rutas de fuentes se resuelven respecto a raw_root.

| Campo | Significado |
| --- | --- |
| paths.raw_root | Raíz local de CSV originales |
| paths.lakehouse_root | Raíz separada de salidas |
| dataset.kind | Obligatorio: real o synthetic |
| dataset.reference | Declaración de procedencia y versión, no comprobación remota |
| sources.electricity | Un CSV ancho con timestamp y al menos un edificio |
| sources.metadata | Un CSV con building_id |
| sources.weather | Opcional: un CSV con timestamp y site_id |
| spark.master | local[2] por defecto; solo ejecución local |
| spark.ivy_dir | Opcional, caché de dependencias Java |

No se permiten fuentes fuera de raw_root, entradas idénticas, solapamientos entre
raw y salidas ni alias mediante enlaces en los destinos bajo lakehouse_root.
También se rechazan alias internos entre tablas, informes o namespaces.
Los namespaces `real/` y `synthetic/` mantienen separadas las salidas; dataset.kind es una
declaración del operador y no certifica por sí misma que un archivo sea BDG2.

Variables de entorno: JAVA_HOME/PATH para Java/uv; PYSPARK_PYTHON se establece
temporalmente por la aplicación. `TFM_REAL_CONFIG` habilita la prueba opcional
real y también configura `master`, Ivy y raíces protegidas de la fixture Spark
compartida por las integraciones. Excluir `real_data` no anula ese segundo
efecto. La suite sintética rutinaria debe ejecutarse con la variable ausente:
una configuración real puede cambiar sus opciones de arranque y acceder a
fuentes reales si además se selecciona la prueba correspondiente. No se cambia
la fixture ni se considera la variable una autorización. No se carga .env ni
se requieren credenciales.

## Contrato de entrada y conservación

Una fuente es un archivo local UTF-8 (BOM admitido), separado por comas, con cabecera
y comillas/escape CSV estándar mediante comillas dobles. Se admite texto multilínea.
No se aceptan directorios, patrones ni nombres de ruta con metacaracteres de glob,
ni punteros Git LFS: Spark podría interpretar un nombre literal como un patrón.

Se valida todo el flujo UTF-8 y CSV antes de leerlo con Spark, incluida la anchura
de cada registro. El conteo independiente debe coincidir con Spark y sus archivos
de entrada deben corresponder exactamente al CSV previsto.
Solo se exigen timestamp y al menos una columna de edificio para electricidad,
building_id para metadatos, y timestamp y site_id para meteorología. Se rechazan cabeceras vacías, duplicadas incluso
sin distinguir mayúsculas, el prefijo reservado _bronze_ y caracteres incompatibles
con Delta sin column mapping. No se renombran columnas de forma implícita.

Todas las columnas de origen son `string nullable`: sin inferencia numérica o temporal.
Se conserva orden de columnas, ceros iniciales, espacios, textos NaN, identificadores,
duplicados, ceros, negativos y extremos. Los campos CSV sin valor se representan
como null según el lector de Spark; líneas físicas vacías se omiten. Las comillas
de escape se interpretan como sintaxis CSV. La copia byte a byte permanece en raw;
la tabla Delta representa registros CSV, no su formato físico.

No se asigna una unidad a columnas ni se aplica limpieza, filtrado, unpivot, unión
o deduplicación. La [documentación oficial de medidores](https://github.com/buds-lab/building-data-genome-project-2/wiki/Meters-data-features)
describe raw con energía en kWh, frecuencia horaria y hora local; distingue raw,
cleaned y kaggle. Cualquier archivo incorporado debe corresponder a la variante
declarada antes de usar esas unidades en Silver. En particular no debe sustituirse
raw por cleaned o kaggle sin justificarlo.

La [documentación de metadatos](https://raw.githubusercontent.com/wiki/buds-lab/building-data-genome-project-2/Metadata-features.md)
identifica building_id, site_id, sqm y timezone, entre otros atributos. No se fija
un esquema Silver definitivo. Las cabeceras se obtienen de los CSV locales
recibidos; una URL de GitHub que devuelva un puntero LFS no sirve como entrada.

Meteorología conserva sus columnas y valores originales sin asignar unidades,
normalizar tiempos ni vincular registros a edificios. Esa integración y la
interpretación de ausencias o códigos especiales corresponden a Silver.

## Salida, estrategia de escritura y trazabilidad

Tablas locales:

- `<lakehouse_root>/<kind>/bronze/electricity`.
- `<lakehouse_root>/<kind>/bronze/metadata`.
- `<lakehouse_root>/<kind>/bronze/weather`, si se configura.
- Informes: `<lakehouse_root>/<kind>/_runs/<run_id>.json`.

Cada tabla incorpora cuatro columnas de texto, además de todas las originales:

| Columna | Contenido |
| --- | --- |
| _bronze_source_uri | URI absoluta del CSV recibido |
| _bronze_source_sha256 | Huella SHA-256 del contenido recibido |
| _bronze_data_kind | real o synthetic |
| _bronze_dataset_reference | Referencia declarada en la configuración |

Cada entrada se materializa en caché DISK_ONLY con todos los campos y con
FAILFAST; se desactiva temporalmente el column pruning del parser. Esto permite
detectar filas cortas o con campos adicionales incluso durante los conteos.
Todas las fuentes configuradas se validan antes del primer commit. Las fuentes
deben permanecer inmutables durante la ejecución; se revalida su SHA-256 antes de escribir.
Un bloqueo local por namespace impide dos ingestas simultáneas de esta aplicación;
no coordina escritores Delta ajenos al proceso.

Se usa `overwrite` con `overwriteSchema=true` como recarga completa de la fuente
configurada. Repetir una carga mantiene las mismas filas y valores, aunque crea
una versión Delta nueva. No se hace append ni deduplicación; los duplicados del
CSV original también permanecen. Un CSV más corto reemplaza completamente el snapshot
anterior; no se deben pasar fragmentos esperando una carga incremental.

No se sobrescriben destinos existentes que no sean Delta. Un archivo sin cabecera,
vacío, con solo cabecera o malformado hace fallar la carga: nunca vacía silenciosamente
una tabla anterior. Se guardan informes de error, incluidos commits parciales.

Delta garantiza el commit por tabla. **No hay transacción conjunta** entre fuentes:
un fallo intermedio puede dejar distintas ejecuciones en las versiones más recientes
de las tablas. El informe conserva `cross_table_atomic=false`.

La consulta coherente se obtiene mediante la publicación: solo se escribe un informe
`status=passed` después de verificar todas las versiones y completar la limpieza.
El JSON se publica mediante sustitución atómica. Cada commit guarda el identificador
de ejecución en userMetadata; se identifica ese commit, no una versión posterior de
otro escritor. Los errores de limpieza no ocultan la causa principal y se intenta
guardar el informe aunque falle la liberación de cachés o la restauración de Spark.

La finalización satisfactoria se comprueba expresamente: deben haberse verificado
todas las tablas configuradas y terminado correctamente la liberación de cachés
y la restauración interna de Spark. La mera ausencia de una excepción ordinaria
capturada no basta. Una cancelación anterior a la publicación se registra como
`status=failed`, manteniendo `report_contract_version=1`, los estados parciales
observados y la primera causa diagnóstica. El campo opcional `interruption`
identifica tipo, mensaje y fase de la primera cancelación; los errores posteriores
se conservan como incidencias de limpieza o notas, sin sustituir la causa original.

Los errores ordinarios mantienen `IngestionError` y su causa. `KeyboardInterrupt`
y `SystemExit` se propagan como la misma excepción —conservando el código de
salida— después de los intentos de finalización. Si una cancelación ocurre durante
la limpieza de un error ordinario, se propaga la cancelación y se conserva aquel
error como causa y diagnóstico. Se protegen también los finalizadores del temporal
y del bloqueo. No se inventan versiones Delta ni se simula una reversión cuando
una escritura se interrumpe.

La sustitución atómica del temporal por el manifiesto final es la frontera de
publicación. Una cancelación anterior no puede publicar una ejecución incompleta
como `passed`; si falta el manifiesto final, se admite un único intento adicional
de registrar el fallo. Solo una ausencia confirmada permite ese intento: un error
al inspeccionar el destino no se interpreta como ausencia. Si el manifiesto ya
se publicó íntegramente, una cancelación posterior no lo degrada ni lo invalida
retrospectivamente. Por ello, el llamador puede recibir una cancelación después
de una publicación válida. `passed` acredita el snapshot y su limpieza interna,
no el cierre posterior de la sesión externa o la salida completa de la CLI.

Ante fallos que impidan publicar, terminación forzosa, caída del proceso o
interrupciones repetidas, no se garantiza generar un informe ni completar la
limpieza. El lector continúa rechazando publicaciones incompletas y utilizando
la publicación anterior compatible cuando un intento posterior consta como fallido.

`inspect-bronze` selecciona la publicación satisfactoria más reciente compatible
con kind, referencia y conjunto de fuentes. Lee cada tabla mediante versionAsOf,
verifica la identidad del commit, el esquema y el conteo y comunica si usa una
ejecución anterior por un fallo posterior. Si falta una versión publicada o no coincide,
falla expresamente. No sustituye la versión solicitada por la última disponible.

Para recuperar una carga fallida, corregir la causa y repetir la carga completa.
No borrar tablas ni ejecutar VACUUM sobre las versiones necesarias para recuperación.
Los informes antiguos sin contrato de publicación se conservan como historial;
requieren una nueva ingesta completa para ser consultados con este lector.
Añadir weather también requiere una publicación nueva con las tres fuentes:
una ejecución anterior de dos tablas no satisface esa configuración.

El informe contiene inicio/fin, referencia, huellas, tamaño, esquemas de entrada/salida,
conteos, rutas, versiones Delta, resultado de lectura posterior y errores. Estos
informes registran operaciones de software, no valoraciones de calidad energética.

## Ejecución y consulta

```bash
.venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.synthetic.toml
.venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.synthetic.toml
# También queda instalado el comando:
.venv/bin/tfm-lakehouse --help
```

inspect-bronze devuelve conteos, esquemas y hasta cinco filas por tabla. Ese límite
solo se aplica a la muestra mostrada: no trunca la ingesta. Una consulta propia puede
usar `read_bronze_snapshot(spark, config)` dentro de spark_session para obtener
los DataFrames del mismo conjunto publicado. Leer directamente la última versión
de cada ruta no ofrece esta garantía. No se necesita catálogo Hive ni se recopila
el conjunto completo en memoria del driver.

Errores de configuración, lectura, JVM o persistencia producen código de salida 1
en la CLI y un traceback en stderr. La ingesta, cuando puede crear el directorio
de informes, registra además el error en JSON. Los procesos que no llegan a iniciar
Spark solo pueden producir el registro CLI.

## Incorporación de BDG2

1. Obtener los archivos seleccionados de la
   [publicación de autores en Zenodo v1.0](https://zenodo.org/records/3887306)
   o del [repositorio oficial](https://github.com/buds-lab/building-data-genome-project-2).
   No se ha descargado el conjunto durante esta intervención.
2. Mantener la estructura original y los archivos inalterados:
   `data/raw/bdg2/data/meters/raw/electricity.csv`,
   `data/raw/bdg2/data/metadata/metadata.csv` y, si se configura,
   `data/raw/bdg2/data/weather/weather.csv`.
   Si provienen de GitHub/LFS, asegurarse de tener CSV reales y no punteros.
3. Copiar config/bronze.example.toml a una configuración local, adaptar raw_root y
   dataset.reference con versión/revisión y URL realmente usadas.
   Reexaminar las cabeceras y tipos originales; no asumir que los fixtures
   representan todas las columnas ni todas las incidencias del conjunto real.
4. Conservar el alcance de una posible extracción de prueba en un manifiesto:
   archivo original y SHA, filas/edificios/periodo elegidos, comando de extracción,
   huella del subconjunto y limitaciones. Guardar el subconjunto en otra carpeta;
   nunca editar el archivo original ni inventar o completar mediciones.
5. Ejecutar con esa configuración:

```bash
.venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.example.toml
.venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.example.toml
TFM_REAL_CONFIG=config/bronze.example.toml .venv/bin/python -m pytest -m real_data -q
```

El ejemplo requiere las tres fuentes; se puede omitir sources.weather para una
carga de dos. La prueba real_data valida y cuenta por completo las fuentes configuradas,
usando su master y caché Ivy. No sustituye la ejecución de ingesta ni demuestra
validez analítica o rendimiento.

Desde PowerShell, sustituir la ruta de ejemplo por la ubicación del repositorio
dentro de Ubuntu/WSL y ejecutar cada comando `wsl` en una sola línea, sin
continuaciones de línea de Bash:

```powershell
$RepositoryWslPath = '/ruta/al/repositorio/tfm-lakehouse'
wsl -d Ubuntu --cd $RepositoryWslPath -- .venv/bin/python -m tfm_lakehouse ingest-bronze --config config/bronze.example.toml
wsl -d Ubuntu --cd $RepositoryWslPath -- .venv/bin/python -m tfm_lakehouse inspect-bronze --config config/bronze.example.toml
```

## Pruebas y criterios de aceptación

```bash
env -u TFM_REAL_CONFIG .venv/bin/python -B -m pytest -m unit -q
env -u TFM_REAL_CONFIG .venv/bin/python -B -m pytest -m environment -q
env -u TFM_REAL_CONFIG .venv/bin/python -B -m pytest -m integration -q
env -u TFM_REAL_CONFIG .venv/bin/python -B -m pytest -m "not real_data and not performance" -q
```

unit no inicia una JVM. environment prueba persistencia Delta efectiva. integration
usa exclusivamente copias/archivos sintéticos en directorios temporales independientes.
real_data requiere autorización; su exclusión explícita evita habilitarla por
accidente aunque una variable estuviera definida. Si no se define, esa prueba
también dispone de su propia omisión preventiva.
El marcador performance está reservado; no hay benchmarks implementados.

### Antecedente: regresiones AUD-05/AUD-08

Para las regresiones AUD-05/AUD-08 del 31/10/2026 se excluyeron expresamente
`real_data`, `performance` y estas dos pruebas, que entonces inicializaban
repositorios Git temporales:

- `tests/test_characterization.py::test_real_output_ignore_is_checked_from_destination_not_external_config`.
- `tests/test_weather_characterization.py::test_real_output_git_ignore_guard_uses_only_synthetic_files`.

La selección completa sin JVM obtuvo 423 pruebas superadas. La suite permitida
con JAR locales temporales obtuvo 449 pruebas superadas y tres excluidas
(las dos anteriores y una `real_data`). Incluye integraciones sintéticas de
Bronze/Delta y la comprobación de entorno; las pruebas nuevas del gestor usan
dobles. Se sustituyó únicamente el helper de resolución por `spark.jars` con
copias de los tres JAR ya disponibles y `spark.jars.packages=""`. El gestor,
sus bloqueos y el default Ivy se ejecutaron realmente. Los homes de Java y
Python coincidieron y se verificó la ruta efectiva transmitida a Spark.
No se revalidó la resolución Maven ni se ejecutó BDG2 real en aquel incremento.
Las cifras anteriores son históricas y no se atribuyen a AUD-06/AUD-07.

### Perfil actual y consultas Git de solo lectura (AUD-06/AUD-07)

Los dos casos anteriores ahora utilizan dobles estrictos de Git: verifican
comando, argumentos, directorio, entorno y la secuencia rechazo/aceptación/error
sobre el mismo destino, sin repositorios nuevos. No deben excluirse. Una prueba
complementaria consulta el checkout existente: `check-ignore --no-index` verifica
las reglas y `ls-files` comprueba por separado que los datos privados no estén
versionados. Los controles de fixtures y código detectan exclusiones excesivas.
Los hashes requeridos por las fixtures H03.3 siguen siendo exclusivamente
sintéticos. Si falta Git, esa comprobación adicional declara su omisión; los
dos casos con dobles siguen ejecutándose.

La verificación AUD-06/AUD-07 del 31/10/2026, sobre la base `19cab0a` con los
cambios autorizados en pruebas y documentación, obtuvo los resultados siguientes.
No modifica las cifras históricas anteriores ni acredita una ejecución BDG2.

| Selección ejecutada | Superadas | Fallidas | Excluidas por selección |
| --- | --- | --- | --- |
| Dos casos corregidos y política Git del checkout | 3 | 0 | 0 |
| H03.2 + H03.3 completos | 163 | 0 | 0 |
| Selección completa `unit` | 426 | 0 | 27, fuera del marcador seleccionado |
| Suite general con el perfil temporal siguiente | 452 | 0 | 1, `real_data` |

No hay pruebas `performance` implementadas. Las dos pruebas corregidas se
ejecutaron sin exclusiones específicas; la suite general tardó 48,12 segundos,
sin considerarlo un benchmark. Las comprobaciones independientes fueron de solo
lectura; la revisión de tests añadió 20 sondeos de los dobles en memoria.

### Perfil temporal sin resolución Maven

Desde la raíz del repositorio en WSL, con Java y los tres JAR ya disponibles,
el siguiente procedimiento ejecuta la suite sintética sin descargar dependencias
ni escribir en la caché Ivy histórica. Si falta un JAR, termina antes de pytest;
no intenta instalarlo. El cambio del helper existe solo en ese proceso Python,
no en el código de producción. Las copias de JAR y tablas viven en un directorio
temporal; el gestor Spark real conserva sus validaciones y bloqueos. La prueba
unitaria del helper original comprueba por separado la coordenada Delta fijada.
Este perfil verifica ejecución con JAR locales, no resolución Maven.

```bash
env -u TFM_REAL_CONFIG .venv/bin/python -B - <<'PY'
import hashlib
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import pytest
from tfm_lakehouse import spark as manager

os.environ["GIT_OPTIONAL_LOCKS"] = "0"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
names = (
    "io.delta_delta-spark_2.12-3.3.3.jar",
    "io.delta_delta-storage-3.3.3.jar",
    "org.antlr_antlr4-runtime-4.9.3.jar",
)
cache = Path.home() / ".ivy2" / "jars"
for name in names:
    source = cache / name
    if source.is_symlink() or not source.is_file():
        raise RuntimeError(f"Required local JAR unavailable: {name}")

with TemporaryDirectory(prefix="tfm-synthetic-tests-") as temporary:
    root = Path(temporary)
    copies = []
    for name in names:
        source, target = cache / name, root / name
        shutil.copyfile(source, target)
        assert hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(target.read_bytes()).digest()
        copies.append(str(target))

    def local_jars(builder):
        return builder.config("spark.jars", ",".join(copies)).config("spark.jars.packages", "")

    manager.configure_spark_with_delta_pip = local_jars
    status = pytest.main([
        "-q", "-m", "not real_data and not performance",
        "--basetemp", str(root / "pytest"),
    ])
    raise SystemExit(status)
PY
```

Se comprueban: fuente válida/inexistente, cabeceras, cero bytes, solo cabecera,
conservación exacta de valores/columnas (incluidos duplicados), linaje, persistencia,
recuperación, recarga repetida, sustitución por snapshot menor, entradas malformadas,
protección de tablas ante validación fallida y registro de escritura parcial.
Las regresiones incluyen alias de rutas, glob literales, UTF-8 y comillas inválidas
en el cuerpo, diferencias de conteo, errores de limpieza/publicación, escritores
ajenos y recuperación del conjunto anterior tras una carga parcial.
No se modifican datos reales para probar errores.

## Git y límites actuales

.venv, data, salidas Delta, caché Ivy local, temporales Spark, metadatos de construcción
y logs se excluyen de Git. Los fixtures pequeños identificados como sintéticos y las
pruebas y documentación sí se versionan. Los informes JSON/XML y logs de ejecución
se guardan localmente bajo data/checks o data/lakehouse/<kind>/_runs y están ignorados.
El historial y la publicación del repositorio los gestionan los autores.

La [transformación preliminar H05.1](hitos-5.md#h051--transformación-técnica-preliminar)
ya conserva la electricidad en formato largo y está probada con datos sintéticos;
no interpreta valores o tiempos ni persiste Silver. Siguen pendientes la aprobación
conjunta de selección y contratos, la interpretación y validación analítica,
unidades y zonas verificadas por fuente, persistencia Silver, reglas de calidad,
integración meteorológica, Gold, explotación mínima y evaluación de rendimiento.
No hay streaming, predicción, cloud ni control operativo de instalaciones.
