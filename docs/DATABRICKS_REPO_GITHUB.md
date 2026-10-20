# Conexión de Databricks con el repositorio GitHub

Esta guía describe cómo conectar Databricks con el repositorio del laboratorio, clonar el proyecto desde GitHub y dejarlo listo para ejecutar los notebooks de Bronze, Silver y Gold.

## Repositorio del laboratorio

- URL del repositorio: https://github.com/danny-salas_bac/Laboratorio_BAC_SDP.git

## Prerrequisitos

- Acceso a un workspace de Databricks.
- Un cluster activo o disponible para ejecutar notebooks.
- Credenciales válidas de GitHub para acceder al repositorio.
- Permisos para crear o gestionar repositorios dentro de Databricks Repos.

## Paso a paso

### 1. Abrir Databricks

1. Inicia sesión en el workspace de Databricks.
2. En el menú lateral, selecciona Repos.
3. Haz clic en Add Repo.

### 2. Conectar el repositorio GitHub

1. Selecciona GitHub como proveedor de repositorio.
2. Pegue la URL del proyecto:
   - https://github.com/danny-salas_bac/Laboratorio_BAC_SDP.git
3. Cuando Databricks solicite autenticación, usa el token personal de GitHub del usuario autorizado.
4. Confirma la conexión y espera a que termine la clonación del repositorio.

> Importante: no guardes ni compartas el token en archivos del proyecto ni en mensajes públicos. Ingrésalo solo en la autenticación de Databricks.

### 3. Validar que el repo quedó clonado

Una vez finalizada la clonación, revisa que se haya creado la estructura del proyecto en Databricks Repos. Deben verse carpetas como:

- notebooks/
- docs/
- jobs/
- bronze/
- silver/
- gold/

### 4. Asegurar el cluster correcto

1. Ve a Compute.
2. Selecciona o crea un cluster activo.
3. Asegúrate de que el cluster esté en ejecución y tenga permisos para:
   - leer repositorios
   - crear esquemas y tablas
   - ejecutar Spark/Delta Lake

### 5. Abrir el proyecto desde el repo

1. En Databricks Repos, abre la carpeta del repositorio clonado.
2. Navega a la carpeta notebooks.
3. Abre el notebook que corresponda a cada capa del laboratorio.

## Orden de ejecución recomendado

1. Bronze
   - Carga la base sintética y publica datos en la capa raw.
2. Silver
   - Construye hubs, satélites y links con el enfoque Data Vault.
3. Gold
   - Genera agregados, resúmenes y KPIs para consumo analítico.

## Validación mínima por capa

### Bronze

Revisa que:

- exista el catálogo y esquema correcto
- las tablas tengan filas
- los conteos sean consistentes
- no haya columnas clave nulas o mal nombradas

### Silver

Revisa que:

- existan los hubs, satélites y links
- las llaves no estén nulas
- las relaciones cliente-deal-transacción estén alineadas
- no haya duplicados en llaves de negocio o surrogate keys

### Gold

Revisa que:

- los resúmenes se creen correctamente
- las agregaciones tengan sentido de negocio
- los KPI no presenten nulos masivos
- las tablas finales queden publicadas en la capa Gold

## Buenas prácticas

- Ejecuta cada notebook en orden y sin saltarte celdas.
- No avances a Gold si Silver no está validada.
- No avances a Silver si Bronze no está correcta.
- Revisa siempre los counts y samples antes de asumir que el flujo fue exitoso.

## Resultado esperado

Con esta conexión, el laboratorio quedará listo para ejecutarse en Databricks desde un repositorio versionado en GitHub, manteniendo el flujo profesional de Bronze, Silver y Gold.
