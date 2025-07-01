# server.py
from mcp.server.fastmcp import FastMCP
from google.cloud import bigquery

# Create an MCP server
mcp = FastMCP("Firebase MCP Server", "1.0.0")


@mcp.tool()
def listar_proyectos() -> str:
    """Lista todos los proyectos disponibles en BigQuery"""
    try:
        client = bigquery.Client()
        proyectos = []

        # Obtener información del proyecto actual
        proyecto_actual = client.project
        proyectos.append(f"Proyecto actual: {proyecto_actual}")

        return f"Proyectos disponibles:\n" + "\n".join(proyectos)
    except Exception as e:
        return f"Error al listar proyectos: {str(e)}"


@mcp.tool()
def listar_datasets(project_id: str = "") -> str:
    """Lista todos los datasets en un proyecto específico"""
    try:
        client = bigquery.Client()
        if not project_id:
            project_id = client.project

        datasets = client.list_datasets(project_id)
        dataset_list = []

        for dataset in datasets:
            dataset_list.append(f"- {dataset.dataset_id}")

        if not dataset_list:
            return f"No se encontraron datasets en el proyecto {project_id}"

        return f"Datasets en el proyecto '{project_id}':\n" + "\n".join(dataset_list)
    except Exception as e:
        return f"Error al listar datasets: {str(e)}"


@mcp.tool()
def listar_tablas(dataset_id: str, project_id: str = "") -> str:
    """Lista todas las tablas en un dataset específico"""
    try:
        client = bigquery.Client()
        if not project_id:
            project_id = client.project

        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = client.list_tables(dataset_ref)
        table_list = []

        for table in tables:
            table_info = f"- {table.table_id} ({table.table_type})"
            if hasattr(table, "num_rows") and table.num_rows is not None:
                table_info += f" - {table.num_rows:,} filas"
            table_list.append(table_info)

        if not table_list:
            return f"No se encontraron tablas en el dataset {dataset_id}"

        return f"Tablas en '{project_id}.{dataset_id}':\n" + "\n".join(table_list)
    except Exception as e:
        return f"Error al listar tablas: {str(e)}"


@mcp.tool()
def describir_tabla(dataset_id: str, table_id: str, project_id: str = "") -> str:
    """Obtiene el esquema y metadatos de una tabla específica"""
    try:
        client = bigquery.Client()
        if not project_id:
            project_id = client.project

        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table = client.get_table(table_ref)

        # Información básica de la tabla
        info = [
            f"Tabla: {project_id}.{dataset_id}.{table_id}",
            f"Tipo: {table.table_type}",
            f"Creada: {table.created}",
            f"Modificada: {table.modified}",
            f"Filas: {table.num_rows:,}" if table.num_rows else "Filas: No disponible",
            (
                f"Tamaño: {table.num_bytes:,} bytes"
                if table.num_bytes
                else "Tamaño: No disponible"
            ),
            "",
            "Esquema:",
        ]

        # Schema de la tabla
        for field in table.schema:
            mode = f" ({field.mode})" if field.mode != "NULLABLE" else ""
            description = f" - {field.description}" if field.description else ""
            info.append(f"  - {field.name}: {field.field_type}{mode}{description}")

        return "\n".join(info)
    except Exception as e:
        return f"Error al describir tabla: {str(e)}"


@mcp.tool()
def ejecutar_consulta(query: str, max_results: int = 100) -> str:
    """Ejecuta una consulta SQL en BigQuery y devuelve los resultados"""
    try:
        client = bigquery.Client()

        # Validar que la consulta no esté vacía
        if not query.strip():
            return "Error: La consulta SQL está vacía."

        # Configurar el job de consulta
        job_config = bigquery.QueryJobConfig()
        job_config.maximum_bytes_billed = (
            100 * 1024 * 1024
        )  # Límite de 100MB para seguridad
        job_config.dry_run = False

        # Ejecutar la consulta
        query_job = client.query(query, job_config=job_config)

        # Esperar a que termine la consulta
        results = query_job.result(max_results=max_results)

        # Verificar si hay errores en el job
        if query_job.errors:
            error_messages = [error["message"] for error in query_job.errors]
            return f"Errores en la consulta:\n" + "\n".join(error_messages)

        # Procesar resultados
        if results.total_rows == 0 or results.total_rows is None:
            return "✓ Consulta ejecutada exitosamente, pero no devolvió resultados."

        # Obtener nombres de columnas
        columns = [field.name for field in results.schema]

        # Formatear resultados
        output = []
        output.append(f"✓ Consulta ejecutada exitosamente")

        # Manejar total_rows que puede ser None
        total_rows = results.total_rows if results.total_rows is not None else 0
        showing_rows = min(max_results, total_rows)

        output.append(
            f"Resultados (mostrando {showing_rows:,} de {total_rows:,} filas):"
        )
        output.append("=" * 60)

        # Encabezados
        header = " | ".join(
            f"{col:15}" for col in columns[:8]
        )  # Limitar a 8 columnas para legibilidad
        if len(columns) > 8:
            header += " | ..."
        output.append(header)
        output.append("-" * len(header))

        # Datos
        for row in results:
            row_values = []
            for i, col in enumerate(columns[:8]):
                value = row[col]
                # Formatear valores None/NULL
                if value is None:
                    formatted_value = "NULL"
                elif isinstance(value, (int, float)):
                    formatted_value = (
                        f"{value:,}" if isinstance(value, int) else f"{value:.2f}"
                    )
                else:
                    formatted_value = str(value)[:15]  # Truncar strings largos
                row_values.append(f"{formatted_value:15}")

            row_data = " | ".join(row_values)
            if len(columns) > 8:
                row_data += " | ..."
            output.append(row_data)

        # Información adicional
        output.append("=" * 60)
        output.append(f"📊 Estadísticas:")
        output.append(f"   • Total de filas: {total_rows:,}")
        output.append(f"   • Columnas: {len(columns)}")
        if query_job.total_bytes_processed is not None:
            output.append(f"   • Bytes procesados: {query_job.total_bytes_processed:,}")
        if query_job.ended is not None and query_job.started is not None:
            duration = query_job.ended - query_job.started
            output.append(f"   • Tiempo de ejecución: {duration}")

        return "\n".join(output)

    except Exception as e:
        # Manejo de errores más específico
        error_type = type(e).__name__
        error_msg = str(e)

        # Errores comunes de BigQuery
        if "syntax error" in error_msg.lower():
            return f"❌ Error de sintaxis SQL:\n{error_msg}\n\n💡 Tip: Revisa la sintaxis de tu consulta SQL."
        elif "not found" in error_msg.lower():
            return f"❌ Tabla o dataset no encontrado:\n{error_msg}\n\n💡 Tip: Verifica que el nombre de la tabla sea correcto: `proyecto.dataset.tabla`"
        elif (
            "permission denied" in error_msg.lower()
            or "access denied" in error_msg.lower()
        ):
            return f"❌ Sin permisos para acceder:\n{error_msg}\n\n💡 Tip: Verifica que tengas permisos de lectura en el dataset."
        elif (
            "quota exceeded" in error_msg.lower()
            or "limit exceeded" in error_msg.lower()
        ):
            return f"❌ Límite de recursos excedido:\n{error_msg}\n\n💡 Tip: Intenta limitar tu consulta con LIMIT o filtros WHERE."
        elif "invalid" in error_msg.lower():
            return f"❌ Consulta inválida:\n{error_msg}\n\n💡 Tip: Revisa los nombres de columnas y tipos de datos."
        else:
            return f"❌ Error ({error_type}):\n{error_msg}\n\n💡 Tip: Si el error persiste, verifica la conexión y permisos de BigQuery."


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"
