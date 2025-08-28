import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ================================
# CONFIGURACIÓN DE CREDENCIALES GCP
# ================================

def setup_gcp_credentials():
    """Configuración minimalista de credenciales GCP"""
    try:
        # Cloud Run
        if os.getenv('K_SERVICE'):
            print("🚀 Cloud Run - usando credenciales automáticas")
            return bigquery.Client()
        
        # Local - buscar archivo
        paths = [
            'service-account-key.json',
            './service-account-key.json',
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        ]
        
        for path in paths:
            if path and os.path.exists(path):
                print(f"✅ Credenciales encontradas: {path}")
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
                return bigquery.Client()
        
        # Variable de entorno JSON
        cred_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        if cred_json:
            print("✅ Credenciales desde variable de entorno")
            cred_info = json.loads(cred_json)
            credentials = service_account.Credentials.from_service_account_info(cred_info)
            project_id = cred_info.get('project_id')
            return bigquery.Client(credentials=credentials, project=project_id)
        
        raise Exception("No se encontraron credenciales")
        
    except Exception as e:
        print(f"❌ Error configurando credenciales: {e}")
        print("💡 Crear 'service-account-key.json' en raíz del proyecto")
        raise

# ================================
# CONFIGURACIÓN MINIMALISTA
# ================================

PROJECT_ID = os.getenv("PROJECT_ID", "data-engineer-gcp-469614")
BUCKET = os.getenv("BUCKET", "raw-data-etl-lab")

# Cliente BigQuery con credenciales configuradas
bq = setup_gcp_credentials()

# ================================
# FUNCIONES CORE CON MANEJO DE ERRORES
# ================================

def run_query(sql: str, desc: str = ""):
    """Ejecuta query SQL con manejo de errores"""
    try:
        job = bq.query(sql)
        job.result()
        print(f"✅ {desc}")
        return True
    except Exception as e:
        print(f"❌ Error en {desc}: {str(e)[:100]}...")  # Solo primeros 100 chars del error
        return False

def bronze_layer():
    """Carga datos raw desde GCS a BigQuery"""
    print("🥉 Bronze Layer - Cargando datos...")
    
    # Crear dataset
    if not run_query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.bronze`", "Dataset bronze"):
        return False
    
    # Cargar CSVs desde GCS
    tables = {
        'employees': f"gs://{BUCKET}/landing/employees.csv",
        'products': f"gs://{BUCKET}/landing/products.csv", 
        'sales': f"gs://{BUCKET}/landing/sales.csv"
    }
    
    success = True
    for table, gcs_path in tables.items():
        sql = f"""
        LOAD DATA OVERWRITE `{PROJECT_ID}.bronze.{table}`
        FROM FILES (
            format = 'CSV',
            uris = ['{gcs_path}'],
            skip_leading_rows = 1,
            allow_jagged_rows = false,
            allow_quoted_newlines = false
        )
        """
        if not run_query(sql, f"Cargando {table}"):
            success = False
    
    return success

def silver_layer():
    """Datos limpios y validados"""
    print("🥈 Silver Layer - Limpiando datos...")
    
    # Crear dataset
    if not run_query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.silver`", "Dataset silver"):
        return False
    
    # Transformaciones
    transformations = [
        (f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.employees` AS
        SELECT 
            employee_id,
            TRIM(first_name) as first_name,
            TRIM(last_name) as last_name,
            LOWER(TRIM(email)) as email,
            department,
            salary,
            hire_date
        FROM `{PROJECT_ID}.bronze.employees`
        WHERE email IS NOT NULL AND salary > 0
        """, "Limpiando empleados"),
        
        (f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.products` AS
        SELECT *
        FROM `{PROJECT_ID}.bronze.products`
        WHERE price > 0 AND is_active = true
        """, "Filtrando productos"),
        
        (f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.sales` AS
        SELECT *
        FROM `{PROJECT_ID}.bronze.sales`
        WHERE quantity > 0 AND total_amount > 0
        """, "Validando ventas")
    ]
    
    success = True
    for sql, desc in transformations:
        if not run_query(sql, desc):
            success = False
    
    return success

def gold_layer():
    """Tablas analíticas para BI"""
    print("🥇 Gold Layer - Creando métricas...")
    
    # Crear dataset
    if not run_query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.gold`", "Dataset gold"):
        return False
    
    # Métricas de negocio
    analytics = [
        (f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.gold.sales_analytics` AS
        SELECT 
            s.transaction_id,
            s.sale_date,
            s.quantity,
            s.total_amount,
            p.product_name,
            p.category,
            p.price as unit_price,
            e.first_name || ' ' || e.last_name as sales_rep,
            e.department,
            s.customer_segment,
            s.channel,
            s.region
        FROM `{PROJECT_ID}.silver.sales` s
        LEFT JOIN `{PROJECT_ID}.silver.products` p ON s.product_id = p.product_id
        LEFT JOIN `{PROJECT_ID}.silver.employees` e ON s.sales_rep_id = e.employee_id
        """, "Enriqueciendo ventas"),
        
        (f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.gold.product_metrics` AS
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            COUNT(s.transaction_id) as total_sales,
            SUM(s.quantity) as total_quantity_sold,
            SUM(s.total_amount) as total_revenue,
            ROUND(AVG(s.total_amount), 2) as avg_sale_amount
        FROM `{PROJECT_ID}.silver.products` p
        LEFT JOIN `{PROJECT_ID}.silver.sales` s ON p.product_id = s.product_id
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_revenue DESC
        """, "Métricas de productos"),
        
        (f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.gold.sales_summary` AS
        SELECT 
            EXTRACT(YEAR FROM sale_date) as year,
            EXTRACT(MONTH FROM sale_date) as month,
            channel,
            region,
            COUNT(*) as total_transactions,
            SUM(total_amount) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_transaction_value
        FROM `{PROJECT_ID}.silver.sales`
        GROUP BY year, month, channel, region
        ORDER BY year DESC, month DESC, total_revenue DESC
        """, "Resumen mensual")
    ]
    
    success = True
    for sql, desc in analytics:
        if not run_query(sql, desc):
            success = False
    
    return success

def verify_results():
    """Verifica que el pipeline funcionó correctamente"""
    print("\n🔍 Verificando resultados...")
    
    datasets = ['bronze', 'silver', 'gold']
    for dataset in datasets:
        try:
            # Obtener lista de tablas
            query = f"""
            SELECT table_name
            FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            
            job = bq.query(query)
            tables = list(job.result())
            
            if tables:
                print(f"📊 {dataset.upper()}:")
                # Para cada tabla, obtener el conteo de filas
                for table_row in tables:
                    table_name = table_row.table_name
                    try:
                        count_query = f"SELECT COUNT(*) as row_count FROM `{PROJECT_ID}.{dataset}.{table_name}`"
                        count_job = bq.query(count_query)
                        count_result = list(count_job.result())[0]
                        row_count = count_result.row_count
                        print(f"   ✅ {table_name}: {row_count:,} filas")
                    except Exception as count_error:
                        print(f"   ⚠️ {table_name}: Error contando filas")
            else:
                print(f"❌ {dataset.upper()}: Sin tablas")
                
        except Exception as e:
            print(f"❌ Error verificando {dataset}: {str(e)[:50]}...")

def verify_results_detailed():
    """Verificación más detallada con información adicional"""
    print("\n🔍 Verificación detallada de resultados...")
    
    datasets = [
        ('bronze', ['employees', 'products', 'sales']),
        ('silver', ['employees', 'products', 'sales']),
        ('gold', ['sales_analytics', 'product_metrics', 'sales_summary'])
    ]
    
    for dataset, expected_tables in datasets:
        print(f"\n📊 {dataset.upper()} DATASET:")
        
        try:
            # Verificar cada tabla esperada
            for table in expected_tables:
                try:
                    count_query = f"SELECT COUNT(*) as row_count FROM `{PROJECT_ID}.{dataset}.{table}`"
                    count_job = bq.query(count_query)
                    count_result = list(count_job.result())[0]
                    row_count = count_result.row_count
                    
                    # También obtener una muestra de columnas
                    sample_query = f"""
                    SELECT column_name
                    FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                    LIMIT 5
                    """
                    sample_job = bq.query(sample_query)
                    columns = [col.column_name for col in sample_job.result()]
                    
                    print(f"   ✅ {table}: {row_count:,} filas | Columnas: {', '.join(columns[:3])}{'...' if len(columns) > 3 else ''}")
                    
                except Exception as table_error:
                    print(f"   ❌ {table}: Tabla no encontrada o error")
                    
        except Exception as e:
            print(f"   ❌ Error verificando dataset {dataset}: {str(e)[:50]}...")

# ================================
# PIPELINE PRINCIPAL
# ================================

def run_etl():
    """Ejecuta pipeline completo con manejo de errores"""
    print("🚀 Pipeline ETL Medallion - Iniciando...")
    print("=" * 50)
    
    # Ejecutar capas en orden
    layers = [
        ("Bronze", bronze_layer),
        ("Silver", silver_layer),
        ("Gold", gold_layer)
    ]
    
    overall_success = True
    
    for layer_name, layer_func in layers:
        success = layer_func()
        if not success:
            print(f"❌ {layer_name} Layer falló - Pipeline detenido")
            overall_success = False
            break
        print()  # Línea en blanco entre capas
    
    # Verificar resultados si todo salió bien
    if overall_success:
        verify_results()
        
        print("\n" + "=" * 50)
        print("🎉 Pipeline ETL completado exitosamente!")
        print(f"🔗 BigQuery: https://console.cloud.google.com/bigquery?project={PROJECT_ID}")
        print("\n📋 Tablas creadas:")
        print("   🥉 bronze: employees, products, sales")
        print("   🥈 silver: employees, products, sales (limpios)")
        print("   🥇 gold: sales_analytics, product_metrics, sales_summary")
        
        # Opción de verificación detallada
        print("\n💡 Para verificación detallada, ejecuta: verify_results_detailed()")
        
    else:
        print("\n❌ Pipeline falló. Revisa los errores anteriores.")
        print("💡 Consejos:")
        print("   - Verifica que los archivos CSV estén en GCS")
        print("   - Revisa permisos de BigQuery")
        print("   - Confirma que el proyecto ID sea correcto")

def show_sample_data():
    """Muestra datos de ejemplo de las tablas gold"""
    print("\n📊 Datos de ejemplo:")
    
    sample_queries = [
        ("Top 5 Productos por Revenue", f"""
        SELECT product_name, category, total_revenue, total_quantity_sold
        FROM `{PROJECT_ID}.gold.product_metrics`
        ORDER BY total_revenue DESC
        LIMIT 5
        """),
        
        ("Resumen por Canal", f"""
        SELECT channel, 
               SUM(total_revenue) as total_revenue,
               SUM(total_transactions) as total_transactions,
               ROUND(AVG(avg_transaction_value), 2) as avg_value
        FROM `{PROJECT_ID}.gold.sales_summary`
        GROUP BY channel
        ORDER BY total_revenue DESC
        """),
        
        ("Ventas por Departamento", f"""
        SELECT department, 
               COUNT(*) as total_sales,
               ROUND(SUM(total_amount), 2) as total_revenue
        FROM `{PROJECT_ID}.gold.sales_analytics`
        WHERE department IS NOT NULL
        GROUP BY department
        ORDER BY total_revenue DESC
        LIMIT 5
        """)
    ]
    
    for title, query in sample_queries:
        try:
            print(f"\n🔹 {title}:")
            job = bq.query(query)
            results = list(job.result())
            
            if results:
                for i, row in enumerate(results[:3]):  # Solo primeras 3 filas
                    row_data = [str(value) for value in row.values()]
                    print(f"   {i+1}. {' | '.join(row_data)}")
                if len(results) > 3:
                    print(f"   ... y {len(results)-3} más")
            else:
                print("   Sin datos")
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:50]}...")

if __name__ == "__main__":
    run_etl()