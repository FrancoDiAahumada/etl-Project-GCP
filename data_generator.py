from faker import Faker
import random
import string
import csv
import json
from datetime import datetime, timedelta
from google.cloud import storage
import os
from google.oauth2 import service_account



# =============================================================================
# 🎯 CONFIGURACIÓN DEL GENERADOR
# =============================================================================

# Configuración principal
CONFIG = {
    'num_employees': 2500,
    'num_sales': 5000,
    'num_products': 150,
    'bucket_name': 'raw-data-etl-lab',  # 🔹 CAMBIAR POR TU BUCKET
    'project_id': 'data-engineer-gcp-469614'         # 🔹 CAMBIAR POR TU PROJECT ID
}

# Instancias de Faker para diferentes regiones
fake_us = Faker('en_US')
fake_es = Faker('es_ES')
fake_mx = Faker('es_MX')

# =============================================================================
# 📊 DATOS MAESTROS REALISTAS
# =============================================================================

DEPARTMENTS = [
    'Engineering', 'Data Science', 'Product Management', 'Marketing', 
    'Sales', 'Customer Success', 'Human Resources', 'Finance', 
    'Operations', 'Legal', 'Security', 'DevOps'
]

JOB_LEVELS = ['Junior', 'Mid-Level', 'Senior', 'Lead', 'Manager', 'Director', 'VP']

OFFICE_LOCATIONS = [
    'San Francisco, CA', 'New York, NY', 'Austin, TX', 'Seattle, WA',
    'Madrid, Spain', 'Mexico City, Mexico', 'Remote', 'London, UK'
]

PRODUCT_CATEGORIES = [
    'Electronics', 'Software', 'Books', 'Home & Garden', 'Sports',
    'Fashion', 'Automotive', 'Health', 'Beauty', 'Food & Beverage'
]

SALES_CHANNELS = ['Online', 'Retail Store', 'Partner', 'Direct Sales', 'Mobile App']

CUSTOMER_SEGMENTS = ['Enterprise', 'SMB', 'Individual', 'Government', 'Education']

# =============================================================================
# 🏢 GENERADOR DE EMPLEADOS
# =============================================================================

def generate_employees(num_employees):
    """Genera datos realistas de empleados con relaciones jerárquicas"""
    employees = []
    
    for emp_id in range(1, num_employees + 1):
        # Seleccionar faker por región
        faker = random.choice([fake_us, fake_es, fake_mx])
        
        # Generar datos base
        first_name = faker.first_name()
        last_name = faker.last_name()
        department = random.choice(DEPARTMENTS)
        job_level = random.choice(JOB_LEVELS)
        location = random.choice(OFFICE_LOCATIONS)
        
        # Calcular salario basado en nivel y departamento
        base_salary = {
            'Junior': random.uniform(50000, 70000),
            'Mid-Level': random.uniform(70000, 95000),
            'Senior': random.uniform(95000, 130000),
            'Lead': random.uniform(120000, 160000),
            'Manager': random.uniform(140000, 180000),
            'Director': random.uniform(170000, 220000),
            'VP': random.uniform(200000, 350000)
        }
        
        # Ajuste por departamento (Tech paga más)
        dept_multiplier = 1.3 if department in ['Engineering', 'Data Science', 'DevOps'] else 1.0
        salary = round(base_salary[job_level] * dept_multiplier, 2)
        
        # Datos de fechas realistas
        hire_date = faker.date_between(start_date='-8y', end_date='-1m')
        last_promotion = faker.date_between(start_date=hire_date, end_date='today') if random.random() > 0.3 else None
        
        employee = {
            'employee_id': f'EMP-{emp_id:05d}',
            'first_name': first_name,
            'last_name': last_name,
            'full_name': f'{first_name} {last_name}',
            'email': f'{first_name.lower()}.{last_name.lower()}@company.com',
            'department': department,
            'job_title': f'{job_level} {faker.job()}',
            'job_level': job_level,
            'location': location,
            'hire_date': hire_date.isoformat(),
            'last_promotion_date': last_promotion.isoformat() if last_promotion else None,
            'salary': salary,
            'phone': faker.phone_number(),
            'manager_id': f'EMP-{random.randint(1, max(1, emp_id-1)):05d}' if emp_id > 1 and random.random() > 0.1 else None,
            'performance_rating': round(random.uniform(2.5, 5.0), 1),
            'is_active': random.choice([True] * 9 + [False]),  # 90% activos
            'work_type': random.choice(['Full-time', 'Part-time', 'Contract']),
            'created_at': datetime.now().isoformat()
        }
        employees.append(employee)
    
    return employees

# =============================================================================
# 📦 GENERADOR DE PRODUCTOS
# =============================================================================

def generate_products(num_products):
    """Genera catálogo de productos con precios y categorías"""
    products = []
    
    for prod_id in range(1, num_products + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        
        # Precios realistas por categoría
        price_ranges = {
            'Electronics': (50, 2000),
            'Software': (10, 500),
            'Books': (5, 50),
            'Home & Garden': (15, 300),
            'Sports': (20, 400),
            'Fashion': (25, 200),
            'Automotive': (100, 5000),
            'Health': (10, 150),
            'Beauty': (8, 100),
            'Food & Beverage': (3, 80)
        }
        
        min_price, max_price = price_ranges.get(category, (10, 100))
        price = round(random.uniform(min_price, max_price), 2)
        
        product = {
            'product_id': f'PRD-{prod_id:05d}',
            'product_name': fake_us.catch_phrase(),
            'category': category,
            'subcategory': fake_us.word().title(),
            'price': price,
            'cost': round(price * random.uniform(0.3, 0.7), 2),  # Margen realista
            'supplier': fake_us.company(),
            'launch_date': fake_us.date_between(start_date='-3y', end_date='today').isoformat(),
            'is_active': random.choice([True] * 8 + [False]),  # 80% activos
            'stock_quantity': random.randint(0, 1000),
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'rating': round(random.uniform(2.0, 5.0), 1),
            'reviews_count': random.randint(0, 500),
            'created_at': datetime.now().isoformat()
        }
        products.append(product)
    
    return products

# =============================================================================
# 💰 GENERADOR DE VENTAS
# =============================================================================

def generate_sales(num_sales, employees, products):
    """Genera transacciones de ventas con patrones estacionales"""
    sales = []
    active_products = [p for p in products if p['is_active']]
    sales_employees = [e for e in employees if e['department'] in ['Sales', 'Customer Success']]
    
    for sale_id in range(1, num_sales + 1):
        # Fecha con patrones estacionales (más ventas en Nov-Dic)
        if random.random() > 0.7:  # 30% en temporada alta
            sale_date = fake_us.date_between(start_date='-2m', end_date='today')
        else:
            sale_date = fake_us.date_between(start_date='-1y', end_date='-2m')
        
        # Seleccionar producto y calcular cantidades realistas
        product = random.choice(active_products)
        quantity = random.choices(
            [1, 2, 3, 4, 5, 10, 25, 50],
            weights=[30, 25, 15, 10, 8, 7, 3, 2]
        )[0]
        
        unit_price = product['price']
        # Descuentos ocasionales
        discount_pct = random.choices([0, 5, 10, 15, 20], weights=[60, 20, 10, 7, 3])[0]
        final_price = unit_price * (1 - discount_pct/100)
        total_amount = round(final_price * quantity, 2)
        
        # Cliente (empresa o individual)
        customer_segment = random.choice(CUSTOMER_SEGMENTS)
        if customer_segment == 'Individual':
            customer_name = fake_us.name()
            customer_email = fake_us.email()
        else:
            customer_name = fake_us.company()
            customer_email = f'orders@{fake_us.domain_name()}'
        
        sale = {
            'transaction_id': f'TXN-{sale_id:08d}',
            'product_id': product['product_id'],
            'product_name': product['product_name'],
            'category': product['category'],
            'customer_name': customer_name,
            'customer_email': customer_email,
            'customer_segment': customer_segment,
            'sales_rep_id': random.choice(sales_employees)['employee_id'] if sales_employees else None,
            'sale_date': sale_date.isoformat(),
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_pct': discount_pct,
            'final_unit_price': round(final_price, 2),
            'total_amount': total_amount,
            'channel': random.choice(SALES_CHANNELS),
            'region': random.choice(['North America', 'Europe', 'Latin America', 'Asia-Pacific']),
            'payment_method': random.choice(['Credit Card', 'Bank Transfer', 'PayPal', 'Cash']),
            'shipping_cost': round(random.uniform(0, 25), 2) if random.random() > 0.3 else 0,
            'status': random.choices(['Completed', 'Pending', 'Cancelled'], weights=[85, 10, 5])[0],
            'created_at': datetime.now().isoformat()
        }
        sales.append(sale)
    
    return sales

# =============================================================================
# ☁️ FUNCIONES DE GCP
# =============================================================================

def setup_gcp_credentials():
    """
    Configuración minimalista de credenciales GCP
    - Local: usa archivo JSON
    - Cloud Run: usa credenciales automáticas
    """
    try:
        # 🔍 Detectar entorno
        if os.getenv('K_SERVICE'):  # Cloud Run
            print("🚀 Ejecutando en Cloud Run - usando credenciales automáticas")
            return storage.Client()
        
        # 💻 Desarrollo local
        credential_paths = [
            'service-account-key.json',
            './service-account-key.json', 
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        ]
        
        for path in credential_paths:
            if path and os.path.exists(path):
                print(f"✅ Credenciales encontradas: {path}")
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
                return storage.Client()
        
        # 📝 Si no encuentra archivos, intentar con variable de entorno JSON
        cred_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        if cred_json:
            credentials_info = json.loads(cred_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            print("✅ Credenciales cargadas desde variable de entorno")
            return storage.Client(credentials=credentials)
            
        raise Exception("No se encontraron credenciales")
        
    except Exception as e:
        print(f"❌ Error configurando credenciales: {e}")
        print("💡 Verifica que existe 'service-account-key.json' en tu proyecto")
        raise

# Ejemplo de uso
if __name__ == "__main__":
    client = setup_gcp_credentials()
    print("🎉 Cliente GCS configurado correctamente")
def upload_to_gcs(bucket_name, source_file, destination_blob):
    """Sube un archivo a GCS con manejo de errores"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)
        print(f"☁️ ✅ {source_file} → gs://{bucket_name}/{destination_blob}")
        return True
    except Exception as e:
        print(f"❌ Error subiendo {source_file}: {str(e)}")
        return False

def save_to_csv(data, filename, fieldnames=None):
    """Guarda datos en CSV con encoding UTF-8"""
    if not data:
        print(f"⚠️ No hay datos para guardar en {filename}")
        return False
    
    if not fieldnames:
        fieldnames = data[0].keys()
    
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"📄 ✅ {filename} creado con {len(data)} registros")
        return True
    except Exception as e:
        print(f"❌ Error creando {filename}: {str(e)}")
        return False

# =============================================================================
# 🚀 FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    print("🏭 Iniciando generación de datos empresariales...")
    print("=" * 60)
    
    # Configurar credenciales
    setup_gcp_credentials()
    
    # Generar datasets
    print("\n👥 Generando empleados...")
    employees = generate_employees(CONFIG['num_employees'])
    
    print("📦 Generando productos...")
    products = generate_products(CONFIG['num_products'])
    
    print("💰 Generando ventas...")
    sales = generate_sales(CONFIG['num_sales'], employees, products)
    
    # Crear archivos CSV locales
    print("\n💾 Creando archivos CSV...")
    files_created = []
    
    if save_to_csv(employees, 'employees.csv'):
        files_created.append(('employees.csv', 'landing/employees.csv'))
    
    if save_to_csv(products, 'products.csv'):
        files_created.append(('products.csv', 'landing/products.csv'))
    
    if save_to_csv(sales, 'sales.csv'):
        files_created.append(('sales.csv', 'landing/sales.csv'))
    
    # Subir a GCS
    print(f"\n☁️ Subiendo archivos a gs://{CONFIG['bucket_name']}...")
    upload_success = 0
    
    for local_file, gcs_path in files_created:
        if upload_to_gcs(CONFIG['bucket_name'], local_file, gcs_path):
            upload_success += 1
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN DE GENERACIÓN DE DATOS")
    print("=" * 60)
    print(f"👥 Empleados:    {len(employees):,}")
    print(f"📦 Productos:    {len(products):,}")
    print(f"💰 Ventas:       {len(sales):,}")
    print(f"📁 Archivos CSV: {len(files_created)}")
    print(f"☁️ Subidos a GCS: {upload_success}/{len(files_created)}")
    
    if upload_success == len(files_created):
        print("🎉 ¡Todos los archivos subidos exitosamente!")
        print(f"🔗 Revisa tu bucket: https://console.cloud.google.com/storage/browser/{CONFIG['bucket_name']}")
    else:
        print("⚠️ Algunos archivos no se pudieron subir. Revisa las credenciales.")

# =============================================================================
# 🎯 EJECUCIÓN
# =============================================================================

if __name__ == "__main__":
    main()