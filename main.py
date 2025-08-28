#!/usr/bin/env python3
"""
main.py - Servidor HTTP para Cloud Run
Cloud Run necesita un servidor HTTP que responda a requests
"""

import os
from flask import Flask, request, jsonify
import logging
from datetime import datetime

# Importar nuestras funciones ETL (NO una clase)
from etl_medallion import run_etl, verify_results_detailed, show_sample_data

# Configurar Flask app
app = Flask(__name__)

# Configurar logging para Cloud Run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint requerido por Cloud Run"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'etl-pipeline',
        'endpoints': ['/trigger-etl', '/verify-results', '/sample-data']
    }), 200

@app.route('/trigger-etl', methods=['POST'])
def trigger_etl_endpoint():
    """
    Endpoint principal que ejecuta el ETL
    Puede ser activado por:
    - Cloud Scheduler  
    - Pub/Sub
    - HTTP directo
    """
    try:
        logger.info("🚀 Iniciando ETL Pipeline via HTTP request")
        
        # Obtener parámetros opcionales del request
        data = request.get_json() or {}
        logger.info(f"Parámetros recibidos: {data}")
        
        # Ejecutar pipeline ETL
        run_etl()
        
        response = {
            'status': 'success',
            'message': 'ETL pipeline completed successfully',
            'timestamp': datetime.utcnow().isoformat(),
            'layers_processed': ['bronze', 'silver', 'gold']
        }
        
        logger.info("✅ ETL Pipeline completado exitosamente")
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"❌ Error en ETL Pipeline: {str(e)}")
        
        error_response = {
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat(),
            'suggestion': 'Verifica logs de BigQuery y permisos de GCS'
        }
        
        return jsonify(error_response), 500

@app.route('/verify-results', methods=['GET'])
def verify_results_endpoint():
    """
    Endpoint para verificar los resultados del ETL
    """
    try:
        logger.info("🔍 Verificando resultados del ETL")
        
        # Capturar output de verify_results_detailed
        import io
        import sys
        from contextlib import redirect_stdout
        
        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            verify_results_detailed()
        
        verification_output = output_buffer.getvalue()
        
        response = {
            'status': 'success',
            'message': 'Verification completed',
            'timestamp': datetime.utcnow().isoformat(),
            'verification_details': verification_output
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/sample-data', methods=['GET'])
def sample_data_endpoint():
    """
    Endpoint para mostrar datos de ejemplo
    """
    try:
        logger.info("📊 Obteniendo datos de ejemplo")
        
        # Capturar output de show_sample_data
        import io
        import sys
        from contextlib import redirect_stdout
        
        output_buffer = io.StringIO()
        with redirect_stdout(output_buffer):
            show_sample_data()
        
        sample_output = output_buffer.getvalue()
        
        response = {
            'status': 'success',
            'message': 'Sample data retrieved',
            'timestamp': datetime.utcnow().isoformat(),
            'sample_data': sample_output
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo datos de ejemplo: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route('/status', methods=['GET'])
def status_endpoint():
    """
    Endpoint para verificar el estado del servicio
    """
    try:
        from etl_medallion import PROJECT_ID
        
        response = {
            'status': 'running',
            'service': 'etl-pipeline',
            'project_id': PROJECT_ID,
            'timestamp': datetime.utcnow().isoformat(),
            'available_endpoints': {
                'GET /': 'Health check',
                'POST /trigger-etl': 'Ejecutar ETL completo',
                'GET /verify-results': 'Verificar resultados',
                'GET /sample-data': 'Mostrar datos de ejemplo',
                'GET /status': 'Estado del servicio'
            }
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

if __name__ == '__main__':
    # Obtener puerto de Cloud Run (default 8080)
    port = int(os.environ.get('PORT', 8080))
    
    logger.info(f"🚀 Iniciando servidor ETL en puerto {port}")
    logger.info(f"📋 Endpoints disponibles:")
    logger.info(f"   GET  / - Health check")
    logger.info(f"   POST /trigger-etl - Ejecutar ETL")
    logger.info(f"   GET  /verify-results - Verificar datos")
    logger.info(f"   GET  /sample-data - Datos de ejemplo")
    logger.info(f"   GET  /status - Estado del servicio")
    
    # Ejecutar servidor Flask
    app.run(
        host='0.0.0.0',
        port=port,
        debug=False  # Nunca debug=True en producción
    )