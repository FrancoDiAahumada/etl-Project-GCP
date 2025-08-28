# Usa una imagen base oficial de Python
FROM python:3.11-slim

# Establece variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PORT=8080

# Crea un usuario no-root para seguridad
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos de requirements primero (para aprovechar el cache de Docker)
COPY requirements.txt .

# Instala las dependencias
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia el código de la aplicación y vamos
COPY . .

# Cambia la propiedad de los archivos al usuario no-root
RUN chown -R appuser:appuser /app

# Cambia al usuario no-root
USER appuser

# Expone el puerto que usa la aplicación
EXPOSE $PORT

# Comando para ejecutar la aplicación
CMD exec gunicorn --bind :$PORT --workers 2 --threads 8 --timeout 0 main:app