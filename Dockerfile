FROM python:3.9-slim

# Instala las dependencias del sistema necesarias
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    postgresql-client \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Establece el directorio de trabajo en el contenedor
WORKDIR /usr/src/app

# Copia el archivo requirements.txt al contenedor
COPY requirements.txt .

# Instala las dependencias listadas en requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia el script Python al directorio de trabajo en el contenedor
COPY dags/main_dags.py .
COPY dags/config.py .
COPY dags/main.py .
COPY dags/redshift.py .
COPY dags/spotify.py .
COPY dags/utils.py .

# Especifica el comando que se ejecutará cuando inicie el contenedor
CMD ["python", "dags/main_dags.py"]


