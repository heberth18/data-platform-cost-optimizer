Español:
    # Plataforma de Riesgo de Clientes

![Pipeline CI](https://github.com/heberth18/data-platform-cost-optimizer/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.10-blue)
![dbt](https://img.shields.io/badge/dbt-1.6.0-orange)
![Airflow](https://img.shields.io/badge/airflow-2.7.0-red)

> Plataforma de detección de fraude en tiempo real y analítica Customer 360 con validación automatizada de calidad de datos

---

## 📋 Resumen

Pipeline ELT end-to-end que procesa datos de e-commerce para generar **perfiles Customer 360**, detectar fraude mediante **análisis de riesgo multidimensional**, y entregar inteligencia de negocio accionable a través de dashboards automatizados.

**Capacidades Clave:**
- 🔍 **Detección de fraude multidimensional** con 6 factores de riesgo (velocidad, geográfico, comportamental, perfil, monto, temporal)
- 👥 **Perfiles Customer 360** con historial completo de comportamiento y transacciones
- 🛡️ **Enmascaramiento de PII** para privacidad de datos y cumplimiento normativo
- ✅ **71 tests automatizados de calidad de datos** garantizando confiabilidad del pipeline
- 📊 **3 dashboards de Metabase** para inteligencia de negocio en tiempo real
- 🔄 **Pipeline CI/CD** con validación automatizada de calidad de código

---

## 🏗️ Arquitectura
```
┌─────────────────┐
│  DummyJSON API  │  (External data source)
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              AIRFLOW ORCHESTRATION                   │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ Extract  │→ │Transform │→ │ Validate │         │
│  └──────────┘  └──────────┘  └──────────┘         │
│                                    ↓                │
│                              ┌──────────┐          │
│                              │   dbt    │          │
│                              │ Staging  │          │
│                              └────┬─────┘          │
│                                   ↓                │
│                            ┌──────────┐           │
│                            │dbt Tests │           │
│                            │ Staging  │           │
│                            └────┬─────┘           │
│                                 ↓                 │
│                        ┌──────────────────┐      │
│                        │ Fraud Detection  │      │
│                        │     Engine       │      │
│                        └────────┬─────────┘      │
│                                 ↓                │
│                          ┌──────────┐           │
│                          │   dbt    │           │
│                          │  Marts   │           │
│                          └────┬─────┘           │
│                               ↓                 │
│                        ┌──────────┐            │
│                        │dbt Tests │            │
│                        │  Marts   │            │
│                        └────┬─────┘            │
└─────────────────────────────┼─────────────────┘
                              │
                              ▼
                  ┌────────────────────┐
                  │    PostgreSQL      │
                  │   (3 schemas)      │
                  │ staging/analytics  │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │     Metabase       │
                  │   (3 Dashboards)   │
                  └────────────────────┘
```

**Stack Tecnológico:**
- **Orquestación:** Apache Airflow 2.7.0
- **Transformación:** dbt 1.6.0
- **Base de Datos:** PostgreSQL 14
- **Analítica:** Metabase
- **Containerización:** Docker Compose
- **CI/CD:** GitHub Actions
- **Lenguaje:** Python 3.10

---

## ✨ Características

### 1. **Motor de Detección de Fraude Multidimensional**
6 algoritmos independientes de scoring de riesgo:
- **Riesgo de Velocidad:** Patrones de frecuencia de transacciones
- **Riesgo Geográfico:** Detección de anomalías de ubicación
- **Riesgo Comportamental:** Análisis de patrones de compra
- **Riesgo de Perfil:** Validación de completitud de cuenta
- **Riesgo de Monto:** Anomalías en montos de transacción
- **Riesgo Temporal:** Patrones de tiempo inusuales

**Salida:** Score de riesgo compuesto (0.0-1.0) con recomendaciones accionables

### 2. **Perfiles Customer 360**
Vista unificada del cliente con:
- Historial transaccional completo
- Segmentación comportamental (Premium, Regular, Nuevo)
- Clasificación de valor (VIP, Valor Medio, Estándar)
- Scoring de actividad (0-100)
- Métricas de completitud de perfil

### 3. **Seguridad de Datos**
- Enmascaramiento de PII (emails, teléfonos, nombres)
- Conexiones de Airflow encriptadas (Fernet)
- Gestión de secretos basada en variables de entorno
- Sin credenciales en Git

### 4. **Aseguramiento de Calidad de Datos**
**71 tests automatizados en 3 capas:**
- **20 tests** en fuentes de datos raw
- **25 tests** en modelos staging
- **25 tests** en marts de analytics
- **1 test personalizado** (validación de revenue)

**Tipos de test:** unique, not_null, relationships, accepted_values, accepted_range

### 5. **Inteligencia de Negocio**
3 dashboards de Metabase:
- **Dashboard de Segmentación de Clientes:** Análisis RFM y tiers de valor
- **Dashboard de Analítica de Fraude:** Distribución de riesgo y clientes de alto riesgo
- **Dashboard de Clientes de Alto Valor:** Retención VIP y análisis de revenue

---

## 🚀 Inicio Rápido

### Prerequisitos
- Docker & Docker Compose
- Git
- 8GB RAM mínimo

### Instalación

1. **Clonar repositorio**
```bash
git clone https://github.com/heberth18/data-platform-cost-optimizer.git
cd data-platform-cost-optimizer
```

2. **Crear archivo `.env`**
```bash
cp .env.example .env
```

Editar `.env` con tu configuración:
```env
# PostgreSQL
POSTGRES_USER=dataeng
POSTGRES_PASSWORD=dataeng123
POSTGRES_DB=data_platform

# Airflow
AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY=tu_clave_fernet_aqui

# Enmascaramiento PII
ENABLE_PII_MASKING=true
```

3. **Generar clave Fernet** (para encriptación de Airflow)
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

4. **Iniciar servicios**
```bash
docker-compose up -d
```

5. **Acceder a interfaces**
- **Airflow:** http://localhost:8080 (usuario: `airflow`, contraseña: `airflow`)
- **Metabase:** http://localhost:3000
- **PostgreSQL:** localhost:5432

---

## 📊 Uso

### Ejecutar el Pipeline

1. **Disparar DAG en UI de Airflow:**
   - Ir a http://localhost:8080
   - Habilitar `test_pipeline_4_modules`
   - Click en "Trigger DAG"

2. **Monitorear ejecución:**
   - Ver logs de tareas en UI de Airflow
   - Verificar resultados de calidad de datos
   - Revisar alertas de fraude

### Consultar Datos de Analytics
```sql
-- Clientes de alto riesgo
SELECT customer_id, full_name, composite_risk_score, risk_level
FROM analytics.fraud_analytics
WHERE risk_level IN ('high', 'critical')
ORDER BY composite_risk_score DESC;

-- Segmentación de clientes
SELECT customer_segment, COUNT(*) as clientes, AVG(total_spent) as ltv_promedio
FROM analytics.customer_segmentation
GROUP BY customer_segment;

-- Clientes VIP en riesgo
SELECT customer_id, full_name, total_spent, retention_status
FROM analytics.high_value_customers
WHERE retention_status = 'At Risk Customer';
```

Más queries disponibles en `/queries/metabase_queries.sql`

---

## 🧪 Testing

### Ejecutar tests de dbt
```bash
# Todos los tests
docker exec -it airflow-webserver dbt test --project-dir /opt/airflow/dbt

# Solo staging
docker exec -it airflow-webserver dbt test --select staging --project-dir /opt/airflow/dbt

# Solo marts
docker exec -it airflow-webserver dbt test --select marts --project-dir /opt/airflow/dbt
```

### Pipeline CI/CD
Automatizado en cada push:
- ✅ Linting de Python (Black, Flake8)
- ✅ Linting de SQL (SQLFluff)
- ✅ Validación de compilación de dbt

---

## 📁 Estructura del Proyecto
```
.
├── .github/
│   └── workflows/                # CI/CD configuration
│       └── ci.yml
├── airflow/
│   ├── dags/                     # Airflow DAGs
│   │   └── test_pipeline_4_modules.py
│   └── include/
│       ├── customer_risk_platform/
│       │   ├── extractors.py     # API data extraction
│       │   ├── validators.py     # Data quality validation
│       │   ├── transformers.py   # Data transformation
│       │   ├── fraud_analyzers.py# Fraud detection engine
│       │   └── monitoring.py     # Performance tracking
│       └── security/
│           └── pii_masking.py    # PII protection
├── dbt/
│   ├── models/
│   │   ├── staging/              # Cleaned data models
│   │   │   ├── stg_customer_profiles.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_customer_metrics.sql
│   │   └── marts/                # Business logic models
│   │       ├── customer_segmentation.sql
│   │       ├── fraud_analytics.sql
│   │       └── high_value_customers.sql
│   └── tests/                    # Custom dbt tests
├── docker/
│   └── airflow/                  # Airflow Docker config
├── scripts/
│   ├── init_db.sql              # Database initialization
│   └── setup_connections.py     # Airflow connections
├── queries/                      # Example SQL queries
├── docker-compose.yml           # Service orchestration
├── makefile                     # Development commands
└── requirements.txt             # Python dependencies
```

---

## 🔒 Seguridad

- **Enmascaramiento de PII:** Hashing de emails (SHA-256) y enmascaramiento de campos
- **Gestión de Secretos:** Todas las credenciales en `.env` (excluido de Git)
- **Encriptación:** Conexiones de Airflow encriptadas con Fernet
- **Control de Acceso:** Permisos de usuario de PostgreSQL
- **.gitignore:** Exclusiones comprehensivas para datos sensibles

---

## 🛠️ Desarrollo

### Comandos del Makefile
```bash
make setup      # Setup inicial del proyecto
make build      # Construir imágenes Docker
make up         # Iniciar todos los servicios
make down       # Detener servicios
make logs       # Ver logs
make test       # Ejecutar tests de calidad de datos
```

### Agregar Nuevos Modelos
1. Crear archivo SQL en `dbt/models/`
2. Agregar tests en `schema.yml`
3. Ejecutar `dbt run` y `dbt test`

---

## 🗺️ Roadmap

### En Progreso
- [ ] Deployment en GCP (BigQuery + Cloud Composer)
- [ ] Dashboards de Looker Studio
- [ ] Integración con Great Expectations

---

## 📈 Performance

**Métricas del Pipeline:**
- **Tiempo de procesamiento:** ~1 minutos 8 segundos end to end
- **Registros procesados:** 208 clientes, 198 pedidos
- **Cobertura de pruebas:** 71 pruebas automatizadas de calidad de datos

---

## 🤝 Contribuciones

Este es un proyecto de portafolio. Sugerencias y feedback bienvenidos vía issues.

---

## 📄 Licencia

Licencia MIT - Ver archivo LICENSE para detalles

---

## 👤 Autor

**Heberth** - [GitHub](https://github.com/heberth18)

---

English:

# Customer Risk Platform

![CI Pipeline](https://github.com/heberth18/data-platform-cost-optimizer/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.10-blue)
![dbt](https://img.shields.io/badge/dbt-1.6.0-orange)
![Airflow](https://img.shields.io/badge/airflow-2.7.0-red)

> Real-time fraud detection and Customer 360 analytics platform with automated data quality checks

---

## 📋 Overview

End-to-end ELT pipeline that processes e-commerce data to generate **Customer 360 profiles**, detect fraud through **multi-dimensional risk analysis**, and deliver actionable business intelligence through automated dashboards.

**Key Capabilities:**
- 🔍 **Multi-dimensional fraud detection** with 6 risk factors (velocity, geographic, behavioral, profile, amount, temporal)
- 👥 **Customer 360 profiles** with complete behavioral and transactional history
- 🛡️ **PII masking** for data privacy and security compliance
- ✅ **71 automated data quality tests** ensuring pipeline reliability
- 📊 **3 Metabase dashboards** for real-time business intelligence
- 🔄 **CI/CD pipeline** with automated code quality checks

---

## 🏗️ Architecture
```
┌─────────────────┐
│  DummyJSON API  │  (External data source)
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              AIRFLOW ORCHESTRATION                   │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │ Extract  │→ │Transform │→ │ Validate │         │
│  └──────────┘  └──────────┘  └──────────┘         │
│                                    ↓                │
│                              ┌──────────┐          │
│                              │   dbt    │          │
│                              │ Staging  │          │
│                              └────┬─────┘          │
│                                   ↓                │
│                            ┌──────────┐           │
│                            │dbt Tests │           │
│                            │ Staging  │           │
│                            └────┬─────┘           │
│                                 ↓                 │
│                        ┌──────────────────┐      │
│                        │ Fraud Detection  │      │
│                        │     Engine       │      │
│                        └────────┬─────────┘      │
│                                 ↓                │
│                          ┌──────────┐           │
│                          │   dbt    │           │
│                          │  Marts   │           │
│                          └────┬─────┘           │
│                               ↓                 │
│                        ┌──────────┐            │
│                        │dbt Tests │            │
│                        │  Marts   │            │
│                        └────┬─────┘            │
└─────────────────────────────┼─────────────────┘
                              │
                              ▼
                  ┌────────────────────┐
                  │    PostgreSQL      │
                  │   (3 schemas)      │
                  │ staging/analytics  │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │     Metabase       │
                  │   (3 Dashboards)   │
                  └────────────────────┘
```

**Technology Stack:**
- **Orchestration:** Apache Airflow 2.7.0
- **Transformation:** dbt 1.6.0
- **Database:** PostgreSQL 14
- **Analytics:** Metabase
- **Containerization:** Docker Compose
- **CI/CD:** GitHub Actions
- **Language:** Python 3.10

---

## ✨ Features

### 1. **Multi-Dimensional Fraud Detection Engine**
6 independent risk scoring algorithms:
- **Velocity Risk:** Transaction frequency patterns
- **Geographic Risk:** Location anomaly detection  
- **Behavioral Risk:** Purchase pattern analysis
- **Profile Risk:** Account completeness validation
- **Amount Risk:** Transaction amount anomalies
- **Temporal Risk:** Unusual timing patterns

**Output:** Composite risk score (0.0-1.0) with actionable recommendations

### 2. **Customer 360 Profiles**
Unified customer view with:
- Complete transactional history
- Behavioral segmentation (Premium, Regular, New)
- Value tier classification (VIP, Medium Value, Standard)
- Activity scoring (0-100)
- Profile completeness metrics

### 3. **Data Security**
- PII masking (emails, phones, names)
- Encrypted Airflow connections (Fernet)
- Environment-based secrets management
- No credentials in Git

### 4. **Data Quality Assurance**
**71 automated tests across 3 layers:**
- **20 tests** on raw data sources
- **25 tests** on staging models
- **25 tests** on analytics marts
- **1 custom test** (revenue validation)

**Test types:** unique, not_null, relationships, accepted_values, accepted_range

### 5. **Business Intelligence**
3 Metabase dashboards:
- **Customer Segmentation Dashboard:** RFM analysis and value tiers
- **Fraud Analytics Dashboard:** Risk distribution and high-risk customers
- **High-Value Customer Dashboard:** VIP retention and revenue analysis

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Git
- 8GB RAM minimum

### Installation

1. **Clone repository**
```bash
git clone https://github.com/heberth18/data-platform-cost-optimizer.git
cd data-platform-cost-optimizer
```

2. **Create `.env` file**
```bash
cp .env.example .env
```

Edit `.env` with your configuration:
```env
# PostgreSQL
POSTGRES_USER=dataeng
POSTGRES_PASSWORD=dataeng123
POSTGRES_DB=data_platform

# Airflow
AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY=your_fernet_key_here

# PII Masking
ENABLE_PII_MASKING=true
```

3. **Generate Fernet key** (for Airflow encryption)
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

4. **Start services**
```bash
docker-compose up -d
```

5. **Access interfaces**
- **Airflow:** http://localhost:8080 (user: `airflow`, pass: `airflow`)
- **Metabase:** http://localhost:3000
- **PostgreSQL:** localhost:5432

---

## 📊 Usage

### Run the Pipeline

1. **Trigger DAG in Airflow UI:**
   - Go to http://localhost:8080
   - Enable `test_pipeline_4_modules`
   - Click "Trigger DAG"

2. **Monitor execution:**
   - View task logs in Airflow UI
   - Check data quality results
   - Review fraud alerts

### Query Analytics Data
```sql
-- High-risk customers
SELECT customer_id, full_name, composite_risk_score, risk_level
FROM analytics.fraud_analytics
WHERE risk_level IN ('high', 'critical')
ORDER BY composite_risk_score DESC;

-- Customer segmentation
SELECT customer_segment, COUNT(*) as customers, AVG(total_spent) as avg_ltv
FROM analytics.customer_segmentation
GROUP BY customer_segment;

-- VIP customers at risk
SELECT customer_id, full_name, total_spent, retention_status
FROM analytics.high_value_customers
WHERE retention_status = 'At Risk Customer';
```

More queries available in `/queries/metabase_queries.sql`

---

## 🧪 Testing

### Run dbt tests
```bash
# All tests
docker exec -it airflow-webserver dbt test --project-dir /opt/airflow/dbt

# Staging only
docker exec -it airflow-webserver dbt test --select staging --project-dir /opt/airflow/dbt

# Marts only
docker exec -it airflow-webserver dbt test --select marts --project-dir /opt/airflow/dbt
```

### CI/CD Pipeline
Automated on every push:
- ✅ Python linting (Black, Flake8)
- ✅ SQL linting (SQLFluff)  
- ✅ dbt compilation checks

---

## 📁 Project Structure
```
.
├── .github/
│   └── workflows/                # CI/CD configuration
│       └── ci.yml
├── airflow/
│   ├── dags/                     # Airflow DAGs
│   │   └── test_pipeline_4_modules.py
│   └── include/
│       ├── customer_risk_platform/
│       │   ├── extractors.py     # API data extraction
│       │   ├── validators.py     # Data quality validation
│       │   ├── transformers.py   # Data transformation
│       │   ├── fraud_analyzers.py# Fraud detection engine
│       │   └── monitoring.py     # Performance tracking
│       └── security/
│           └── pii_masking.py    # PII protection
├── dbt/
│   ├── models/
│   │   ├── staging/              # Cleaned data models
│   │   │   ├── stg_customer_profiles.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_customer_metrics.sql
│   │   └── marts/                # Business logic models
│   │       ├── customer_segmentation.sql
│   │       ├── fraud_analytics.sql
│   │       └── high_value_customers.sql
│   └── tests/                    # Custom dbt tests
├── docker/
│   └── airflow/                  # Airflow Docker config
├── scripts/
│   ├── init_db.sql              # Database initialization
│   └── setup_connections.py     # Airflow connections
├── queries/                      # Example SQL queries
├── docker-compose.yml           # Service orchestration
├── makefile                     # Development commands
└── requirements.txt             # Python dependencies
```

---

## 🔒 Security

- **PII Masking:** Email hashing (SHA-256) and field masking
- **Secret Management:** All credentials in `.env` (excluded from Git)
- **Encryption:** Airflow connections encrypted with Fernet
- **Access Control:** PostgreSQL user permissions
- **.gitignore:** Comprehensive exclusions for sensitive data

---

## 🛠️ Development

### Makefile Commands
```bash
make setup      # Initial project setup
make build      # Build Docker images
make up         # Start all services
make down       # Stop services
make logs       # View logs
make test       # Run data quality tests
```

### Adding New Models
1. Create SQL file in `dbt/models/`
2. Add tests in `schema.yml`
3. Run `dbt run` and `dbt test`

---

## 🗺️ Roadmap

### In Progress
- [ ] GCP deployment (BigQuery + Cloud Composer)
- [ ] Looker Studio dashboards
- [ ] Great Expectations integration

---

## 📈 Performance

**Pipeline Metrics:**
- **Processing time:** 1 minutes 8 seconds end-to-end
- **Records processed:** 208 customers, 198 orders
- **Test coverage:** 71 automated data quality tests

---

## 🤝 Contributing

This is a portfolio project. Suggestions and feedback welcome via issues.

---

## 📄 License

MIT License - See LICENSE file for details

---

## 👤 Author

**Heberth** - [GitHub](https://github.com/heberth18)


