Español:
# Optimizador de Costos de Plataforma de Datos

## 🎯 Resumen del Proyecto
Plataforma de Datos de Extremo a Extremo con capacidades automatizadas de optimización de costos. Construida con prácticas de nivel senior para despliegue en producción.

## 🏗️ Arquitectura
```
Fuentes de Datos → Airflow (Ingesta) → S3 (Capa Cruda) → dbt (Transformación) → PostgreSQL (Servicio) → Grafana (Monitoreo)
```

## 🛠️ Stack Tecnológico
* Orquestación: Apache Airflow
* Transformación: dbt (Herramienta de Construcción de Datos)
* Almacenamiento: AWS S3 (Crudo), PostgreSQL (Procesado)
* Infraestructura: Terraform, Docker
* Monitoreo: Grafana, Prometheus
* CI/CD: GitHub Actions

## 📊 Características Clave
* [ ]Pipeline CDC con optimización automática de particiones
* [ ] Monitoreo de costos con sugerencias de optimización
* [ ]Pruebas automatizadas de calidad de datos
* [ ]Pipeline completo de CI/CD
* [ ]Dashboard de costos en tiempo real
 
## 🚀 Inicio Rápido
```
bash
# Clonar repositorio
git clone https://github.com/TU_USUARIO/data-platform-cost-optimizer.git
cd data-platform-cost-optimizer

# Configurar entorno
make setup
make deploy-local
```

## 📁 Estructura del Proyecto
```
├── airflow/           # DAGs y configuración de Airflow
├── dbt/              # Modelos y transformaciones de dbt
├── docker/           # Configuraciones de Docker
├── infrastructure/   # IaC de Terraform
├── monitoring/       # Dashboards y alertas de Grafana
├── scripts/         # Scripts de utilidad
└── docs/            # Documentación del proyecto
```

## 🎯 Métricas de Éxito
* Procesar más de 1M de registros usando AWS Free Tier
* Lograr más del 99% de tiempo de actividad del pipeline
* Implementar despliegue menor a 5 minutos
* Construir motor de recomendaciones de optimización de costos
* Crear dashboard de monitoreo listo para producción

## 👤 Autor
**Heberth Caripa** - Proyecto de Portafolio de Ingeniero de Datos

## 📄 Licencia
Licencia MIT

---

English:
# Data Platform Cost Optimizer

## 🎯 Project Overview
End-to-End Data Platform with automated cost optimization capabilities. Built with senior-level practices for production deployment.

## 🏗️ Architecture
```
Data Sources → Airflow (Ingestion) → S3 (Raw Layer) → dbt (Transformation) → PostgreSQL (Serving) → Grafana (Monitoring)
```

## 🛠️ Tech Stack
- **Orchestration**: Apache Airflow
- **Transformation**: dbt (Data Build Tool)
- **Storage**: AWS S3 (Raw), PostgreSQL (Processed)
- **Infrastructure**: Terraform, Docker
- **Monitoring**: Grafana, Prometheus
- **CI/CD**: GitHub Actions

## 📊 Key Features
- [ ] CDC Pipeline with automatic partition optimization
- [ ] Cost monitoring with optimization suggestions
- [ ] Automated data quality testing
- [ ] Complete CI/CD pipeline
- [ ] Real-time cost dashboard

## 🚀 Quick Start
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/data-platform-cost-optimizer.git
cd data-platform-cost-optimizer

# Setup environment
make setup
make deploy-local
```

## 📁 Project Structure
```
├── airflow/           # Airflow DAGs and configuration
├── dbt/              # dbt models and transformations
├── docker/           # Docker configurations
├── infrastructure/   # Terraform IaC
├── monitoring/       # Grafana dashboards and alerts
├── scripts/         # Utility scripts
└── docs/            # Project documentation
```

## 🎯 Project Goals
- [ ] Process 1M+ records using AWS Free Tier
- [ ] Achieve 99%+ pipeline uptime
- [ ] Implement sub-5 minute deployment
- [ ] Build cost optimization recommendations engine
- [ ] Create production-ready monitoring dashboard

## 👤 Author
**Heberth Caripa** - Data Engineer Portfolio Project

## 📄 License
MIT License

