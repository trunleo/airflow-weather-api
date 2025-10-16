# Weather Airflow Project - Recommended Folder Structure

```
airflow/
├── dags/                           # Main DAGs directory
│   ├── weather_pipeline.py        # Main production DAG
│   ├── weather_test_pipeline.py   # Test/development DAG
│   └── weather_maintenance.py     # Maintenance/cleanup DAG
│
├── plugins/                        # Custom Airflow plugins
│   └── weather_plugin.py          # Custom operators/sensors
│
├── include/                        # Shared code and utilities
│   ├── weather/                    # Weather domain package
│   │   ├── __init__.py
│   │   ├── core/                   # Core business logic
│   │   │   ├── __init__.py
│   │   │   ├── database/           # Database operations
│   │   │   │   ├── __init__.py
│   │   │   │   ├── manager.py      # Database manager class
│   │   │   │   ├── models.py       # Data models/schemas
│   │   │   │   └── queries.py      # SQL queries
│   │   │   ├── etl/                # ETL operations
│   │   │   │   ├── __init__.py
│   │   │   │   ├── extractors.py   # Data extraction logic
│   │   │   │   ├── transformers.py # Data transformation logic
│   │   │   │   └── loaders.py      # Data loading logic
│   │   │   ├── alerts/             # Alert system
│   │   │   │   ├── __init__.py
│   │   │   │   ├── detector.py     # Alert detection logic
│   │   │   │   ├── thresholds.py   # Alert thresholds config
│   │   │   │   └── notifications.py # Alert notifications
│   │   │   └── utils/              # Utility functions
│   │   │       ├── __init__.py
│   │   │       ├── date_utils.py   # Date/time utilities
│   │   │       ├── validation.py   # Data validation
│   │   │       └── logging.py      # Custom logging
│   │   ├── operators/              # Custom Airflow operators
│   │   │   ├── __init__.py
│   │   │   ├── weather_operator.py # Weather-specific operators
│   │   │   └── db_operator.py      # Database operators
│   │   ├── sensors/                # Custom Airflow sensors
│   │   │   ├── __init__.py
│   │   │   └── weather_sensor.py   # Weather data sensors
│   │   ├── hooks/                  # Custom Airflow hooks
│   │   │   ├── __init__.py
│   │   │   └── weather_hook.py     # Weather API hooks
│   │   └── tasks/                  # Reusable task functions
│   │       ├── __init__.py
│   │       ├── connection_tasks.py # Connection management
│   │       ├── etl_tasks.py        # ETL task functions
│   │       └── notification_tasks.py # Notification tasks
│   ├── config/                     # Configuration management
│   │   ├── __init__.py
│   │   ├── settings.py             # Application settings
│   │   ├── connections.py          # Connection configurations
│   │   └── variables.py            # Airflow variables
│   └── shared/                     # Shared utilities across domains
│       ├── __init__.py
│       ├── exceptions.py           # Custom exceptions
│       ├── constants.py            # Application constants
│       └── helpers.py              # General helper functions
│
├── sql/                            # SQL scripts and migrations
│   ├── migrations/                 # Database migrations
│   ├── views/                      # Database views
│   └── procedures/                 # Stored procedures
│
├── tests/                          # Test suite
│   ├── unit/                       # Unit tests
│   │   ├── test_database/
│   │   ├── test_etl/
│   │   └── test_alerts/
│   ├── integration/                # Integration tests
│   │   └── test_pipelines/
│   ├── fixtures/                   # Test data fixtures
│   └── conftest.py                 # Pytest configuration
│
├── config/                         # Environment configurations
│   ├── local/                      # Local development config
│   ├── staging/                    # Staging environment config
│   └── production/                 # Production environment config
│
├── scripts/                        # Utility scripts
│   ├── setup/                      # Setup scripts
│   │   ├── create_connections.py   # Create Airflow connections
│   │   ├── create_variables.py     # Create Airflow variables
│   │   └── setup_database.py       # Database setup
│   ├── deployment/                 # Deployment scripts
│   └── maintenance/                # Maintenance scripts
│
├── docs/                           # Documentation
│   ├── api/                        # API documentation
│   ├── architecture/               # Architecture diagrams
│   └── user_guide/                 # User guides
│
├── k8s/                           # Kubernetes configurations
│   ├── airflow/                    # Airflow K8s configs
│   ├── postgres/                   # PostgreSQL K8s configs
│   └── monitoring/                 # Monitoring configs
│
├── docker/                         # Docker configurations
│   ├── airflow/                    # Airflow Docker setup
│   └── postgres/                   # PostgreSQL Docker setup
│
├── requirements/                   # Python dependencies
│   ├── base.txt                    # Base requirements
│   ├── dev.txt                     # Development requirements
│   └── prod.txt                    # Production requirements
│
├── .github/                        # GitHub workflows
│   └── workflows/                  # CI/CD workflows
│
├── pyproject.toml                  # Python project configuration
├── README.md                       # Project documentation
├── .gitignore                      # Git ignore rules
├── .env.example                    # Environment variables example
└── Makefile                        # Common commands
```

## 📁 **Key Organization Principles**

### 1. **Separation of Concerns**
- **DAGs**: Only contain workflow definitions
- **Core Logic**: Business logic separated from Airflow-specific code
- **Configuration**: Centralized configuration management
- **Tests**: Comprehensive test coverage

### 2. **Domain-Driven Design**
- **Weather Domain**: All weather-related functionality grouped together
- **Database Layer**: Separate abstraction for data operations
- **ETL Layer**: Dedicated ETL operations with clear responsibilities

### 3. **Airflow Best Practices**
- **Include Directory**: Non-DAG code in `include/` folder
- **Custom Components**: Operators, sensors, and hooks in dedicated folders
- **Task Functions**: Reusable task functions for DAG composition

## 🔧 **Implementation Benefits**

### **Maintainability**
- Clear module boundaries
- Easy to locate and modify code
- Reduced coupling between components

### **Testability**
- Isolated business logic
- Easy to mock dependencies
- Comprehensive test structure

### **Scalability**
- Easy to add new weather data sources
- Simple to extend alert system
- Clear path for new features

### **Deployment**
- Environment-specific configurations
- Container-ready structure
- CI/CD friendly organization