# 🌤️ Pipeline ETL - Dados Climáticos de São Paulo

Pipeline ETL automatizado para coleta, transformação e armazenamento de dados meteorológicos em tempo real da cidade de São Paulo.

## 📋 Índice

- Sobre o Projeto
- Arquitetura do Pipeline
- Stack Tecnológica
- Estrutura do Projeto
- Pré-requisitos
- Instalação e Configuração
- Como Executar
- Detalhamento das Etapas
- Análise de Dados
- Troubleshooting
- Referência

---

# 🎯 Sobre o Projeto

Este projeto demonstra a construção de um **pipeline ETL completo** utilizando boas práticas de Engenharia de Dados.

O pipeline coleta dados meteorológicos da API **OpenWeatherMap**, transforma os dados para um formato estruturado e os armazena em um banco de dados **PostgreSQL** para análises futuras.

A automação do pipeline é feita utilizando **Apache Airflow**, executando a coleta de dados periodicamente.

---

# 🏗️ Arquitetura do Pipeline

![Arquitetura do Pipeline](images/pipeline_architecture.png)

Fluxo do pipeline:

1. **Extract** – coleta dos dados da API
2. **Transform** – tratamento e padronização dos dados
3. **Load** – armazenamento em banco de dados PostgreSQL

Arquitetura geral:

API → Extract → Transform → Load → PostgreSQL → Análise de dados

---

# 🛠️ Stack Tecnológica

## Core

- Python 3.14+
- Apache Airflow 3.1.7
- PostgreSQL 14
- Docker
- Docker Compose

## Bibliotecas Python

- pandas
- requests
- SQLAlchemy
- psycopg2
- python-dotenv

## Outras Ferramentas

- Redis
- Jupyter Notebook
- UV (gerenciador de pacotes Python)

---

# 📂 Estrutura do Projeto

