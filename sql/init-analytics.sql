-- Executado uma única vez na criação do volume do Postgres.
-- Cria a base analítica separada do metadata do Airflow.
CREATE USER analytics WITH PASSWORD 'analytics';
CREATE DATABASE analytics OWNER analytics;
