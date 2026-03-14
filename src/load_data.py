from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Carrega config/.env dentro do container (volume montado) e no ambiente local.
BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / "config" / ".env"
load_dotenv(env_path, encoding="utf-8")

USER = os.getenv("user")
PASSWORD = os.getenv("password")
DATABASE = os.getenv("database")
HOST = os.getenv("host")
PORT = os.getenv("port")
CLIENT_ENCODING = os.getenv("client_encoding")  # opcional: evita mascarar erros reais por default
DATABASE_URL = os.getenv("database_url") or os.getenv("DATABASE_URL")
AIRFLOW_SQLALCHEMY_CONN = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")


def _in_docker() -> bool:
    return os.path.exists("/.dockerenv") or os.getenv("AIRFLOW_HOME") == "/opt/airflow"


def _redact_url(url: str) -> str:
    try:
        parsed = make_url(url)
    except Exception:
        return "<invalid_url>"

    if parsed.password is None:
        return str(parsed)
    return str(parsed.set(password="***"))


def _engine_from_url(url: str):
    connect_args: dict[str, str] = {}
    # Se voce forcar client_encoding e o servidor devolver mensagens em outra codificacao,
    # pode acabar quebrando o decode e escondendo o erro original.
    if CLIENT_ENCODING:
        connect_args["client_encoding"] = CLIENT_ENCODING

    logging.info(f"Usando conexao: {_redact_url(url)}")
    return create_engine(url, connect_args=connect_args)


def _engine_from_components():
    if not USER or not PASSWORD or not DATABASE:
        raise ValueError("Variaveis ausentes em config/.env: 'user', 'password', 'database'.")

    resolved_host = HOST or ("postgres" if _in_docker() else "host.docker.internal")
    resolved_port = int(PORT or "5432")
    logging.info(f"Conectando em {resolved_host}:{resolved_port}/{DATABASE}")

    return create_engine(
        f"postgresql+psycopg2://{USER}:{quote_plus(PASSWORD)}@{resolved_host}:{resolved_port}/{DATABASE}"
    )


def get_engine():
    if DATABASE_URL:
        return _engine_from_url(DATABASE_URL)
    return _engine_from_components()


def _get_engine_with_fallback():
    """
    Fallback para o Postgres interno do Airflow (service `postgres` no docker-compose).

    Isso contorna o caso comum em que o Postgres do host retorna mensagens em uma
    codificacao diferente de UTF-8 e o psycopg2 estoura `UnicodeDecodeError` antes
    de expor o erro real (DB inexistente, senha errada, etc.).
    """
    try:
        engine = get_engine()
        with engine.connect():
            pass
        return engine
    except Exception as exc:
        if _in_docker() and AIRFLOW_SQLALCHEMY_CONN:
            logging.warning(
                "Falha ao conectar no banco configurado via config/.env; tentando fallback para AIRFLOW__DATABASE__SQL_ALCHEMY_CONN. "
                f"Erro original: {type(exc).__name__}: {exc}"
            )
            engine = _engine_from_url(AIRFLOW_SQLALCHEMY_CONN)
            with engine.connect():
                pass
            return engine
        raise


def load_weather_data(table_name: str, df: pd.DataFrame):
    engine = _get_engine_with_fallback()

    # Corrigir encoding das colunas texto
    for col in df.select_dtypes(include=["object"]):
        df[col] = df[col].astype(str).str.encode("utf-8", "ignore").str.decode("utf-8")

    df.to_sql(name=table_name, con=engine, if_exists="append", index=False)

    logging.info("âœ… Dados carregados com sucesso!")

    df_check = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    logging.info(f"Total de registros na tabela: {len(df_check)}")
