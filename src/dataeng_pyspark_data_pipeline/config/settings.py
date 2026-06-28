# src/config/settings.py
import yaml


def carregar_config(path: str = "config/settings.yaml") -> dict:
    """Carrega um arquivo de configuração YAML."""

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)
