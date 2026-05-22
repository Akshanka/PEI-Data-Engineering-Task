import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


def load_dq_rules(rules_file_path: str):
    """
    Reads DQX checks from a YAML file.
    """
    with open(rules_file_path, "r") as file:
        return yaml.safe_load(file)


def apply_dq_checks(df, rules_file_path: str):
    """
    Applies DQX checks and splits data into:
    1. valid_df
    2. invalid_df / quarantine_df
    """
    checks = load_dq_rules(rules_file_path)

    dq_engine = DQEngine(WorkspaceClient())

    valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(
        df=df,
        checks=checks
    )

    return valid_df, quarantine_df