import dagster as dg
from pathlib import Path
from dagster_duckdb import DuckDBResource


class IngestionFileConfig(dg.Config):
    path: str = "2018-01-22.csv"


@dg.asset()
def import_file(context: dg.AssetExecutionContext, config: IngestionFileConfig) -> str:
    file_path = Path(__file__).absolute().parent / f"../../../data/source/{config.path}"
    return str(file_path.resolve())


@dg.asset(
    kinds={"duckdb"},
)
def duckdb_table(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    import_file,
):
    table_name = "raw_data"
    with database.get_connection() as conn:
        table_query = f"""
            create table if not exists {table_name} (
                date date,
                share_price float,
                amount float,
                spend float,
                shift float,
                spread float
            ) 
        """
        conn.execute(table_query)
        conn.execute(f"copy {table_name} from '{import_file}' (format csv, header);")
