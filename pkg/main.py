import sys
from pathlib import Path
import logging

# sql
from sqlalchemy import text

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.utils import Connector, get_dev_logger
from pkg.errors import S3Error, FileSystemError, VerticaError

# gets airflow default logger and use it
# logger = logging.getLogger("airflow.task")
logger = get_dev_logger(logger_name=str(Path(Path(__file__).name)))


class DWHCreator:
    def __init__(self) -> None:
        self.dwh_conn = Connector(type="vertica-dwh").connect()
        self.path_to_sql = Path(Path.cwd(), "sql")

    logger.info("Starting initializing process.")

    def create_stg_layer(self) -> None:
        logger.info("Initializing STG layer.")
        SQL = "stg-ddl"

        try:
            logger.info(f"Reading `{SQL}.sql`.")
            query = Path(self.path_to_sql, f"{SQL}.sql").read_text(encoding="UTF-8")
            logger.info(f"`{SQL}.sql` loaded.")
        except Exception:
            logger.exception(
                f"Unable to read `{SQL}.sql`! Initializing process failed."
            )
            raise FileSystemError

        try:
            logger.info(f"Executing DDL query for STG layer.")
            with self.dwh_conn.begin() as conn:
                conn.execute(statement=text(query))
            logger.info(f"STG layer created.")
        except Exception:
            logger.exception(f"Unable to execute DDL query!")
            raise VerticaError

    def create_dds_layer(self) -> None:
        logger.info("Initializing DDS layer.")
        SQL = "dds-ddl"

        try:
            logger.info(f"Reading `{SQL}.sql`.")
            query = Path(self.path_to_sql, f"{SQL}.sql").read_text(encoding="UTF-8")
            logger.info(f"`{SQL}.sql` loaded.")
        except Exception:
            logger.exception(
                f"Unable to read `{SQL}.sql`! Initializing process failed."
            )
            raise FileSystemError

        try:
            logger.info(f"Executing DDL query for DDS layer.")
            with self.dwh_conn.begin() as conn:
                conn.execute(statement=text(query))
            logger.info(f"DDS layer created.")
        except Exception:
            logger.exception(f"Unable to execute DDL query!")
            raise VerticaError


class StagingDataLoader:
    """
    # Staging data loader

    Gets data from s3 warehouse and then loads to staging layer on Vertica.
    """

    def __init__(self) -> None:
        self.dwh_conn = Connector(type="vertica-dwh").connect()
        self.s3_conn = Connector(type="s3").connect()
        self.path_to_data = Path(Path.cwd(), "data")
        self.path_to_sql = Path(Path.cwd(), "sql")

    def _get_data_from_s3(self, file_name: str) -> None:
        """
        Gets data from s3 and save to
        """
        S3_BUCKET = "sprint6"

        logger.info(f"Getting `{file_name}` from s3.")

        if Path.exists(self.path_to_data) == False:
            Path.mkdir(self.path_to_data)

        try:
            self.s3_conn.download_file(
                Filename=Path(self.path_to_data, f"{file_name}.csv"),
                Bucket=S3_BUCKET,
                Key=f"{file_name}.csv",
            )
            logger.info(
                f"Successfully downloaded `{file_name}` file to `{self.path_to_data}` folder."
            )
        except Exception:
            logger.exception(f"Unable to download `{file_name}` from s3!")
            raise S3Error

    def _remove_data_folder(self) -> None:
        pass

    def load_to_dwh(self, file_name: str) -> None:

        self._get_data_from_s3(file_name=file_name)

        try:
            logger.info(f"Getting `{file_name}.sql`.")
            query = Path(self.path_to_sql, f"{file_name}.sql").read_text(
                encoding="UTF-8"
            )
            logger.info(f"`{file_name}.sql` loaded.")
        except Exception:
            logger.exception(
                f"Unable to load `{file_name}.sql`! Loading process failed."
            )
            raise FileSystemError

        try:
            logger.info(f"Trying to insert `{file_name}` data into DWH.")
            with self.dwh_conn.begin() as conn:
                conn.execute(
                    statement=text(
                        query.format(path=Path(self.path_to_data, f"{file_name}.csv"))
                    )
                )
            logger.info(f"Successfully inserted `{file_name}` data.")
        except Exception:
            logger.exception(f"Unable to insert `file_name`! ")
            raise VerticaError


if __name__ == "__main__":
    #TODO make dags for dwh cration and stg data loading

    # creator = DWHCreator()
    # creator.create_stg_layer()
    # creator.create_dds_layer()

    # data_loader = StagingDataLoader()
    # data_loader.load_to_dwh(file_name="users")
