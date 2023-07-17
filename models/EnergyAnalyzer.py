from pyspark.sql import SparkSession


class EnergyAnalyzer:
    hdfs_path = None
    energy_data = None
    session = None

    def __init__(
            self,
            session: SparkSession,
            hdfs_path: str
    ):
        self.session = session
        self.hdfs_path = hdfs_path

    def load_data_from_csv(
            self,
            file_name: str,
            schema: str = None
    ) -> None:
        """
            Load data from a CSV file
        :param schema:
        :param file_name:
        :return:
        """
        self.energy_data = self.session.read.csv(
            path=self.hdfs_path + file_name,
            header=True,
            schema=schema if schema is not None else None
        )
