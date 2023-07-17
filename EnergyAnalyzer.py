from SessionBuilder import SessionBuilder


class EnergyAnalyzer:
    energy_data = None
    session = None
    hdfs_path = None

    def __init__(
            self,
            session: SessionBuilder
    ):
        session.build_spark_session()
        self.session = session.get_spark_session()
        self.hdfs_path = session.get_hdfs_path()

    def load_data_from_csv(
            self,
            file_name: str
    ) -> None:
        """
            Load data from a CSV file
        :param file_name:
        :return:
        """
        self.energy_data = self.session.read.csv(
            path=self.hdfs_path + file_name,
            header=True
        )
