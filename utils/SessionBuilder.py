from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class SessionBuilder:
    """
        This class is used to configure the SparkSession object
    """
    conf = SparkConf()  # Create a SparkConf object
    hadoop_home = "/home/hadoopuser/hadoop"  # Set HADOOP_HOME environment variable
    config_props = [
        ("spark.app.name", "EnergyAnalyzerApp"),  # Set the application name
        ("spark.master", "local[*]"),  # Set the Spark master URL
        ("spark.executor.memory", "2g"),  # Set executor memory to 2GB
        ("spark.driver.memory", "2g"),  # Set driver memory to 2GB
        # ("spark.driver.host", "192.168.1.100"),  # Set the driver IP address
        ("spark.executorEnv.LD_LIBRARY_PATH", hadoop_home+"/lib"),  # Set Hadoop's LD_LIBRARY_PATH
        ('spark.sql.repl.eagerEval.enabled', True)  # Enable eager evaluation
    ]

    def __init__(
            self: 'SessionBuilder'
    ) -> None:
        """
            Constructor
        """
        # Set configuration properties
        self.conf.setAll(self.config_props)  # Set properties

    def get_spark_session(
            self: 'SessionBuilder'
    ) -> SparkSession:
        """
            Return a SparkSession object
        :return:
        """
        return SparkSession.builder.config(conf=self.conf).getOrCreate()
