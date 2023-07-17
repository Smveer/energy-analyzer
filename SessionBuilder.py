from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class SessionBuilder:
    """
        This class is used to configure the SparkSession object
    """
    session = None  # SparkSession object
    conf = SparkConf()  # Create a SparkConf object
    hadoop_home = "/home/hadoopuser/hadoop"  # Set HADOOP_HOME environment variable
    hdfs_path = "hdfs://localhost:9000/"  # Set HDFS path
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

    def build_spark_session(
            self: 'SessionBuilder'
    ) -> None:
        """
            Set the SparkSession object
        """
        self.session = SparkSession.builder.config(conf=self.conf).getOrCreate()

    def get_spark_session(
            self: 'SessionBuilder'
    ) -> SparkSession:
        """
            Get the SparkSession object
        """
        return self.session

    def get_hdfs_path(
            self: 'SessionBuilder'
    ) -> str:
        """
            Get the HDFS path
        """
        return self.hdfs_path

    def get_hadoop_home(
            self: 'SessionBuilder'
    ) -> str:
        """
            Get the HADOOP_HOME environment variable
        """
        return self.hadoop_home

    def get_config_props(
            self: 'SessionBuilder'
    ) -> list:
        """
            Get the configuration properties
        """
        return self.config_props

    def get_spark_conf(
            self: 'SessionBuilder'
    ) -> SparkConf:
        """
            Get the SparkConf object
        """
        return self.conf

    def set_hdfs_path(
            self: 'SessionBuilder',
            hdfs_path: str
    ) -> None:
        """
            Set the HDFS path
        """
        self.hdfs_path = hdfs_path

    def set_hadoop_home(
            self: 'SessionBuilder',
            hadoop_home: str
    ) -> None:
        """
            Set the HADOOP_HOME environment variable
        """
        self.hadoop_home = hadoop_home

    def set_config_props(
            self: 'SessionBuilder',
            config_props: list
    ) -> None:
        """
            Set the configuration properties
        """
        self.config_props = config_props

    def load_spark_conf_from_list(
            self: 'SessionBuilder',
            config_props: list
    ) -> None:
        """
            Load the configuration properties from a list
        """
        self.conf.setAll(config_props)

    def __str__(
            self: 'SessionBuilder'
    ) -> str:
        """
            String representation of the object
        """
        return "SessionBuilder(conf={}, hadoop_home={}, hdfs_path={}, config_props={})".format(
            self.conf,
            self.hadoop_home,
            self.hdfs_path,
            self.config_props
        )
