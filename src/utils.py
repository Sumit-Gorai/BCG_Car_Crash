import yaml

class utilities:
    def read_csv_data(spark, path):
        """
        Reads a csv file from given path into a pyspark dataframe.

        Args:
            spark : Sparksession
            path (string): path of the csv file

        Returns:
            Dataframe: Dataframe containing data values.
        """

        return spark.read.format("csv").option("header","true").option("inferSchema", "true").load(path)


    def read_yaml(yaml_path):
        """Read config file

        Args:
            yaml_path (string): path of the config file

        Returns:
            dict: config details
        """
        
        with open(yaml_path, "r") as f:
            return yaml.safe_load(f)


    def write_analysed_dataframe(df, path, format, mode):
        """Write resultant dataframe to desired location.

        Args:
            df (DataFrame): Dataframe containing the analysis result
            path (string): Output file path
            format (string): File format of the output
            mode (string): Mode of writing the file
        """

        df.write.format(format).mode(mode).option("header", "true").save(path)


