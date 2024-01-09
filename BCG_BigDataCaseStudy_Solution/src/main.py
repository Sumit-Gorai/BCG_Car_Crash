from pyspark.sql import SparkSession
from analysis import VehicleCrashesAnalysis
from utils import utilities as utils

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("CrashAnalysis") \
        .getOrCreate()

    config_path = "config/config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    crash_analysis = VehicleCrashesAnalysis(config_path,spark)
    config = utils.read_yaml(config_path)
    output_file_paths = config.get("OUTPUT")
    file_format = config.get("FORMAT")
    mode = config.get("MODE")

    # Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
    temp1 = crash_analysis.male_killed_greater_than_2()
    result1 = spark.createDataFrame([(temp1.count())], "integer").toDF("MALE_KILLED_GREATER_THAN_2")
    utils.write_analysed_dataframe(result1, output_file_paths.get(1), file_format.get("Output"), mode.get("Output"))

    # Analysis 2: How many two wheelers are booked for crashes? 
    temp2 = crash_analysis.two_wheeler_booked()
    result2 = spark.createDataFrame([(temp2.count())], "integer").toDF("TWO_WHEELERS_BOOKED")
    utils.write_analysed_dataframe(result2, output_file_paths.get(2), file_format.get("Output"), mode.get("Output"))

    # Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    result3 = crash_analysis.top5_makes_driver_died_airbag_not_deployed()
    utils.write_analysed_dataframe(result3, output_file_paths.get(3), file_format.get("Output"), mode.get("Output"))

    # Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
    temp4 = crash_analysis.driver_valid_license_hit_and_run()
    result4 = spark.createDataFrame([(temp4.count())], "integer").toDF("VALID_LICENCES_AND_HIT_&_RUN_COUNT")
    utils.write_analysed_dataframe(result4, output_file_paths.get(4), file_format.get("Output"), mode.get("Output"))

    # Analysis 5: Which state has highest number of accidents in which females are not involved? 
    result5 = crash_analysis.highest_state_female_not_involved()
    utils.write_analysed_dataframe(result5, output_file_paths.get(5), file_format.get("Output"), mode.get("Output"))

    # Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    result6 = crash_analysis.top_3rd_to_5th_make_largest_inj()
    utils.write_analysed_dataframe(result6, output_file_paths.get(6), file_format.get("Output"), mode.get("Output"))

    # Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
    result7 = crash_analysis.top_ethnic_user_group_each_body_style()
    utils.write_analysed_dataframe(result7, output_file_paths.get(7), file_format.get("Output"), mode.get("Output"))

    # Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
    result8 = crash_analysis.top_5_zip_codes_alcohol_cf()
    utils.write_analysed_dataframe(result8, output_file_paths.get(8), file_format.get("Output"), mode.get("Output"))

    # Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    temp9 = crash_analysis.damage_prop_level_abv_4_avails_insurance()
    result9 = spark.createDataFrame([(temp9.count())], "integer").toDF("COUNT_DISTINCT_CRASH_ID_NO_DP_DAMAGE_LVEL>4_AVAILS_INSURANCE")
    utils.write_analysed_dataframe(result9, output_file_paths.get(9), file_format.get("Output"), mode.get("Output"))


    # Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    result10 = crash_analysis.top_5_drivers_with_conditions()
    utils.write_analysed_dataframe(result10, output_file_paths.get(10), file_format.get("Output"), mode.get("Output"))
    
    spark.stop()