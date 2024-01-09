from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, regexp_extract
from utils import utilities as utils


class VehicleCrashesAnalysis:
    def __init__(self, config_file_path,spark):
        input_file_paths = utils.read_yaml(config_file_path).get("DATASETS")
        self.charges_df = utils.load_csv_data_to_df(spark, input_file_paths.get("Charges"))
        self.charges_df = self.charges_df.dropDuplicates()
        self.damage_df = utils.load_csv_data_to_df(spark, input_file_paths.get("Damages"))
        self.damage_df = self.damage_df.dropDuplicates()
        self.endorse_df = utils.load_csv_data_to_df(spark, input_file_paths.get("Endorse"))
        self.endorse_df = self.endorse_df.dropDuplicates()
        self.person_df = utils.load_csv_data_to_df(spark, input_file_paths.get("Primary_Person"))
        self.person_df = self.person_df.dropDuplicates()
        self.units_df = utils.load_csv_data_to_df(spark, input_file_paths.get("Units"))
        self.units_df = self.units_df.dropDuplicates()
        self.restrict_df = utils.load_csv_data_to_df(spark, input_file_paths.get("Restrict"))
        self.restrict_df = self.restrict_df.dropDuplicates()



    def male_killed_greater_than_2(self):
        """Crashes (accidents) in which number of males killed are greater than 2

        Returns:
            df(Pyspark DataFrame object): Crashes (accidents) in which number of males killed are greater than 2
        """
              
        df = self.person_df.filter((col('PRSN_GNDR_ID') =='MALE') & (col('DEATH_CNT')>0)).groupBy('CRASH_ID').count()\
            .filter("count>2")
        return df

    def two_wheeler_booked(self):
        """Two wheelers booked for crashes

        Returns:
            df(Pyspark DataFrame object): Two wheelers booked for crashes
        """
        
        df = self.units_df.filter(col('VEH_BODY_STYL_ID').contains('MOTORCYCLE'))
        return df

    def top5_makes_driver_died_airbag_not_deployed(self):
        """Top 5 Vehicle Makes present in the crashes in which driver died and Airbags did not deploy.

        Returns:
            df(Pyspark DataFrame object): Top 5 Vehicle Makes
        """

        df = self.person_df.filter((col('DEATH_CNT')>0) & (col('PRSN_AIRBAG_ID')=='NOT DEPLOYED'))\
            .join(self.units_df,on=['CRASH_ID','UNIT_NBR'],how="Inner")\
            .groupBy('VEH_MAKE_ID').count()\
            .orderBy(col('count').desc())\
            .limit(5)\
            .select('VEH_MAKE_ID')
        return df

    def driver_valid_license_hit_and_run(self):
        """Vehicles with driver having valid licences involved in hit and run

        Returns:
            df(Pyspark DataFrame object)):  Vehicles with driver having valid licences involved in hit and run
        """
        df = self.units_df.filter(col('VEH_HNR_FL')=='Y')\
            .join(self.person_df,on=['CRASH_ID','UNIT_NBR'],how="inner")\
            .filter(~col("DRVR_LIC_CLS_ID").isin(['UNLICENSED','NA']))

        return df

    def highest_state_female_not_involved(self):
        """State having highest number of accidents in which females are not involved

        Returns:
            df(Pyspark DataFrame object): State having highest number of accidents in which females are not involved
        """

        df = self.person_df.filter((col('PRSN_GNDR_ID') != "FEMALE") & (~col('DRVR_LIC_STATE_ID').isin(['NA','Unknown','Other'])))\
            .groupBy("DRVR_LIC_STATE_ID").count(). \
            orderBy(col("count").desc()).limit(1)\
            .select("DRVR_LIC_STATE_ID")
        
        return df

    def top_3rd_to_5th_make_largest_inj(self):
        """Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        Returns:
            df(Pyspark DataFrame object): Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        df = self.units_df.filter(col('VEH_MAKE_ID')!='NA')\
            .withColumn('INJURY_DEATH_TOTAL',col('TOT_INJRY_CNT')+col('DEATH_CNT'))\
            .groupBy('VEH_MAKE_ID').agg(sum('INJURY_DEATH_TOTAL').alias('SUM_INJURED_DEATH'))\
            .orderBy(col('SUM_INJURED_DEATH').desc())
        
        df = df.limit(5).subtract(df.limit(2))

        return df

    def top_ethnic_user_group_each_body_style(self):
        """Top ethnic user group of each unique body style  

        Returns:
            df(Pyspark DataFrame object):Top ethnic user group of each unique body style 
        """

        df = self.units_df.filter((~col('VEH_BODY_STYL_ID').isin(['NA', 'UNKNOWN', 'NOT REPORTED'])) & (~col('VEH_BODY_STYL_ID').like('OTHER%')))\
        .join(self.person_df.filter(~col('PRSN_ETHNICITY_ID').isin(["NA", "UNKNOWN"])),on=['CRASH_ID','UNIT_NBR'],how="inner")\
        .groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').count()
   
        window = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc())
        df= df.withColumn("EG_RANK",row_number().over(window)).filter(col('EG_RANK')==1).drop('EG_RANK','count')

        return df

    def top_5_zip_codes_alcohol_cf(self):
        """Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash

        Returns:
            DataFrame(Pyspark DataFrame object): Top 5 Zip Codes
        """

        df = self.units_df.filter((col('CONTRIB_FACTR_1_ID').contains('ALCOHOL'))|(col('CONTRIB_FACTR_2_ID').contains('ALCOHOL'))|(col('CONTRIB_FACTR_P1_ID').contains('ALCOHOL'))|(col('CONTRIB_FACTR_1_ID').contains('DRINKING'))|(col('CONTRIB_FACTR_2_ID').contains('DRINKING'))|(col('CONTRIB_FACTR_P1_ID').contains('DRINKING')))\
            .join(self.person_df.dropna(subset=["DRVR_ZIP"]),on=['CRASH_ID','UNIT_NBR'],how="inner")\
            .groupBy('DRVR_ZIP').count()\
            .orderBy(col('count').desc())\
            .limit(5)\
            .select('DRVR_ZIP')

        return df

    def damage_prop_level_abv_4_avails_insurance(self):
        """Unique Property where:
                1. No Damaged Property was observed.
                2. Damage Level (VEH_DMAG_SCL~) is above 4.
                3. Car is Insured. 

        Returns:
            df : Distinct Crash IDs
        """

        insured=self.charges_df.filter(col('CHARGE').contains('NO'))\
                .filter(col('CHARGE')\
                .contains('INSURANCE'))\
                .select('CRASH_ID','UNIT_NBR').distinct()
        nodamage=self.damage_df.filter((col('DAMAGED_PROPERTY').isNull())|(col('DAMAGED_PROPERTY').contains('NONE')))\
                .distinct().select('CRASH_ID')

        df = self.units_df.join(insured,on=['CRASH_ID','UNIT_NBR'],how="inner")\
                    .join(nodamage,on=['CRASH_ID'],how="inner")\
                    .withColumn('DMG_1_LEVEL',regexp_extract(col('VEH_DMAG_SCL_1_ID'), "\\d+", 0))\
                    .withColumn('DMG_2_LEVEL',regexp_extract(col('VEH_DMAG_SCL_2_ID'), "\\d+", 0)) \
                    .filter((col('DMG_1_LEVEL').cast('int') > 4) | (col('DMG_2_LEVEL').cast('int') > 4))\
                    .select('CRASH_ID').distinct()

        return df
    
    def top_5_drivers_with_conditions(self):
        """Top 5 Vehicle Makes based on:
                1. Where drivers are charged with speeding related offences
                2. Has licensed Drivers
                3. Used top 10 used vehicle colours
                4. Has car licensed with the Top 25 states with highest number of offences 

        Returns:
            DataFrame(Pyspark DataFrame object):Top ethnic user group of each unique body style 
        """

        top10colourdf = self.units_df.filter(col('VEH_COLOR_ID') != 'NA')\
                        .groupBy('VEH_COLOR_ID').count()\
                        .withColumn('RN',row_number().over(Window.orderBy(col('count').desc()))).filter("RN <=10").select('VEH_COLOR_ID')
        top10States = self.units_df.filter(col('VEH_LIC_STATE_ID')!='NA')\
                    .groupBy('VEH_LIC_STATE_ID').count()\
                    .withColumn('RN',row_number().over(Window.orderBy(col('count').desc()))).filter("RN <=25").select('VEH_LIC_STATE_ID')

        df = self.charges_df.filter(col('CHARGE').contains('SPEED')).distinct()\
            .join(self.person_df.filter(~col("DRVR_LIC_CLS_ID").isin(['UNLICENSED','NA'])),on=['CRASH_ID','UNIT_NBR'],how="inner")\
            .join(self.units_df,on=['CRASH_ID','UNIT_NBR'],how="inner")\
            .join(top10colourdf,on=['VEH_COLOR_ID'],how="inner")\
            .join(top10States,on=['VEH_LIC_STATE_ID'],how="inner")\
            .groupBy('VEH_MAKE_ID').count()\
            .orderBy(col('count').desc())\
            .limit(5)\
            .select('VEH_MAKE_ID')

        return df