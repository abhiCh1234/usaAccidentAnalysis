# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from utilities.utils import *
from utilities.spark import *
from readers.load import *
from writer.write import *

class AccidentDataAnalysis:
    def __init__(self, yaml_config):
        """
        Initialize analysis class
        :params: yaml_config: yaml config parameters
        :return: dataframe count
        """
        data_paths =  yaml_config.get("INPUT_FILES_PATH")
        self.result_path = yaml_config.get("RESULT_PATH")

        # Initialze SparkSession
        self.spark = get_spark_session(yaml_config.get("SPARK_APP_NAME"))
        
        # Load dataframes
        self.df_charges = load_csv(self.spark, data_paths.get("Charges"))
        self.df_damages = load_csv(self.spark, data_paths.get("Damages"))
        self.df_endorse = load_csv(self.spark, data_paths.get("Endorse"))
        self.df_primary_person = load_csv(self.spark, data_paths.get("Primary_Person"))
        self.df_units = load_csv(self.spark, data_paths.get("Units"))
        self.df_restrict = load_csv(self.spark, data_paths.get("Restrict"))
      
    def terminate_spark_session(self):
        """
        Terminatee spark instance
        :return: Null
        """
        stop_spark(self.spark)
        
    def count_accidents_with_greater_than_2_males(self) -> int:
        """
        Crash counts where male count is greater than 2
        :return: dataframe count
        """
        df_primary_person_male = self.df_primary_person.filter(col('PRSN_GNDR_ID') == "MALE")
        male_crash_count = df_primary_person_male.groupBy('CRASH_ID') \
            .agg(count('CRASH_ID').alias('MALE_COUNT')) \
            .filter(col('MALE_COUNT')>2)
            
        write_to_parquet(male_crash_count, self.result_path.get("Analysis1"))
        
        return male_crash_count.count()

    def two_wheeler_involved_in_crash(self) -> int:
        """
        Count of two-wheeler involved in accident.
        :return: dataframe count
        """
        # As per analysis we have duplicate units in a crash
        df_units_cleaned = self.df_units.select('CRASH_ID','UNIT_NBR','VIN','VEH_BODY_STYL_ID').distinct()
        two_Wheelers_count = df_units_cleaned.filter(col('VEH_BODY_STYL_ID').contains('MOTORCYCLE'))

        write_to_parquet(two_Wheelers_count, self.result_path.get("Analysis2"))

        return two_Wheelers_count.count()
    
    def top_5_makers_with_deaths_and_airbag_failure(self) -> dict:
        """
        Top 5 makers with deaths and airbag failure.
        :return: dictionary of top 5 makers and accident count
        """
        df_pp_airbag_failure_and_death = self.df_primary_person \
            .filter(col('PRSN_DEATH_TIME').isNotNull() \
                    & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')\
                    & (col('PRSN_TYPE_ID').contains('DRIVER')))

        df_units_cleaned = self.df_units.select('CRASH_ID','UNIT_NBR','VIN','VEH_MAKE_ID').distinct()
        
        df_top_make = df_units_cleaned.filter(col('VEH_MAKE_ID')!='NA') \
            .join(df_pp_airbag_failure_and_death, ['CRASH_ID','UNIT_NBR'], 'inner') \
            .groupby('VEH_MAKE_ID') \
            .agg(count('VEH_MAKE_ID').alias('COUNT_OF_DRIVER_DEATH_AND_AB_FAILURE')) \
            .orderBy(col('COUNT_OF_DRIVER_DEATH_AND_AB_FAILURE').desc()) \
            .limit(5)

        write_to_parquet(df_top_make, self.result_path.get("Analysis3"))

        result_dict = {row['VEH_MAKE_ID']:row['COUNT_OF_DRIVER_DEATH_AND_AB_FAILURE'] \
                       for row in df_top_make.collect()}
        return result_dict
    
    def count_vehicle_with_valid_licence_and_hr(self) -> int:
        """
        Number of Vehicles with driver having valid licences involved in hit and run
        :return: count of such vehicles
        """
        df_hit_n_run = self.df_units.filter(col('VEH_HNR_FL')=='Y') \
                .select('CRASH_ID','UNIT_NBR','VIN','VEH_HNR_FL').distinct()

        # Only VALID license requirement
        df_valid_license = self.df_primary_person\
            .filter(~col('DRVR_LIC_CLS_ID').isin(['UNKNOWN','NA','UNLICENSED']) \
                    & (col('DRVR_LIC_TYPE_ID').isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])))

        df_result = df_valid_license.join(df_hit_n_run, ['CRASH_ID','UNIT_NBR'],'inner')
        df_result_vh_info = df_result.select(['CRASH_ID','UNIT_NBR','VIN',\
                                      'VEH_HNR_FL','DRVR_LIC_TYPE_ID','DRVR_LIC_CLS_ID'])

        write_to_parquet(df_result_vh_info, self.result_path.get("Analysis4"))
 
        return df_result.count()

    def get_state_with_highest_accidents_with_no_females_involved(self) -> str:
        """
        Finds state name with highest accidents with no females involved
        :return: list name with highest female accidents
        """
        # Crash Id's with females involved
        df_crash_with_females = self.df_primary_person.filter(col('PRSN_GNDR_ID') == 'FEMALE') \
            .select('CRASH_ID').distinct()
        
        # Crash Id's with no females involved
        df_no_female_crash = self.df_primary_person \
            .filter(~lower(col('DRVR_LIC_STATE_ID')).isin(['na','unknown', 'other'])) \
            .join(df_crash_with_females, ['CRASH_ID'], 'leftanti') \
            .select('CRASH_ID','DRVR_LIC_STATE_ID').distinct()

        df_result = df_no_female_crash.groupby('DRVR_LIC_STATE_ID') \
            .agg(count('DRVR_LIC_STATE_ID').alias('NO_FEMALE_ACCIDENT_COUNT')) \
            .orderBy(col('NO_FEMALE_ACCIDENT_COUNT').desc())
            
        write_to_parquet(df_result, self.result_path.get("Analysis5"))

        return df_result.first().DRVR_LIC_STATE_ID

    def get_top3to5_companies_contributing_to_injuries(self)-> list:
        """
        finds top 3rd-5th manufacturers contributing to large number of injuries and deaths
        :return: list
        """
        df_units_cleaned = self.df_units.filter(col('VEH_MAKE_ID') != 'NA') \
                    .select('CRASH_ID','UNIT_NBR','VIN','VEH_MAKE_ID','TOT_INJRY_CNT','DEATH_CNT').distinct()

        df_top_5 = df_units_cleaned.groupby('VEH_MAKE_ID') \
                    .agg(sum('TOT_INJRY_CNT').alias('AGG_INJURIES'), sum('DEATH_CNT').alias('AGG_DEATHS')) \
                    .withColumn('AGG_COUNT', col('AGG_INJURIES')+col('AGG_DEATHS')) \
                    .orderBy(col('AGG_COUNT').desc()) \
                    .limit(5)

        df_top_3_to_5 = df_top_5.withColumn('id', monotonically_increasing_id()) \
                    .filter(col('id') >= 2).drop('id')

        write_to_parquet(df_top_3_to_5, self.result_path.get("Analysis6"))

        return ([company_row['VEH_MAKE_ID'] for company_row in df_top_3_to_5.collect()])

    def get_top_ethnic_group_for_bodystyle(self):
        """
        Finds top ethnic user group for each unique body style that were involved in crashes
        :return: Dataframe
        """
        undefined_body_values = ['NOT REPORTED','OTHER  (EXPLAIN IN NARRATIVE)', 'UNKNOWN','NA']
     
        df_units_cleaned = self.df_units.filter(~col('VEH_BODY_STYL_ID').isin(undefined_body_values)) \
                .select('CRASH_ID','UNIT_NBR','VIN','VEH_BODY_STYL_ID') \
                .distinct()

        df_primary_person_cleaned = self.df_primary_person \
                    .filter(~col('PRSN_ETHNICITY_ID').isin(['NA', 'UNKNOWN', 'OTHER']))

        window = Window.partitionBy('VEH_BODY_STYL_ID')
        df_result = df_units_cleaned.join(df_primary_person_cleaned, on=['CRASH_ID','UNIT_NBR'], how='inner') \
            .groupby('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count() \
            .withColumn('rn', row_number().over(window.orderBy(col('count').desc()))) \
            .filter(col('rn') == 1) \
            .drop('rn', 'count') 
        
        write_to_parquet(df_result, self.result_path.get("Analysis7"))
        
        return df_result

    def get_top5_zipcodes_with_highest_alcohol_crashes(self)-> list:
        """
        Finds top 5 Zip Codes with highest car crash cases due to alcohol
        :return: List of Zip Codes
        """
        df_units_cleaned = self.df_units.filter(lower(col('CONTRIB_FACTR_1_ID')).contains('alcohol') \
                        | lower(col('CONTRIB_FACTR_2_ID')).contains('alcohol') \
                        | lower(col('CONTRIB_FACTR_P1_ID')).contains('alcohol')) \
                .select('CRASH_ID','UNIT_NBR','VIN','VEH_HNR_FL','VEH_BODY_STYL_ID') \
                .distinct()
        df_primary_person_cleaned = self.df_primary_person.na.drop(subset = ['DRVR_ZIP'])

        df_alc_crash = df_units_cleaned.join(df_primary_person_cleaned, on=['CRASH_ID','UNIT_NBR'], how='inner') \
                .groupBy('DRVR_ZIP').agg(count('DRVR_ZIP').alias('ALCOHOL_CRASH_COUNT')) \
                .orderBy(col('ALCOHOL_CRASH_COUNT').desc()) \
                .limit(5)
                
        write_to_parquet(df_alc_crash, self.result_path.get("Analysis8"))

        return([zip_row['DRVR_ZIP'] for zip_row in df_alc_crash.collect()])

    def get_cid_count_for_insurance_and_no_prop_dmg(self)-> list:
        """
        Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        :return: List of crash ids
        """
        df_damages_unique = self.df_damages.filter(upper(col('DAMAGED_PROPERTY')).isin(['NONE', 'NONE1'])) \
                .select('CRASH_ID','DAMAGED_PROPERTY') \
                .distinct()
                
        # Status where driver can claim insurance
        insurance_claims = ['INSURANCE BINDER', 'LIABILITY INSURANCE POLICY', 'PROOF OF LIABILITY INSURANCE']
        df_units_cleaned = self.df_units.filter(
                    (
                        ((~col('VEH_DMAG_SCL_1_ID').isin(['NA', 'NO DAMAGE', 'INVALID VALUE']))\
                            & (col('VEH_DMAG_SCL_1_ID') >= 'DAMAGED 5')) | \
                        ((~col('VEH_DMAG_SCL_2_ID').isin(['NA', 'NO DAMAGE', 'INVALID VALUE']))\
                            & (col('VEH_DMAG_SCL_2_ID') >= 'DAMAGED 5'))) \
                    & col('FIN_RESP_TYPE_ID').isin(insurance_claims)
                ) \
                .select('CRASH_ID','UNIT_NBR','VIN','VEH_DMAG_SCL_1_ID', \
                        'VEH_DMAG_SCL_2_ID','FIN_RESP_TYPE_ID') \
                .distinct()

        df_crash_id_result = df_units_cleaned.join(df_damages_unique, ['CRASH_ID'], 'inner')
        write_to_parquet(df_crash_id_result, self.result_path.get("Analysis9"))
        
        return(list(set([crash_id_row['CRASH_ID'] for crash_id_row in df_crash_id_result.collect()])))

    def get_top_5_vehicle_brand_with_speedcharges(self)-> list:
        """
        Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        :return List of Vehicle brands
        """
        df_units_cleaned = self.df_units.select('CRASH_ID','UNIT_NBR','VIN','VEH_LIC_STATE_ID','VEH_COLOR_ID','VEH_MAKE_ID').distinct()
        df_top_25_states = df_units_cleaned.filter(col('VEH_LIC_STATE_ID').cast("float").isNull()) \
            .groupBy('VEH_LIC_STATE_ID').count() \
            .orderBy(col('count').desc()).limit(25)

        df_top_10_colors = df_units_cleaned.filter((col('VEH_COLOR_ID') != 'NA') \
                                           & (col('VEH_COLOR_ID').cast("float").isNull())) \
            .groupby('VEH_COLOR_ID').count() \
            .orderBy(col('count').desc()).limit(10)

        df_pp_licenced = self.df_primary_person.filter(col('DRVR_LIC_TYPE_ID') \
                                                       .isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.']))
        df_speed_charges = self.df_charges.filter(col('CHARGE').contains('SPEED') | col('CHARGE').contains('FAST'))\
                    .select('CRASH_ID', 'UNIT_NBR').distinct()

        df_result = df_units_cleaned.join(df_speed_charges, ['CRASH_ID', 'UNIT_NBR'], 'inner') \
                            .join(df_pp_licenced, ['CRASH_ID', 'UNIT_NBR'], 'inner') \
                            .join(df_top_10_colors, ['VEH_COLOR_ID'], 'inner') \
                            .join(df_top_25_states, ['VEH_LIC_STATE_ID'], 'inner') \
                            .groupby("VEH_MAKE_ID").count() \
                            .orderBy(col("count").desc()).limit(5)
        write_to_parquet(df_result, self.result_path.get("Analysis10"))

        return([result_row['VEH_MAKE_ID'] for result_row in df_result.collect()])
    
    def getResult(self):
        """
        Executes all analytical methods in sequence
        :return None
        """
        print('Analyis 1 result-> ', \
              self.count_accidents_with_greater_than_2_males() )
        print('Analyis 2 result-> ', \
              self.two_wheeler_involved_in_crash() )
        print('Analyis 3 result-> ', \
              self.top_5_makers_with_deaths_and_airbag_failure() )
        print('Analyis 4 result-> ', \
              self.count_vehicle_with_valid_licence_and_hr() )
        print('Analyis 5 result-> ', \
              self.get_state_with_highest_accidents_with_no_females_involved() )
        print('Analyis 6 result-> ', \
              self.get_top3to5_companies_contributing_to_injuries() )
        print('Analyis 7 result-> ')
        self.get_top_ethnic_group_for_bodystyle().show()
        print('Analyis 8 result-> ', \
              self.get_top5_zipcodes_with_highest_alcohol_crashes() )
        print('Analyis 9 result-> ', \
              self.get_cid_count_for_insurance_and_no_prop_dmg() )
        print('Analyis 10 result-> ', \
              self.get_top_5_vehicle_brand_with_speedcharges() )


