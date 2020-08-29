//spark-submit --packages net.snowflake:snowflake-jdbc:3.4.2,net.snowflake:spark-snowflake_2.11:2.2.8,com.amazon.emr:emr-dynamodb-hadoop:4.6.0,com.amazon.emr:emr-dynamodb-hive:4.8.0 --master local[2] --deploy-mode "client" --class "qa_framework_main" qaframework_2.11-1.0.11.jar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame 
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import qa_framework_functions._ 

object qa_framework_main {
	def main(args : Array[String]) : Unit = {

		var spark = SparkSession.builder().enableHiveSupport().getOrCreate()
		spark.conf.set("hive.mapred.mode", "nonstrict")
		spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		spark.conf.set("hive.exec.dynamic.partition", "true")
		spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
		spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
		val table_name = "dsmsca_staging.ip_doms_inventory_daily_base_t"
		val canary_value="N"
		val qry = s"""select distinct id from eda_common.eda_quality_framework_config_ddb where table_name='${table_name}' and canary_flag='$canary_value'"""
		print ("\n query is :"+qry)
		var src_tbl = spark.sql(qry)
		src_tbl.cache()
		val df1 = src_tbl.rdd.collect()
		val dag_exec_dt="2020-08-17"
		println(df1)
		src_tbl.show()
		var final_query=s"""select '0' as id,
								'0' as kpi_val,
								'Y' as full_tbl_scan,
								'0' as variance_tolerance_limit,
								'' as condition_to_check,
								'' as partition_col_nm,
								'' as parent_id,
								''+'_canary' as environment,
								'' as team_name,
								'' as process_dt,
								'' as table_name"""

		var df=spark.sql(final_query)

		for(i <- 0 until df1.length) 
		    { 
		        var id=df1(i).mkString(",")
		        print ("\n id: "+id)
		        var result_set=quality_parameter_extraction(spark,id,dag_exec_dt)
		        var season_flg=result_set._1
		        var query=result_set._2
		        var table_name=result_set._3
		        var kpi=result_set._4
		        var full_tbl_scan=result_set._5
		        var canary_flag=result_set._6
		        var variance_tolerance_limit=result_set._7
		        var condition_to_check=result_set._8
		        var partition_col_nm=result_set._9
		        var query_temp=result_set._10
		        var parent_id=result_set._11
		        var environment=result_set._12
		        var team_name=result_set._13
		        print ("\n Season_flg: "+season_flg)
		        print ("\n canary_flag is :"+canary_flag)
				print ("\n query_temp is :"+query_temp,query_temp.trim.length)

				if (query_temp.trim.length ==0){
			        var kpi_val= get_kpi_val(spark,query,environment)

			        print ("KPI VAL is :"+kpi_val)
			        //var kpi_avg=get_average(spark,season_flg,id,dag_exec_dt)
				var kpi_avg=0
			        print ("\n variance_tolerance_limit : is "+variance_tolerance_limit)
			        var status = "Failed"
				//var status = calculate_status(spark,kpi_val,kpi_avg,variance_tolerance_limit,dag_exec_dt,parent_id)
			        //final_query="""select '$id' as id,
			        print ("\n. id :"+id+"\n kpi_avg is :"+kpi_avg+"\n status is : "+status+"\n kpi_val:"+kpi_val)
			        var final_query=s"""select '$id' as id,
			        						'$kpi_val' as kpi_val,
			        						'$full_tbl_scan' as full_tbl_scan,
			        						'$variance_tolerance_limit' as variance_tolerance_limit,
			        						'$condition_to_check' as condition_to_check,
			        						'$partition_col_nm' as partition_col_nm,
			        						'$parent_id' as parent_id,
			        						'$environment'+'_canary' as environment,
			        						'$team_name' as team_name,
			        						'$dag_exec_dt' as process_dt,
			        						'$table_name' as table_name"""
			        var df_temp=spark.sql(final_query)
			        print ("\n printing the final query ")
			        if (i==0){
			        	 df = df_temp
			        }else{
			        	 df = df.union(df_temp)
			    	}
			        df.show(false)
			        
				}
				else 
				{
					print ("query processing")
				}

		    } 
		    df.show()
		    df.write.partitionBy("team_name","environment","process_dt","table_name").mode("overwrite").parquet("s3://zz-testing/jcher2/qa_testing/")
	}

}
