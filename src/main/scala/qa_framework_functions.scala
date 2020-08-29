
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object qa_framework_functions {
	def get_kpi_val(spark:SparkSession,query:String,environment:String) : Double ={
		var df_val=0.0
		if (environment.toLowerCase()=="hive"){
			var df_val=spark.sql(query).first().getAs[Double](0)
		}else{
			var df_val=spark.sql(query).first().getAs[Double](0)
		}
		return df_val
	}
def calculate_status(spark:SparkSession,kpi_val:Double,kpi_avg:Double,variance_tolerance_limit:String,dag_exec_dt:String,parent_id:String) : String ={
		if (variance_tolerance_limit.toInt==100 && kpi_val<=0.0){
			return "Failed"
		}
		if ( ( ((Math.abs(kpi_val - kpi_avg)/kpi_avg) *100) > variance_tolerance_limit.toInt) ){
			print ("inside 1")
			return "Failed"
		}
		if (parent_id.length >1){	
			var parent_id_val=spark.sql(s"""select cast(coalesce(kpi_val,0.0) as double) as kpi_val from dev_eda_common.mpa_audit_metric_table  where id='$parent_id' and process_dt='$dag_exec_dt'""").first().getAs[Double](0)
			if (parent_id_val != kpi_val){
				return "Failed"
			}
		}
		return "Success"
	}

	def get_average(spark:SparkSession,season_flg:String,id:String,dag_exec_dt:String) : Double ={
		var kpi_avg=0
		var ret_kpi_avg=0.0
		print ("\n before check ")
		if (season_flg=='Y') {
			print ("inside season condition")

		}
		if (season_flg=='N') {
			print ("\n  inside season condition with N")

		}
		if (season_flg.trim()=="") {
			print ("\n  inside season condition with N")

		}
		else {
			print ("\n  inside  else ")
			var comp_dt_val = dag_exec_dt
			print ("comp_dt_val is :  "+comp_dt_val)
			var compar_dt = LocalDate.parse(comp_dt_val, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
			var mod_compar_dt=compar_dt.minusDays(season_flg.toInt)
			print ("\n mod_compar_dt is :  "+mod_compar_dt,id)
			print ("\n id is :  "+mod_compar_dt,id)

			var kpi_avg=spark.sql(s"""select cast(coalesce(avg(coalesce(kpi_val,0.0)),0.0) as double) as kpi_val
									 from 
									   dev_eda_common.mpa_audit_metric_table  
									   where id='$id' and process_dt >= '$mod_compar_dt'""").first().getAs[Double](0)
			print ("\n kpi_avg is "+kpi_avg)
			/*
			var kpi_avg1=spark.sql(s"""select cast(coalesce(avg(coalesce(kpi_val,0.0)),0.0) as double) as kpi_val
									 from 
									   dev_eda_common.mpa_audit_metric_table  
									   where id='a20191108200253_54ed432e_2dd7_4358_a7z' """).first().getAs[Double](0)
			*/
			if (kpi_avg >0){
				 ret_kpi_avg=kpi_avg
			} else{
				ret_kpi_avg=0.0
			}
					print (" \n kpi_avg is "+ret_kpi_avg)
		}
		return ret_kpi_avg
	}

	def quality_parameter_extraction( spark:SparkSession,id:String ,dag_exec_dt:String) : (String, String, String, String, String, String, String, String, String, String, String,String,String)  = {
	 	val reslt_without_query=spark.sql(s"""select coalesce(table_name,''),
	 												 coalesce(season_flag,''),
	 												 coalesce(kpi,''),
	 												 coalesce(environment,''),
	 												 coalesce(full_tbl_scan,''),
	 												 coalesce(canary_flag,''),
	 												 coalesce(variance_tolerance_limit,''),
	 												 coalesce(condition_to_check,'') ,
	 												 coalesce(partition_col_nm,''),
	 												 coalesce(query,''),
	 												 coalesce(parent_id,''),
	 												 coalesce(team_name,'')
	 										from eda_common.eda_quality_framework_config_ddb 
	 										where id='$id'""").collect()
	 	
	 	val table_name = reslt_without_query.map(x => x.get(0)).mkString(",")
	 	var season_flag = reslt_without_query.map(x => x.get(1)).mkString(",")
	 	val kpi = reslt_without_query.map(x => x.get(2)).mkString(",")
	 	val environment = reslt_without_query.map(x => x.get(3)).mkString(",")
	 	var full_tbl_scan = reslt_without_query.map(x => x.get(4)).mkString(",")
	 	val canary_flag = reslt_without_query.map(x => x.get(5)).mkString(",")
	 	val variance_tolerance_limit = reslt_without_query.map(x => x.get(6)).mkString(",")
	 	val condition_to_check = reslt_without_query.map(x => x.get(7)).mkString(",")
	 	val partition_col_nm = reslt_without_query.map(x => x.get(8)).mkString(",")
	 	val query_temp = reslt_without_query.map(x => x.get(9)).mkString(",")
	 	val parent_id = reslt_without_query.map(x => x.get(10)).mkString(",")
	 	val team_name = reslt_without_query.map(x => x.get(11)).mkString(",")
	 	//print ("full_tbl_scan"+full_tbl_scan,"partition_col_nm"+partition_col_nm)
		print ("\n  partition_col_nm IS :"+partition_col_nm)
		print ("\n  full_tbl_scan IS :"+full_tbl_scan)
		print ("\n  condition_to_check IS :"+condition_to_check)
		print ("\n  After condition to check")
	 	var query = "select cast("+kpi+" as double) as kpi_val from "+table_name
	 	full_tbl_scan="Y"
	 	season_flag="100"
	 	print ("\n  full_tbl_scan IS :"+full_tbl_scan)
	 	if (full_tbl_scan=='Y'){
	 		query = "select cast("+kpi+" as double) as kpi_val from "+table_name
	 	}
	 	
	 	if (condition_to_check.length >1 && partition_col_nm.length <=1 && full_tbl_scan !='Y' ) {
	 		query = "select cast("+kpi+" as double) as kpi_val  from "+table_name+" where "+condition_to_check
	 		print ("\n  inside where condition to check \n")
	 		}
	 	print ("partition_col_nm.length :"+partition_col_nm.length ,"full_tbl_scan:"+full_tbl_scan)

	 	if (partition_col_nm.length >1 && full_tbl_scan !="Y" && condition_to_check.length >1 ){
	 		query = "select cast("+kpi+" as double) as kpi_val from "+table_name+" where "+condition_to_check+"and "+partition_col_nm+" = '"+dag_exec_dt+"'"
	 		print ("\n  inside where condition to check and partition column condition \n")

	 	}
	 	if (partition_col_nm.length >1 && full_tbl_scan !="Y" && condition_to_check.length <=1 ){

	 		query = "select cast("+kpi+"  as double) as kpi_val from "+table_name+" where  "+partition_col_nm+" = '"+dag_exec_dt+"'"
	 		print ("\n  inside partition column condition \n")
	 	}
	 	else{
	 		if (query_temp.length>0){
	 			query=query
	 		}
	 	}
		 return (season_flag,query,table_name,kpi,full_tbl_scan,canary_flag,variance_tolerance_limit,condition_to_check,partition_col_nm,query_temp,parent_id,environment,team_name)
		}
}
