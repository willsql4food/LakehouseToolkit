
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.connect.session import SparkSession

# Build Lineage - enumerates the levels of a hierarchy and a concatenated lineage path
# Parameters:
#   src:        the source dataframe
#   spark:      the spark session from the caller
#   child:      the name of the field containing the child identifier
#   parent:     the name of the field containing the parent identifier
#   lineage:    the name of the field containing the element to be added to the lineage
#   delimiter:  the delimiter to be used between elements in the lineage
#               Optional - defaults to comma
#
# Returns a copy of the given dataframe with two fields added:
#   level:      depth of row in the hierarchy (0 is top / row(s) without parents)
#   lineage:    a delimited list the row and its ancestors
#
def buildLineage(src: DataFrame, spark: SparkSession, child, parent, lineage, delimiter = ","):
    # Build names for temporary objects using given parameters and a timestamp
    now = datetime.now()
    vw = f"vw_recursive_{child}_{parent}_{now.strftime('%Y%m%d')}_{now.strftime('%H%M%S')}"
    tbl = f"hive_metastore.default.tbl_recursive_{child}_{parent}_{now.strftime('%Y%m%d')}_{now.strftime('%H%M%S')}"

    # Write the source data frame to a temporary view
    src.createOrReplaceTempView(vw)

    # Create the temp table with the row(s) having no parent
    spark.sql(f"select *, 0 as level, {lineage} as lineage from {vw} where {parent} is null"
              ).writeTo(tbl).createOrReplace()
    
    # Iterate until all rows have been added to the result (with circuit breaker)
    iter = 0 
    while spark.sql(f"select * from {vw} where {child} not in (select {child} from {tbl})").count() > 0:
        iter += 1
        assert iter < 100, f"Max recursion limit ({iter}) reached"
        
        spark.sql(f"""select s.*
                        , p.level + 1 as level
                        , concat(p.lineage, '{delimiter}', s.{lineage}) as lineage
                from {vw} s 
                join {tbl} p 
                on s.{parent} = p.{child}
                where not exists (select * from {tbl} where id = s.{child})"""
                ).writeTo(tbl).append()

    # Collect the data and create a new dataframe with it
    result = spark.createDataFrame(
        spark.sql(f"select * from {tbl}").collect(), 
        spark.read.table(tbl).schema
        )

    # Clean up temporary objects
    spark.sql(f"drop view {vw}")
    spark.sql(f"drop table {tbl}")
    
    # Send the result to the caller
    return result