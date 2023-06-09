package cz.edu.poc.bigquery.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConfigurationProperties(prefix = BigQueryProductProperties.PROP_BASE)
class BigQueryProductProperties @ConstructorBinding constructor(
    val datasetName: String,
    val tableName: String,
    val tempMergeTableName: String,
    val exportFilePath: String,
    val batchSize: Int,
    val performanceTestSize: Int
) {
    companion object {
        const val PROP_BASE = "big-query.product"
    }
}
