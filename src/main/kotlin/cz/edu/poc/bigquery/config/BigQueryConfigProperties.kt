package cz.edu.poc.bigquery.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConfigurationProperties(BigQueryConfigProperties.PROP_BASE)
class BigQueryConfigProperties @ConstructorBinding constructor(
    val credentialsFilePath: String
) {
    companion object {
        const val PROP_BASE = "bigquery.config"
    }
}
