package cz.edu.poc.bigquery.config

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.io.FileInputStream

@Configuration
@EnableConfigurationProperties(BigQueryConfigProperties::class)
class BigQueryConfig(
    private val properties: BigQueryConfigProperties
) {

    @Bean
    fun getBigQuery(): BigQuery {
        val credentials = GoogleCredentials.fromStream(FileInputStream(properties.credentialsFilePath))

        return BigQueryOptions.newBuilder()
            .setCredentials(credentials)
            .build()
            .service
    }
}
