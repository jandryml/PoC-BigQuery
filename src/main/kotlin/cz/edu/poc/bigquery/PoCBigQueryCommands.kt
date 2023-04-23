package cz.edu.poc.bigquery

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class PoCBigQueryCommands

fun main(args: Array<String>) {
	runApplication<PoCBigQueryCommands>(*args)
}
