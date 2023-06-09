package cz.edu.poc.bigquery.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.FormatOptions
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.WriteChannelConfiguration
import cz.edu.poc.bigquery.config.BigQueryProductProperties
import cz.edu.poc.bigquery.dto.BigQueryProductDTO
import cz.edu.poc.bigquery.mapper.BigQueryProductMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.util.StopWatch
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.nio.channels.Channels
import java.nio.file.Files
import java.nio.file.Path
import kotlin.math.min


@Service
class BigQueryService(
    private val bigQuery: BigQuery,
    private val properties: BigQueryProductProperties,
    private val mapper: BigQueryProductMapper
) {

    fun readValues(): List<BigQueryProductDTO> {
        val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(selectQuery).build()

        var queryJob: Job? = bigQuery.create(JobInfo.newBuilder(queryConfig).build())
        queryJob = queryJob?.waitFor()

        if (queryJob == null) {
            throw Exception("job no longer exists")
        } else if (queryJob.status.error != null) {
            throw Exception(queryJob.status.error.toString())
        }

        return queryJob.getQueryResults().values.map {
            mapper.convertToDTO(it)
        }
    }

    fun exportProducts(products: List<BigQueryProductDTO>): Boolean {
        // convert list to provider, which is simulating batch query to database
        val dataProvider: (Int, Int) -> List<BigQueryProductDTO> = { offset: Int, batchSize: Int ->
            if (offset >= products.size) {
                emptyList()
            } else {
                products.subList(offset, min(products.size, offset + batchSize))
            }
        }

        return exportValues(dataProvider)
    }

    fun exportProductsPerformanceTest(): Long {
        val watch = StopWatch()

        val productTemplate = BigQueryProductDTO(
            longArticleId = "1",
            title = "Example Product 2 - updated",
            article = "ABC123",
            descriptionContent = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
            mainCategoryTitle = "Laptops",
            categoryTree = "Electronics > Computers & Tablets > Laptops",
            image = "https://example.com/image1.jpg",
            producerTitle = "Example Manufacturer 1",
            modified = "2022-04-22T10:31:00Z"
        )

        watch.start()

        val dataProvider: (Int, Int) -> List<BigQueryProductDTO> = { offset: Int, batchSize: Int ->
            if (offset > properties.performanceTestSize) {
                emptyList()
            } else {
                IntRange(offset, offset + batchSize - 1).map {
                    productTemplate.copy(
                        longArticleId = it.toString()
                    )
                }
            }
        }

        exportValues(dataProvider)
        watch.stop()
        return watch.totalTimeMillis.also {
            LOG.info("Performance run finished, total millis: $it")
        }
    }


    private fun exportValues(dataProvider: (offset: Int, batchSize: Int) -> List<BigQueryProductDTO>): Boolean {
        return try {
            LOG.debug("Converting product list to JSON file.")
            convertProductsToJSON(dataProvider, properties.exportFilePath)
            LOG.debug("Export products to BigQuery tmp table.")
            exportProductsToTmpTable()
            LOG.debug("Executing merge command.")
            mergeProductsToBqTable()
            LOG.info("Data merged successfully into BigQuery table")
//            File(properties.exportFilePath).delete()
            true
        } catch (ex: IOException) {
            LOG.error("Error during work with export JSON file.", ex)
            false
        } catch (ex: InterruptedException) {
            LOG.error("Export of product to BigQuery failed.", ex)
            false
        } catch (ex: BigQueryException) {
            LOG.error("Export of product to BigQuery failed.", ex)
            false
        }
    }

    private fun mergeProductsToBqTable() {
        val mergeQueryConfig = QueryJobConfiguration.newBuilder(mergeQuery).build()
        val mergeJob = bigQuery.create(JobInfo.of(mergeQueryConfig))
        mergeJob.waitFor()
    }

    private fun exportProductsToTmpTable() {
        val tmpTableId = TableId.of(
            properties.datasetName,
            properties.tempMergeTableName
        )

        val writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tmpTableId)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
            .setFormatOptions(FormatOptions.json()).build()

        val writer = bigQuery.writer(writeChannelConfiguration)

        Channels.newOutputStream(writer)
            .use { stream -> Files.copy(Path.of(properties.exportFilePath), stream) }

        val job = writer.job.waitFor()

        job.status?.executionErrors?.forEach {
            LOG.warn("Error during exporting discounts: ${it.message}")
        }
        
        println("State: " + job.status.state)
    }

    fun convertProductsToJSON(
        dataProvider: (offset: Int, batchSize: Int) -> List<BigQueryProductDTO>,
        filename: String
    ) {
        BufferedWriter(FileWriter(filename)).use { writer ->
            val objectMapper = ObjectMapper()
            var rowsInserted = 0
            var batch: List<BigQueryProductDTO> = dataProvider(rowsInserted, properties.batchSize)

            while (batch.isNotEmpty()) {
                batch.map { mapper.convertToBigQueryRowContent(it) }
                    .forEach {
                        writer.appendLine(objectMapper.writeValueAsString(it))
                    }

                rowsInserted += properties.batchSize
                batch = dataProvider(rowsInserted, properties.batchSize)
            }
        }
    }

    val selectQuery = """
        SELECT 
            longArticleId,
            title,
            article,
            descriptionContent,
            mainCategoryTitle,
            categoryTree,
            image,
            producerTitle,
            modified
        FROM `${properties.datasetName}.${properties.tableName}`
    """.trimIndent()

    private val mergeQuery = """
        MERGE `${properties.datasetName}.${properties.tableName}` t 
            USING `${properties.datasetName}.${properties.tempMergeTableName}` s 
            ON t.longArticleId = s.longArticleId WHEN 
        MATCHED THEN UPDATE SET 
            t.title = s.title, 
            t.article = s.article, 
            t.descriptionContent = s.descriptionContent, 
            t.mainCategoryTitle = s.mainCategoryTitle, 
            t.categoryTree = s.categoryTree, 
            t.image = s.image, 
            t.producerTitle = s.producerTitle, 
            t.modified = s.modified 
        WHEN NOT MATCHED THEN INSERT (
            longArticleId, 
            title, 
            article, 
            descriptionContent, 
            mainCategoryTitle, 
            categoryTree, 
            image, 
            producerTitle, 
            modified
        ) VALUES (
            s.longArticleId, 
            s.title, 
            s.article, 
            s.descriptionContent, 
            s.mainCategoryTitle, 
            s.categoryTree, 
            s.image, 
            s.producerTitle, 
            s.modified
        )
        """
        .trimIndent()

    companion object {
        private val LOG = LoggerFactory.getLogger(BigQueryService::class.java)

    }
}
