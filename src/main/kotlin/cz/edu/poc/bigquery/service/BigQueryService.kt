package cz.edu.poc.bigquery.service

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.InsertAllResponse
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableId
import cz.edu.poc.bigquery.config.BigQueryProductProperties
import cz.edu.poc.bigquery.dto.BigQueryProductDTO
import cz.edu.poc.bigquery.mapper.BigQueryProductMapper
import org.springframework.stereotype.Service


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

    fun writeValues(products: List<BigQueryProductDTO>): Boolean {
        val tmpTableId = TableId.of(
            properties.datasetName,
            properties.tempMergeTableName
        )

        val requestBuilder = InsertAllRequest.newBuilder(tmpTableId)

        products.forEach {
            requestBuilder.addRow(mapper.convertToBigQueryRowContent(it))
        }

        val response: InsertAllResponse = bigQuery.insertAll(requestBuilder.build())

        if (response.hasErrors()) {
            for (entry in response.insertErrors.entries) {
                println("Error on row ${entry.key}: ${entry.value}")
            }
        } else {
            println("Rows successfully inserted.")
        }

        return try {
            val mergeQueryConfig = QueryJobConfiguration.newBuilder(mergeQuery).build()
            val mergeJob = bigQuery.create(JobInfo.of(mergeQueryConfig))
            mergeJob.waitFor()
            println("Data merged successfully into BigQuery table")

            true
        } catch (ex: BigQueryException) {
            println("Export of product to BigQuery failed.")
            false
        } catch (ex: InterruptedException) {
            println("Export of product to BigQuery failed. ")
            false
        } finally {
            val truncateQueryConfig = QueryJobConfiguration.newBuilder(truncateQuery).build()
            val truncateJob = bigQuery.create(JobInfo.of(truncateQueryConfig))
            truncateJob.waitFor()
            println("Truncate finished")
        }
    }

    val selectQuery ="""
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

    private val truncateQuery = "TRUNCATE TABLE `${properties.datasetName}.${properties.tempMergeTableName}`"
}
