package cz.edu.poc.bigquery.dto

data class BigQueryProductDTO(
    val longArticleId: String = "",
    val title: String = "",
    val article: String = "",
    val descriptionContent: String = "",
    val mainCategoryTitle: String = "",
    val categoryTree: String = "",
    val image: String = "",
    val producerTitle: String = "",
    val modified: String = ""
)
