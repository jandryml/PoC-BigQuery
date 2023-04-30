package cz.edu.poc.bigquery.mapper

import com.google.cloud.bigquery.FieldValueList
import cz.edu.poc.bigquery.dto.BigQueryProductDTO
import org.springframework.stereotype.Component

@Component
class BigQueryProductMapper {

    fun convertToDTO(fieldValueList: FieldValueList): BigQueryProductDTO {
        return BigQueryProductDTO(
            fieldValueList.get("longArticleId").stringValue,
            fieldValueList.get("title").stringValue,
            fieldValueList.get("article").stringValue,
            fieldValueList.get("descriptionContent").stringValue,
            fieldValueList.get("mainCategoryTitle").stringValue,
            fieldValueList.get("categoryTree").stringValue,
            fieldValueList.get("image").stringValue,
            fieldValueList.get("producerTitle").stringValue,
            fieldValueList.get("modified").stringValue
        )
    }
}
