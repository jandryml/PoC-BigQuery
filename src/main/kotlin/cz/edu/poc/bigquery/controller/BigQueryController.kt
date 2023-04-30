package cz.edu.poc.bigquery.controller

import cz.edu.poc.bigquery.dto.BigQueryProductDTO
import cz.edu.poc.bigquery.service.BigQueryService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class BigQueryController(
    private val bigQueryService: BigQueryService
) {

    @GetMapping("/products")
    fun getValues(): List<BigQueryProductDTO> {
        return bigQueryService.readValues()
    }

    @PostMapping("/products")
    fun insertValues(@RequestBody products: List<BigQueryProductDTO>): Boolean {
        return bigQueryService.exportValues(products)
    }
}
