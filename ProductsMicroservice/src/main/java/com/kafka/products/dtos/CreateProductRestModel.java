package com.kafka.products.dtos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateProductRestModel {
	private String title;
	private BigDecimal price;
	private Integer quantity;
}
