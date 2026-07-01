package com.kafka.products.service;

import com.kafka.products.dtos.CreateProductRestModel;

public interface ProductService {
	
	String createProduct(CreateProductRestModel productRestModel) throws Exception ;

}
