package com.atguigu.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public interface PublisherService {
    Long getDau(String date);

    Map<String, Long> getHourDau(String date);


}