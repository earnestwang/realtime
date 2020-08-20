package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    DauMapper dau;
    @Override
    public Long getDau(String date) {
        return dau.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDau = dau.getHourDau(date);

        HashMap<String, Long> result = new HashMap<>();
        for (Map<String, Object> map : hourDau) {
            String hour = map.get("LOGHOUR").toString();
            Long count = (Long)map.get("COUNT");
            result.put(hour, count);
        }
        return result;
    }







    @Autowired
    OrderMapper order;
    @Override
    public Double getTotalAmount(String date) {
        return order.getTotalAmount(date);
    }

    @Override
    public Map<String, Object> getEachHourAmount(String date) {
        List<Map<String, Object>> hourOrder = order.getHourAmount(date);

        Map<String, Object> res = new HashMap<>();
        for (Map<String, Object> map : hourOrder) {
            String hour = map.get("CREATE_HOUR").toString();
            BigDecimal count = (BigDecimal) map.get("SUM");
            res.put(hour, count);
        }
        return res;
    }


}
