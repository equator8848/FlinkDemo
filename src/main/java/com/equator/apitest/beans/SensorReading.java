package com.equator.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;

/**
 * 传感器温度读数
 *
 * @Author: Equator
 * @Date: 2021/6/18 22:03
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String id;

    private Long timestamp;

    private Double temperature;
}
