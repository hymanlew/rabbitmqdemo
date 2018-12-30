package com.hyman.rabbitmqdemo.rabbitmqClient.rpc;

import com.hyman.rabbitmqdemo.util.Logutil;

// 创建模拟 RPC 服务调用方法
public class RPCMethod {

    public static String addOrder(String orderInfo){
        try {
            Logutil.logger.info("orderInfo已添加到数据库");
            return "订单ID";
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}
