package com.hyman.rabbitmqdemo.rabbitmqBoot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * 该配置是整个程序消费者的入口。
 *
 * SpringBootApplication，这个注解是一个组合注解，聚合了多个注解的功能：排除自启动项，排除自动启动的beanName，扫描包，扫描类，
 *
 * 其中的 @EnableAutoConfiguration 这个注解是用来启动SpringBoot中的自动配置项目，这个注解是必须加上的，否则无法正常使用因为SpringBoot。
 *
 * @EnableAutoConfiguration 这个注解是用来启动SpringBoot中的自动配置项目，这个注解是必须加上的，否则无法正常使用因为SpringBoot。
 * @MapperScan 如果是使用 xml dao 文件，则需要配置。如果使用 @dao 注解接口，则不需要配置
 *
 * 在启动类中添加对mapper包扫描@MapperScan，或者直接在Mapper类上面添加注解@Mapper,建议使用上面那种，不然每个mapper加个注解也挺麻烦的。
 */

@SpringBootApplication
public class ConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}
}
