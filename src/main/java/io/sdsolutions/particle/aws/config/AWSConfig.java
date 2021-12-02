package io.sdsolutions.particle.aws.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import io.sdsolutions.particle.aws.services.AmazonS3Service;
import io.sdsolutions.particle.aws.services.impl.AmazonS3ServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

public class AWSConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSConfig.class);

    private final Environment environment;

    public AWSConfig(Environment environment) {
        this.environment = environment;
    }

    @Bean
    @ConditionalOnProperty(name = "aws.s3.enabled", havingValue = "true")
    public AmazonS3 amazonS3() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(environment.getProperty("aws.s3.bucket.region"))
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    @Bean
    @ConditionalOnProperty(name = "aws.s3.enabled", havingValue = "true")
    public AmazonS3Service amazonS3Service(Environment environment, AmazonS3 amazonS3) {
        return new AmazonS3ServiceImpl(environment, amazonS3);
    }

    @Bean
    @ConditionalOnProperty(name = "aws.dynamodb.enabled", havingValue = "true")
    public AmazonDynamoDB amazonDynamoDbClient() {
        return AmazonDynamoDBClientBuilder.defaultClient();
    }

    @Bean
    @ConditionalOnProperty(name = "aws.dynamodb.enabled", havingValue = "true")
    public DynamoDB dynamoDb(AmazonDynamoDB amazonDynamoDBClient) {
        return new DynamoDB(amazonDynamoDBClient);
    }

}
