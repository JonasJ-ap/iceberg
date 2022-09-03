/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Tag;

public class AssumeRoleAwsClientFactory implements AwsClientFactory {

  private String roleArn;
  private String externalId;
  private Set<Tag> tags;
  private int timeout;
  private String region;
  private AwsProperties awsProperties;

  @Override
  public S3Client s3() {
    return S3Client.builder()
        .applyMutation(this::configure)
        .applyMutation(
            builder -> AwsClientFactories.configureEndpoint(builder, awsProperties.s3Endpoint()))
        .applyMutation(awsProperties::applyS3CredentialConfigurations)
        .build();
  }

  @Override
  public GlueClient glue() {
    return GlueClient.builder().applyMutation(this::configure).build();
  }

  @Override
  public KmsClient kms() {
    return KmsClient.builder().applyMutation(this::configure).build();
  }

  @Override
  public DynamoDbClient dynamo() {
    return DynamoDbClient.builder()
        .applyMutation(this::configure)
        .applyMutation(
            builder ->
                AwsClientFactories.configureEndpoint(builder, awsProperties.dynamodbEndpoint()))
        .build();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.awsProperties = new AwsProperties(properties);
    this.roleArn = properties.get(AwsProperties.CLIENT_ASSUME_ROLE_ARN);
    Preconditions.checkNotNull(
        roleArn, "Cannot initialize AssumeRoleClientConfigFactory with null role ARN");
    this.timeout =
        PropertyUtil.propertyAsInt(
            properties,
            AwsProperties.CLIENT_ASSUME_ROLE_TIMEOUT_SEC,
            AwsProperties.CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT);
    this.externalId = properties.get(AwsProperties.CLIENT_ASSUME_ROLE_EXTERNAL_ID);

    this.region = properties.get(AwsProperties.CLIENT_ASSUME_ROLE_REGION);
    Preconditions.checkNotNull(
        region, "Cannot initialize AssumeRoleClientConfigFactory with null region");

    this.tags = toTags(properties);
  }

  protected <T extends AwsClientBuilder & AwsSyncClientBuilder> T configure(T clientBuilder) {
    AssumeRoleRequest request =
        AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .roleSessionName(genSessionName())
            .durationSeconds(timeout)
            .externalId(externalId)
            .tags(tags)
            .build();

    clientBuilder.credentialsProvider(
        StsAssumeRoleCredentialsProvider.builder()
            .stsClient(sts())
            .refreshRequest(request)
            .build());

    clientBuilder.region(Region.of(region));
    awsProperties.applyHttpClientConfiguration(clientBuilder);

    return clientBuilder;
  }

  protected Set<Tag> tags() {
    return tags;
  }

  protected String region() {
    return region;
  }

  protected AwsProperties awsProperties() {
    return awsProperties;
  }

  private StsClient sts() {
    return StsClient.builder().applyMutation(awsProperties::applyHttpClientConfiguration).build();
  }

  private String genSessionName() {
    return String.format("iceberg-aws-%s", UUID.randomUUID());
  }

  private static Set<Tag> toTags(Map<String, String> properties) {
    return PropertyUtil.propertiesWithPrefix(
            properties, AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX)
        .entrySet().stream()
        .map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
        .collect(Collectors.toSet());
  }
}
