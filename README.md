# aws-rds-dms-kinesis-iceberg
aws cdc pipline


### CDC 파이프라인 구축

S3로 데이터 레이크를 관리하지만, 변경 데이터 처리의 한계를 맛보고 구현 진행함.


### 아키텍쳐 (AWS Submit 참고)
![참고](./img/1.png)

### 작업 간 체크
1. RDS Binlog 
- CDC 를 하려면 Binlog가 활성화 되어있어야함 [관련 Docs](https://docs.aws.amazon.com/ko_kr/dms/latest/userguide/CHAP_Source.MySQL.html)   
2. DMS 를 사용할때 public zone 에 진행되는게 아니고, Severless 솔루션을 쓴다면 VPC endpoint 생성해둬야함 [관련 Docs](https://docs.aws.amazon.com/ko_kr/dms/latest/userguide/CHAP_VPC_Endpoints.html)   
3. 당연하지만 SecretManager 도 열어둬야함   



### Issue 
1. DMS Serverless permissions issues [관련 Docs](https://docs.aws.amazon.com/ko_kr/dms/latest/userguide/security-iam-awsmanpol.html)
- DMS Serverless 를 통하여 RDS to Kinesis cdc를 구축하였는데, cloudWatch Log 생성이 안됨
- 권한 에러가 자주 표시   
-> Severless 전용 iam 과 VPC Endpoint 생성으로 해결

2. Glue receiving data from kinesis DataStream and missing some columns in schema inference
- 파악중이나, 스키마 추론 샘플링 비율을 높이면 약간 개선됨 
```
kds_df = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "~",
        "classification": "json",
        "startingPosition": "TRIM_HORIZON",
        "inferSchema": "true",
    },
    transformation_ctx="kds_df",
    additional_options={"samplingRatio": 0.15}
)

```
- Identify some missing columns within a kinesis shard  
- DMS CDC 쪽 문제 유력 




### 참고자료 
1. [실시간 CDC 데이터처리! AWS Submmit](https://aws.amazon.com/ko/blogs/tech/cdc-data-pipeline-from-db-to-opensearch-service/)