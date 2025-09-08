### 1. Dockerイメージのビルド

各サービスのDockerイメージをビルドします：

```bash
docker build -t kafka-consumer:latest -f src/consumer/Dockerfile .
docker build -t kafka-producer:latest -f src/producer/Dockerfile .
docker build -t kafka-aggregator:latest -f src/aggregator/Dockerfile .
```

### 2. Kubernetesクラスターでのデプロイ

#### 2.1 Kafkaクラスターの起動
```bash
kubectl apply -f ./manifests/kafka.yaml
```

### 2.2 Kafkaのトピックを作成
```bash
kafka-topics.sh \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic control-topic

kafka-topics.sh \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 10 \
  --topic task-topic

kafka-topics.sh \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic result-topic

# describe
kafka-topics.sh \
  --describe \
  --bootstrap-server localhost:9092 \
  --topic task-topic

# alter
kafka-topics.sh \
  --alter \
  --bootstrap-server localhost:9092 \
  --topic task-topic \
  --partitions 20
```

#### 2.2 各サービスのデプロイ
```bash
# Consumerのデプロイ
kubectl apply -f ./manifests/consumer.yaml

# Aggregatorのデプロイ
kubectl apply -f ./manifests/aggregator.yaml

# Producerのデプロイ
kubectl apply -f ./manifests/producer.yaml
```

### 3. 動作確認

#### 3.1 Kafka CLIでの確認
```bash
# Kafka CLIポッドの起動
kubectl apply -f ./manifests/kafka-cli.yaml

# CLIポッドに接続
kubectl exec -it kafka-cli -- /bin/bash

# トピック一覧の確認
kafka-topics --bootstrap-server kafka:9092 --list

# メッセージの確認
kafka-console-consumer --bootstrap-server kafka:9092 --topic test-topic --from-beginning
```
