### 1. Dockerイメージのビルド

各サービスのDockerイメージをビルドします：

```bash
# Consumerイメージのビルド
docker build -t kafka-consumer:latest -f ./src/consumer/Dockerfile ./src/consumer

# Producerイメージのビルド
docker build -t kafka-producer:latest -f ./src/producer/Dockerfile ./src/producer

# Aggregatorイメージのビルド
docker build -t kafka-aggregator:latest -f ./src/aggregator/Dockerfile ./src/aggregator
```

### 2. Kubernetesクラスターでのデプロイ

#### 2.1 Kafkaクラスターの起動
```bash
kubectl apply -f ./manifests/kafka.yaml
```

#### 2.2 各サービスのデプロイ
```bash
# Producerのデプロイ
kubectl apply -f ./manifests/producer.yaml

# Consumerのデプロイ
kubectl apply -f ./manifests/consumer.yaml

# Aggregatorのデプロイ
kubectl apply -f ./manifests/aggregator.yaml
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
