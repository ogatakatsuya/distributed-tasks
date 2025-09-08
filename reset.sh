docker build -t kafka-consumer:latest -f src/consumer/Dockerfile .
docker build -t kafka-producer:latest -f src/producer/Dockerfile .
docker build -t kafka-aggregator:latest -f src/aggregator/Dockerfile .

kubectl delete job aggregator
kubectl delete job producer

kubectl rollout restart deployment consumer
sleep 5
kubectl apply -f manifests/aggregator.yaml
sleep 3
kubectl apply -f manifests/producer.yaml