You have a Kafka producer (the producer.py in Docker) sending messages like "Hello from producer! {count}" into a topic (test_topic).

You have Kafka + Zookeeper running inside Docker via docker-compose.yml.

You have a Spark master and worker, ready to consume.

You have a Jupyter notebook running, connecting Spark to Kafka and displaying incoming messages!

How to run everything:

docker-compose up --build
