
from kafka import KafkaConsumer


consumer =  KafkaConsumer("hungdepzai",
                          group_id ="group",
                          bootstrap_servers = "localhost:9092",
                          enable_auto_commit=True,
                          auto_offset_reset='latest',  # Chỉ lấy message mới
                          )
total_message_count = 0
running = True

while running:
    msg_pack = consumer.poll(timeout_ms=500)
    for tp, messages in msg_pack.items():
        for message in messages:
            print(message.value.decode('utf-8'))
            total_message_count += 1
            print(f"-------------------------{total_message_count}")