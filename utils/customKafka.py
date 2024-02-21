def delivery_report(errmsg, msg):
	if errmsg is not None:
		print(f"Delivery failed for Message: {msg.key()} : {errmsg}")
		return
	print(f'Message: {msg.key()} successfully produced to Topic: {msg.topic()} Partition: [{msg.partition()}] at offset {msg.offset()}')
