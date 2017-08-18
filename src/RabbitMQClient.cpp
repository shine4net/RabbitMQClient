#include "RabbitMQClient.h"
#include <functional>

RabbitMQClient::RabbitMQClient()
{
}


RabbitMQClient::~RabbitMQClient()
{
}

bool RabbitMQClient::connect(const char* hostname, int port, char* user, char* pwd, int heartbeat)
{
	conn = amqp_new_connection();
	socket = amqp_tcp_socket_new(conn);
	if (!socket)
	{
		die("creating TCP socket");
		return false;
	}

	int status = amqp_socket_open(socket, hostname, port);
	if (status)
	{
		this->close();
		die("opening TCP socket");
		return false;
	}

	die_on_amqp_error(amqp_login(conn, "/",
		0,				/* channel_max */
		131072,	/* max frame size, 10MB */
		heartbeat,				/* heartbeat, 30 secs */
		AMQP_SASL_METHOD_PLAIN,
		user, pwd),
		"Logging in");

	return true;
}

Channel& RabbitMQClient::createChannel()
{
	int channel_no = ++this->channel_no;
	amqp_channel_open(conn, channel_no);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening createChannel");
	return *new Channel(this, channel_no);
}

RabbitMQClient& RabbitMQClient::exchange(char* exchange, char* exchangetype)
{
	amqp_exchange_declare(conn, AMQP_CHANNEL, amqp_cstring_bytes(exchange), amqp_cstring_bytes(exchangetype),
		0, /*passive*/
		1, /*durable*/
		0, /*auto_delete*/
		0,
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
	return *this;
}

RabbitMQClient& RabbitMQClient::queue(char* queue)
{
	amqp_queue_declare(conn, AMQP_CHANNEL, amqp_cstring_bytes(queue),
		0, /*passive*/
		1, /*durable*/
		0, /*auto_delete*/
		0,
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
	return *this;
}

RabbitMQClient& RabbitMQClient::bind(char* queue, char* exchange, char* bindingkey)
{
	amqp_queue_bind(conn, AMQP_CHANNEL,
		amqp_cstring_bytes(queue),
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(bindingkey),
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Unbinding");
	return *this;
}

void RabbitMQClient::send(char* exchange, char* routingkey, char* messagebody)
{
	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 2; /* persistent delivery mode */
	die_on_error(amqp_basic_publish(conn,
		AMQP_CHANNEL,
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routingkey),
		0,
		0,
		&props,
		amqp_cstring_bytes(messagebody)),
		"Publishing");
}

void RabbitMQClient::send_amqp_bytes(amqp_bytes_t exchange, amqp_bytes_t routingkey, amqp_bytes_t messagebody)
{
	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 2; /* persistent delivery mode */
	die_on_error(amqp_basic_publish(conn,
		AMQP_CHANNEL,
		exchange,
		routingkey,
		0,
		0,
		&props,
		messagebody),
		"Publishing");
}

void RabbitMQClient::send_bytes(char* exchange, char* routingkey, void *messagebody, const int len)
{
	amqp_bytes_t message_bytes;
	message_bytes.bytes = messagebody;
	message_bytes.len = len;
	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("application/octet-stream");
	props.delivery_mode = 2; /* persistent delivery mode */
	die_on_error(amqp_basic_publish(conn,
		AMQP_CHANNEL,
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routingkey),
		0,
		0,
		&props,
		message_bytes),
		"Publishing");
}

const char* RabbitMQClient::sendRPC(char* exchange, char* routingkey, char* messagebody, char* replyQueue)
{
	/*
	set properties
	*/
	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
		AMQP_BASIC_DELIVERY_MODE_FLAG |
		AMQP_BASIC_REPLY_TO_FLAG |
		AMQP_BASIC_CORRELATION_ID_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 2; /* persistent delivery mode */
	props.reply_to = amqp_cstring_bytes(replyQueue);

	const char* guid = newGUID().c_str();
	props.correlation_id = amqp_cstring_bytes(guid);

	/*
	sendRPC
	*/
	die_on_error(amqp_basic_publish(conn,
		AMQP_CHANNEL,
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routingkey),
		0,
		0,
		&props,
		amqp_cstring_bytes(messagebody)),
		"Publishing");

	return guid;
}

void RabbitMQClient::consume(char* queue, short prefetchCount) {
	//ÉèÖÃQOS
	amqp_basic_qos(conn, AMQP_CHANNEL, 0, prefetchCount, false);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Setting Basic QOS (prefetch limit)");

	amqp_basic_consume(conn,
		AMQP_CHANNEL,
		amqp_cstring_bytes(queue),
		amqp_empty_bytes,
		0,
		0,	 /* no_ack */
		0,
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
}

Channel& RabbitMQClient::addConsumer(char* queue, short prefetchCount, const char* consumer_tag)
{
	Channel channel = this->createChannel();

	amqp_basic_qos(conn, channel.Get_ChannelNo(), 0, prefetchCount, false);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Setting Basic QoS (prefetch limit)");

	amqp_basic_consume(conn,
		channel.Get_ChannelNo(),
		amqp_cstring_bytes(queue),
		consumer_tag == NULL || strlen(consumer_tag) == 0 ? amqp_empty_bytes : amqp_cstring_bytes(consumer_tag),
		0,
		0,	 /* no_ack */
		0,
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

	return channel;
}

void RabbitMQClient::consume_message(MessageReceived callback, void* pUser)
{
	amqp_frame_t frame;
	amqp_rpc_reply_t reply;
	amqp_envelope_t envelope;

	amqp_maybe_release_buffers(conn);
	std::cout << "waiting message ... " << std::endl;
	reply = amqp_consume_message(conn, &envelope, NULL, 0);
	{
		char* tag = amqp_bytes2string_x(envelope.consumer_tag);
		std::cout << "message received from createChannel [" << envelope.channel << "], message tag:  " << tag << std::endl;
		delete tag;
	}

	if (reply.reply_type == AMQP_RESPONSE_NORMAL)
	{
		char* messageBody = new char[envelope.message.body.len + 1];
		__try
		{
			memcpy(messageBody, envelope.message.body.bytes, envelope.message.body.len);
			messageBody[envelope.message.body.len] = 0;
			printf("messageBody.len=%i messageBody.bytes=%s\n", envelope.message.body.len, messageBody);

			if (callback != NULL)
				callback(messageBody, envelope, envelope.message.properties, *this, pUser);
		}
		__finally
		{
			if (amqp_basic_ack(conn, envelope.channel, envelope.delivery_tag, false) > 0) {
				fprintf(stderr, "failing to send the ack to the broker\n");
			}
			delete[] messageBody;
			amqp_destroy_envelope(&envelope);
		}
	}
	else if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION
		&& reply.library_error == AMQP_STATUS_UNEXPECTED_STATE)
	{
		if (AMQP_STATUS_OK != amqp_simple_wait_frame(conn, &frame))
		{
			return;
		}

		if (AMQP_FRAME_METHOD == frame.frame_type)
		{
			switch (frame.payload.method.id)
			{
			case AMQP_BASIC_ACK_METHOD:
				/* if we've turned publisher confirms on, and we've published a message
				* here is a message being confirmed
				*/
				printf("AMQP_BASIC_ACK_METHOD");
				break;

			case AMQP_BASIC_RETURN_METHOD:
				/* if a published message couldn't be routed and the mandatory flag was set
				* this is what would be returned. The message then needs to be read.
				*/
			{
				amqp_message_t message;
				reply = amqp_read_message(conn, frame.channel, &message, 0);
				if (AMQP_RESPONSE_NORMAL != reply.reply_type)
				{
					return;
				}

				amqp_destroy_message(&message);
			}

			break;

			case AMQP_CHANNEL_CLOSE_METHOD:
				printf("AMQP_CHANNEL_CLOSE_METHOD");
				/* a createChannel.close method happens when a createChannel exception occurs, this
				* can happen by publishing to an exchange that doesn't exist for example
				*
				* In this case you would need to open another createChannel redeclare any queues
				* that were declared auto-delete, and restart any consumers that were attached
				* to the previous createChannel
				*/
				return;

			case AMQP_CONNECTION_CLOSE_METHOD:
				printf("AMQP_CONNECTION_CLOSE_METHOD");
				/* a connection.close method happens when a connection exception occurs,
				* this can happen by trying to use a createChannel that isn't open for example.
				*
				* In this case the whole connection must be restarted.
				*/
				return;

			default:
				fprintf(stderr, "An unexpected method was received %u\n", frame.payload.method.id);
				return;
			}
		}
	}
}

void RabbitMQClient::close()
{
	if (!closed)
	{
		die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
		die_on_error(amqp_destroy_connection(conn), "Ending connection");
		closed = true;
	}
}

amqp_connection_state_t& RabbitMQClient::Get_Connection()
{
	return this->conn;
}
