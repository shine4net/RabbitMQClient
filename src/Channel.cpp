#include "Channel.h"

Channel::Channel()
{
}

Channel::Channel(RabbitMQClient* client, int channel_no)
{
	this->conn = client->Get_Connection();
	this->channel_no = channel_no;
}

Channel::~Channel()
{
}

Channel& Channel::exchange(char* exchange, char* exchangetype)
{
	amqp_exchange_declare(conn, channel_no, amqp_cstring_bytes(exchange), amqp_cstring_bytes(exchangetype),
		0, /*passive*/
		1, /*durable*/
		0, /*auto_delete*/
		0,
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
	return *this;
}

Channel& Channel::queue(char* queue)
{
	amqp_queue_declare(conn, channel_no, amqp_cstring_bytes(queue),
		0, /*passive*/
		1, /*durable*/
		0, /*auto_delete*/
		0,
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
	return *this;
}

Channel& Channel::bind(char* queue, char* exchange, char* bindingkey)
{
	amqp_queue_bind(conn, channel_no,
		amqp_cstring_bytes(queue),
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(bindingkey),
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Unbinding");
	return *this;
}

void Channel::send(char* exchange, char* routingkey, char* messagebody)
{
	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 2; /* persistent delivery mode */
	die_on_error(amqp_basic_publish(conn,
		channel_no,
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routingkey),
		0,
		0,
		&props,
		amqp_cstring_bytes(messagebody)),
		"Publishing");
}

const char* Channel::sendRPC(char* exchange, char* routingkey, char* messagebody, char* replyQueue)
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
		channel_no,
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routingkey),
		0,
		0,
		&props,
		amqp_cstring_bytes(messagebody)),
		"Publishing");

	return guid;
}

void Channel::send_bytes(char* exchange, char* routingkey, void* messagebody, const int len)
{
	amqp_bytes_t message_bytes;
	message_bytes.bytes = messagebody;
	message_bytes.len = len;
	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("application/octet-stream");
	props.delivery_mode = 2; /* persistent delivery mode */
	die_on_error(amqp_basic_publish(conn,
		channel_no,
		amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routingkey),
		0,
		0,
		&props,
		message_bytes),
		"Publishing");
}

void Channel::send_amqp_bytes(amqp_bytes_t exchange, amqp_bytes_t routingkey, amqp_bytes_t messagebody)
{
	amqp_basic_properties_t props;
	props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	props.content_type = amqp_cstring_bytes("text/plain");
	props.delivery_mode = 2; /* persistent delivery mode */
	die_on_error(amqp_basic_publish(conn,
		channel_no,
		exchange,
		routingkey,
		0,
		0,
		&props,
		messagebody),
		"Publishing");
}

void Channel::close()
{
	if (!this->closed)
	{
		die_on_amqp_error(amqp_channel_close(conn, channel_no, AMQP_REPLY_SUCCESS), "Closing channel");
		this->closed = true;
	}
}

int Channel::Get_ChannelNo()
{
	return this->channel_no;
}
