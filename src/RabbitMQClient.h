#pragma once

#include "Corlib.h"

#define AMQP_CHANNEL 1

class RabbitMQClient;
class Channel;

typedef void(*MessageReceived)(char* message, amqp_envelope_t& envelope, amqp_basic_properties_t& properties, RabbitMQClient& client, void* pUser);

class RabbitMQClient
{
public:
	RabbitMQClient();
	~RabbitMQClient();

private:
	bool closed = false;
	amqp_socket_t* socket = NULL;
	amqp_connection_state_t conn;
	int channel_no;

public:
	bool connect(const char* hostname, int port, char* user, char* pwd, int heartbeat = 30);
	Channel& createChannel();

	// begin of Deprecated
	RabbitMQClient& exchange(char* exchange, char* exchangetype);
	RabbitMQClient& queue(char* queue);
	RabbitMQClient& bind(char* queue, char* exchange, char* bindingkey);

	void send(char* exchange, char* routingkey, char* messagebody);
	const char* sendRPC(char* exchange, char* routingkey, char* messagebody, char* replyQueue);
	void send_amqp_bytes(amqp_bytes_t exchange, amqp_bytes_t routingkey, amqp_bytes_t messagebody);
	void send_bytes(char* exchange, char* routingkey, void *messagebody, const int len);
	void consume(char* queue, short prefetchCount = 1000);
	// end of Deprecated

	Channel& addConsumer(char* queue, short prefetchCount = 100, const char* consumer_tag = "");
	void consume_message(MessageReceived callback, void* pUser);
	//
	void close();

public:
	amqp_connection_state_t& Get_Connection();
};

inline string newGUID()
{
	GUID guid;
	CoCreateGuid(&guid);
	char buf[64] = { 0 };
	sprintf_s(buf, sizeof(buf), "%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
		guid.Data1, guid.Data2, guid.Data3,
		guid.Data4[0], guid.Data4[1],
		guid.Data4[2], guid.Data4[3],
		guid.Data4[4], guid.Data4[5],
		guid.Data4[6], guid.Data4[7]);
	return string(buf);
}

inline char* amqp_bytes2string_x(amqp_bytes_t data)
{
	char* messageBody = new char[data.len + 1];
	memcpy(messageBody, data.bytes, data.len);
	messageBody[data.len] = 0;
	return  messageBody;
}

inline char* const_to_char(const char* data)
{
	char* pc = new char[strlen(data) + 1];
	strcpy(pc, data);
	return pc;
}

inline void die(const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fprintf(stderr, "\n");
}

inline void die_on_error(int x, char const* context)
{
	if (x < 0)
	{
		fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x));
		//			exit(1);
	}
}

inline void die_on_amqp_error(amqp_rpc_reply_t x, char const* context)
{
	switch (x.reply_type)
	{
	case AMQP_RESPONSE_NORMAL:
		return;

	case AMQP_RESPONSE_NONE:
		fprintf(stderr, "%s: missing RPC reply type!\n", context);
		break;

	case AMQP_RESPONSE_LIBRARY_EXCEPTION:
		fprintf(stderr, "%s: %s\n", context, amqp_error_string2(x.library_error));
		break;

	case AMQP_RESPONSE_SERVER_EXCEPTION:
		switch (x.reply.id)
		{
		case AMQP_CONNECTION_CLOSE_METHOD:
		{
			amqp_connection_close_t* m = static_cast<amqp_connection_close_t *>(x.reply.decoded);
			fprintf(stderr, "%s: server connection error %uh, message: %.*s\n",
				context,
				m->reply_code,
				static_cast<int>(m->reply_text.len), static_cast<char *>(m->reply_text.bytes));
			break;
		}
		case AMQP_CHANNEL_CLOSE_METHOD:
		{
			amqp_channel_close_t* m = static_cast<amqp_channel_close_t *>(x.reply.decoded);
			fprintf(stderr, "%s: server createChannel error %uh, message: %.*s\n",
				context,
				m->reply_code,
				static_cast<int>(m->reply_text.len), static_cast<char *>(m->reply_text.bytes));
			break;
		}
		default:
			fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
			break;
		}
		break;
	}

	//	exit(1);
}
