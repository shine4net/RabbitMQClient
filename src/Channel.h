#pragma once

#include "Corlib.h"

//
class RabbitMQClient;

class Channel
{
public:
	Channel();
	Channel(RabbitMQClient* client, int channel_no);
	~Channel();

private:
	bool closed = false;
	int channel_no;
	amqp_connection_state_t conn;

public:
	Channel& exchange(char* exchange, char* exchangetype);
	Channel& queue(char* queue);
	Channel& bind(char* queue, char* exchange, char* bindingkey);

	void send(char* exchange, char* routingkey, char* messagebody);
	const char* sendRPC(char* exchange, char* routingkey, char* messagebody, char* replyQueue);
	void send_bytes(char* exchange, char* routingkey, void *messagebody, const int len);
	void send_amqp_bytes(amqp_bytes_t exchange, amqp_bytes_t routingkey, amqp_bytes_t messagebody);

	void close();

public:
	int Get_ChannelNo();
};

