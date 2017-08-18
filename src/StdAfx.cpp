#include "stdafx.h"

void callBackOnMessageReceived(char* message, amqp_envelope_t& envelope, amqp_basic_properties_t& properties, RabbitMQClient& client, void* pUser);
DWORD WINAPI ConsumeMessageFunc(LPVOID pM);

int main() {

	try
	{
		RabbitMQClient client;
		auto ok = client.connect("10.0.0.176", 5672, "zgy", "zgy", 30);
		if (!ok) return 0;

		Channel channel = client.createChannel();
		channel
			.exchange("exchange_sample1", "direct")
			.queue("queue_sample1")
			.queue("queue_sample1_reply")
			.bind("queue_sample1", "exchange_sample1", "routingkey_sample1");

		channel
			.exchange("exchange_sample2", "direct")
			.queue("queue_sample2")
			.queue("queue_sample2_reply")
			.bind("queue_sample2", "exchange_sample2", "routingkey_sample2");

		// add consumer
		Channel channel1 = client.addConsumer("queue_sample1", 100, "queue_sample1_tag");
		Channel channel2 = client.addConsumer("queue_sample2", 100, "queue_sample2_tag");
		CreateThread(NULL, 0, ConsumeMessageFunc, &client, 0, NULL);

		for (;;)
		{
			cout << "ÇëÑ¡Ôñ²Ù×÷£º";
			switch (getchar())
			{
			case 's':
				client.send("exchange_sample2", "routingkey_sample2", "this is a test string.");

			case 'c':
				break;

			case 'x':
				channel.close();
				channel1.close();
				channel2.close();
				client.close();
				return 0;

			default:
				break;
			}
		}
	}
	catch (exception& e)
	{
		cout << e.what() << endl;
	}
}


DWORD WINAPI ConsumeMessageFunc(LPVOID pM)
{
	RabbitMQClient *client = (RabbitMQClient*)pM;
	for (;;)
	{
		client->consume_message(callBackOnMessageReceived, NULL);
		Sleep(50);
	}
}

void callBackOnMessageReceived(char* message, amqp_envelope_t& envelope, amqp_basic_properties_t& properties, RabbitMQClient& client, void* pUser)
{
	cout << message;
}
