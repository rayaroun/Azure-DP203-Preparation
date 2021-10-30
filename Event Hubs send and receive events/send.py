'''


Prerequisites 


Microsoft Azure subscription. To use Azure services, including Azure Event Hubs, you need a subscription. If you don't have an existing Azure account, you can sign up for a free trial or use your MSDN subscriber benefits when you create an account.

Python 2.7 or 3.6 or later, with PIP installed and updated.

The Python package for Event Hubs - 

To install the package, run this command in a command prompt that has Python in its path:
pip install azure-eventhub


Install the following package for receiving the events by using Azure Blob storage as the checkpoint store:
pip install azure-eventhub-checkpointstoreblob-aio



'''



import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://namespace203.servicebus.windows.net/;SharedAccessKeyName=sendpermission;SharedAccessKey=xDIDTqa0nVW9AIPnurjBPuvur71qYj838675C5KjXYg=;EntityPath=apphub", eventhub_name="apphub")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData('First event '))
        event_data_batch.add(EventData('Second event'))
        event_data_batch.add(EventData('Third event'))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
