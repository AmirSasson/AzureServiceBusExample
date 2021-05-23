# Azure Service Bus Example
Simple Topic/subscription pub/sub example

## prerequiestes
- [dotnet core3.1](https://dotnet.microsoft.com/download) or higher
- azure subscription (get a free one [here](https://azure.microsoft.com/en-us/free/))
- create an [azure service bus resource](https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-messaging-overview)
- create a topic + subscription, [more info](https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quickstart-topics-subscriptions-portal)
## Setup
- create a file `appsettings.overrides.json` next to file `appsettings.json` ,and set servicebus connection string, topic and susbscription names

## run 
```
dotnet run
```

