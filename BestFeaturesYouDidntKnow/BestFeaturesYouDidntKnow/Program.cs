using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BestFeaturesYouDidntKnow
{
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    public class Item
    {
        public string ItemNumber { get; set; }
        public float UnitCost { get; set; }
        public int Quantity { get; set; }
    }

    public class Order
    {
        public Order()
        {
            id = Guid.NewGuid();
        }
        private Guid id;
        public String Id { get { return id.ToString(); } }
        public String CustomerName { get; set; }
        public DateTime Date { get; set; }
        public List<Item> Items { get; set; }
    }

    class Program
    {
        static NamespaceManager nsm = NamespaceManager.Create();
        static Order CreateOrder()
        {
            var order = new Order()
            {
                CustomerName = "Bob",
                Date = DateTime.Today,
                Items = new List<Item>(){
                    new Item() { ItemNumber = "1234", Quantity = 3, UnitCost = 100 }}
            };
            return order;
        }
        static void Main(string[] args)
        {
            //BE SURE TO PUT YOUR SERVICE BUS CONNECTION STRING INTO app.config
            //PLEASE USE A NAMESPACE LEVEL CONNECTION STRING WITH MANAGE
            //ALSO - you should NEVER use Send() and Receive() -PLEASE ALWAYS- use Send/ReceiveAsync
            //

            var order = CreateOrder();
            DuplicateQueue();
            //SendOrderLater(order, DateTime.UtcNow.AddSeconds(30));
            //Defer(order);
            //AutoForward(order);
            //ActionsOnSubscriptions(order);
            //DeadletterForward(order);
            //OnMessageSample();
            //OnMessageSampleWOptions();
            Console.WriteLine("Samples complete, press enter to exit");
            Console.ReadLine();
        }

        private static void DuplicateQueue()
        {
            #region setup queue
            SetupEntities(SampleScenario.Duplicate);
            #endregion

            var order = CreateOrder();
            SendOrder(order);
            System.Threading.Thread.Sleep(5000);
            SendOrder(order);
            System.Threading.Thread.Sleep(5000);
            SendOrder(order);
            //var recqc.Receive();
        }

 

        static void SendOrder(Order order)
        {
            var qc = QueueClient.Create(DUPLICATE_QUEUE);
            var msg = new BrokeredMessage(order);
            msg.MessageId = order.Id;
            qc.Send(msg);
        }

        static void SendOrderLater(Order order, DateTime whenToSend)
        {
            var qc = QueueClient.Create(DUPLICATE_QUEUE);
            var msg = new BrokeredMessage(order);
            msg.ScheduledEnqueueTimeUtc = whenToSend;
            qc.Send(msg);
        }

        static void Defer(Order order)
        {
            var deferList = new List<long>();
            var qc = QueueClient.Create(DUPLICATE_QUEUE);
            
            var orderMsg = new BrokeredMessage(order);
            orderMsg.Properties.Add("type", "place-order");
            qc.Send(orderMsg);

            var command = new BrokeredMessage();
            command.Properties.Add("type", "stop-command");
            qc.Send(command);

            var orderMsg2 = new BrokeredMessage(order);
            orderMsg2.Properties.Add("type", "place-order");
            qc.Send(orderMsg2);

            for(int i=0;i<3;i++)
            {
                var msg = qc.Receive();
                if(String.Equals(msg.Properties["type"], "stop-command"))
                {
                    deferList.Add(msg.SequenceNumber);
                    msg.Defer();
                    Console.WriteLine("Deferred Command");
                }
                else
                {
                    Console.WriteLine("Place order");
                    msg.Complete();
                }
            }
            Console.WriteLine("Press enter to revieve defered messages");
            Console.ReadLine();
            foreach(var sequenceNumber in deferList)
            {
                var cmd = qc.Receive(sequenceNumber);
                Console.WriteLine("Received command {0}", cmd.Properties["type"]);
                cmd.Complete();
            }
        }

        static void AutoForward(Order order)
        {
            SetupEntities(SampleScenario.AutoForward);
            //send message to old queue
            order.CustomerName += " On Old Queue";
            var oldqc = QueueClient.Create(FORWARD_QUEUE);
            oldqc.Send(new BrokeredMessage(order));

            //read message from forwarded queue
            var qc = QueueClient.Create(DUPLICATE_QUEUE);
            var msg = qc.Receive();
            if(msg != null)
            {
                var forwardedOrder = msg.GetBody<Order>();
                Console.WriteLine("Received Message for customer: {0}", forwardedOrder.CustomerName);
                msg.Complete();
            }
        }

        static void ActionsOnSubscriptions(Order order)
        {
            SetupEntities(SampleScenario.SubscriptionActions);
            var tc = TopicClient.Create(ACTIONS_TOPIC);
            tc.Send(new BrokeredMessage(order));

            var sc = SubscriptionClient.Create(ACTIONS_TOPIC, ACTIONS_SUBSCRIPTION);
            var msg = sc.Receive();
            Console.WriteLine("Message received with stage = {0}", msg.Properties["stage"]);
            msg.Complete();
        }

        static void DeadletterForward(Order order)
        {
            if (!nsm.QueueExists(DEADLETTER_FORWARD))
            {
                nsm.CreateQueue(new QueueDescription(DEADLETTER_FORWARD)
                {
                    ForwardDeadLetteredMessagesTo = DUPLICATE_QUEUE,
                    EnableDeadLetteringOnMessageExpiration = true,
                    
                });
            }
            var qc = QueueClient.Create(DEADLETTER_FORWARD);
            qc.Send(new BrokeredMessage(order) { TimeToLive = new TimeSpan(0, 0, 10) });

            var dlc = QueueClient.Create(DUPLICATE_QUEUE);
            var msg = dlc.Receive();
            Console.WriteLine("Received Message from {0}", DUPLICATE_QUEUE);
            msg.Complete();
        }

        static void OnMessageSample()
        {
            var order = CreateOrder();
            var qc = QueueClient.Create(DUPLICATE_QUEUE);
            qc.OnMessage((message) =>
            {
                var myOrder = message.GetBody<Order>();
                Console.WriteLine("Received Order {0}", myOrder.CustomerName);
                message.Complete();
            });

            for(int i=0;i<5;i++)
            {
                qc.Send(new BrokeredMessage(order));
            }
        }

        static void OnMessageSampleWOptions()
        {
            var order = CreateOrder();
            var options = new OnMessageOptions() { 
                MaxConcurrentCalls = 5, 
                AutoComplete = true};

            var qc = QueueClient.Create(DUPLICATE_QUEUE);
            qc.OnMessage((message) =>
            {
                var myOrder = message.GetBody<Order>();
                Console.WriteLine("Received Order {0}", myOrder.CustomerName);
            }, options);
            for (int i = 0; i < 5; i++)
            {
                qc.Send(new BrokeredMessage(order));
            }
        }

        static readonly String DUPLICATE_QUEUE = "dupequeue";
        static readonly String FORWARD_QUEUE = "oldqueue";
        static readonly String ACTIONS_TOPIC = "actionstopic";
        static readonly String ACTIONS_SUBSCRIPTION = "stage1";
        static readonly String DEADLETTER_FORWARD = "deadletterforward";

        private static void SetupEntities(SampleScenario scenario)
        {
            switch (scenario)
            {
                case SampleScenario.Duplicate:
                    if (!nsm.QueueExists(DUPLICATE_QUEUE))
                    {
                        nsm.CreateQueue(new QueueDescription(DUPLICATE_QUEUE)
                        {
                            RequiresDuplicateDetection = true,
                            DuplicateDetectionHistoryTimeWindow = new TimeSpan(0, 0, 30)
                        }
                        );
                    }
                    break;
                case SampleScenario.AutoForward:
                    string oldQueue = "oldqueue";
                    if (!nsm.QueueExists(oldQueue))
                    {
                        nsm.CreateQueue(new QueueDescription(oldQueue)
                        {
                            ForwardTo = "dupequeue"
                        }
                        );
                    }
                    break;
                case SampleScenario.SubscriptionActions:
                    if (!nsm.TopicExists(ACTIONS_TOPIC))
                    {
                        var td = nsm.CreateTopic(ACTIONS_TOPIC);

                        var sd = new SubscriptionDescription(ACTIONS_TOPIC, ACTIONS_SUBSCRIPTION);

                        var sc = SubscriptionClient.Create(ACTIONS_TOPIC, ACTIONS_SUBSCRIPTION);
                        nsm.CreateSubscription(sd);
                        var rules = nsm.GetRules(ACTIONS_TOPIC, ACTIONS_SUBSCRIPTION);
                        foreach (var rule in rules)
                            sc.RemoveRule(rule.Name);
                        
                        var rd = new RuleDescription("default", 
                            new SqlFilter("NOT EXISTS(stage)")) 
                            { Action = new SqlRuleAction("set stage = 1") };
                        sc.AddRule(rd);
                    }
                    break;

                default:
                    break;
            }

        }
        internal enum SampleScenario
        {
            Duplicate,
            AutoForward,
            SubscriptionActions,
            DeadletteringAutoForward
        }
    }
}
