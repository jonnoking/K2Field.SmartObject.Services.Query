﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Linq;

using SourceCode.SmartObjects.Services.ServiceSDK;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;

using K2Field.SmartObject.Services.Query.Interfaces;
using System.Reflection;
using System.Runtime.Serialization;
using System.Data;
using SourceCode.Hosting.Client.BaseAPI;
using SourceCode.Data.SmartObjectsClient;
using System.Diagnostics;
using System.IO;

namespace K2Field.SmartObject.Services.Query.Data
{
    /// <summary>
    /// A concrete implementation of IDataConnector responsible for interacting with an underlying system or technology. The purpose of this class it to expose and represent the underlying data and services as Service Objects for consumptions by K2 SmartObjects.
    /// </summary>
    class DataConnector : IDataConnector
    {
        #region Class Level Fields

        #region Constants
        /// <summary>
        /// Constant for the Type Mappings configuration lookup in the service instance.
        /// </summary>
        private static string __TypeMappings = "Type Mappings";

        #endregion

        #region Private Fields
        /// <summary>
        /// Local serviceBroker variable.
        /// </summary>
        private ServiceAssemblyBase serviceBroker = null;
        #endregion

        #endregion

        #region Constructor
        /// <summary>
        /// Instantiates a new DataConnector.
        /// </summary>
        /// <param name="serviceBroker">The ServiceBroker.</param>
        public DataConnector(ServiceAssemblyBase serviceBroker)
        {
            // Set local serviceBroker variable.
            this.serviceBroker = serviceBroker;
        }
        #endregion

        #region Methods

        #region void Dispose()
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            // Add any additional IDisposable implementation code here. Make sure to dispose of any data connections.
            // Clear references to serviceBroker.
            serviceBroker = null;
        }
        #endregion

        #region void GetConfiguration()
        /// <summary>
        /// Gets the configuration from the service instance and stores the retrieved configuration in local variables for later use.
        /// </summary>
        public void GetConfiguration()
        {
            // Add the service instance's configuration retrieval code here.
            //throw new NotImplementedException();
        }
        #endregion

        #region void SetupConfiguration()
        /// <summary>
        /// Sets up the required configuration parameters in the service instance. When a new service instance is registered for this ServiceBroker, the configuration parameters are surfaced to the appropriate tooling. The configuration parameters are provided by the person registering the service instance.
        /// </summary>
        public void SetupConfiguration()
        {
            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.ConnectionString, true, "");

            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.Namespace, false, "");
            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.Issuer, false, "");
            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.Key, false, "");
            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.DynamicDiscovery, true, "false");
            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.Delimiter1, true, "|");
            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.Delimiter2, true, "#");
            //serviceBroker.Service.ServiceConfiguration.Add(ServiceConfigurationSettings.RequestTimeout, true, "60");

            serviceBroker.Service.ServiceConfiguration.Add("Query", true, "select * from SmartObject_Blah");
            serviceBroker.Service.ServiceConfiguration.Add("NewSmartObjectName", true, "");
        }
        #endregion

        #region void SetupService()
        /// <summary>
        /// Sets up the service instance's default name, display name, and description.
        /// </summary>
        public void SetupService()
        {
            serviceBroker.Service.Name = "QuerySmartObject" + _newsmartobjectname.Replace(" ", "_");
            serviceBroker.Service.MetaData.DisplayName = "Query SmartObject - " + _newsmartobjectname;
            serviceBroker.Service.MetaData.Description = "Query SmartObject - " + _newsmartobjectname;
        }
        #endregion


        #region Utility Methods

        private DataTable QuerySmartObject(string adoquery)
        {
            DataTable results = new DataTable();

            string sql_query = adoquery;

            //SourceCode.SmartObjects.Client.SmartObjectClientServer server = new SmartObjectClientServer();
            SCConnectionStringBuilder SmOBuilder = new SCConnectionStringBuilder();
            SmOBuilder.Host = "localhost";
            SmOBuilder.Port = 5555;
            SmOBuilder.Integrated = true;
            SmOBuilder.IsPrimaryLogin = true;

            using (SOConnection connection = new SOConnection(SmOBuilder.ToString()))
            using (SOCommand command = new SOCommand(sql_query, connection))
            using (SODataAdapter adapter = new SODataAdapter(command))
            {
                connection.DirectExecution = true;
                adapter.Fill(results);
            }

            return results;
        }


        #region Property Definition
        
        
        /*
        private List<Property> GetMessagePropertiesReturn()
        {
            List<Property> MessageProperties = new List<Property>();
            //            BrokeredMessage Message = new BrokeredMessage("test");

            Property p0 = new Property();
            p0.Name = "path";
            p0.MetaData.DisplayName = "Path";
            p0.SoType = SoType.Text;
            p0.Type = "string";
            MessageProperties.Add(p0);

            //Property p01 = new Property();
            //p01.Name = "requesttimeout";
            //p01.MetaData.DisplayName = "Request Timeout";
            //p01.Value = 60;
            //p01.SoType = SoType.Number;
            //MessageProperties.Add(p01);

            Property p1 = new Property();
            p1.Name = "contenttype";
            p1.MetaData.DisplayName = "Content Type";
            p1.SoType = SoType.Text;
            //p1.Type = Message.ContentType.GetType().ToString();
            MessageProperties.Add(p1);

            Property p2 = new Property();
            p2.Name = "correlationid";
            p2.MetaData.DisplayName = "Correlation Id";
            p2.SoType = SoType.Text;
            //p2.Type = Message.CorrelationId.GetType().ToString();
            MessageProperties.Add(p2);

            Property p3 = new Property();
            p3.Name = "deliverycount";
            p3.MetaData.DisplayName = "Delivery Count";
            p3.SoType = SoType.Number;
            //p3.Type = Message.DeliveryCount.GetType().ToString();
            MessageProperties.Add(p3);

            Property p4 = new Property();
            p4.Name = "enqueuedtimeutc";
            p4.MetaData.DisplayName = "Enqueued Time Utc";
            p4.SoType = SoType.DateTime;
            //p4.Type = Message.EnqueuedTimeUtc.GetType().ToString();
            MessageProperties.Add(p4);

            Property p5 = new Property();
            p5.Name = "expiresatutc";
            p5.MetaData.DisplayName = "Expires At Utc";
            p5.SoType = SoType.Decimal;
            //p5.Type = Message.ExpiresAtUtc.GetType().ToString();
            MessageProperties.Add(p5);

            Property p6 = new Property();
            p6.Name = "label";
            p6.MetaData.DisplayName = "Label";
            p6.SoType = SoType.Text;
            //p6.Type = Message.Label.GetType().ToString();
            MessageProperties.Add(p6);

            Property p7 = new Property();
            p7.Name = "messageid";
            p7.MetaData.DisplayName = "Message Id";
            p7.SoType = SoType.Text;
            //p7.Type = Message.MessageId.GetType().ToString();
            MessageProperties.Add(p7);

            Property p8 = new Property();
            p8.Name = "properties";
            p8.MetaData.DisplayName = "Properties";
            p8.SoType = SoType.Text;
            //p8.Type = "IDictionary<string, object>";
            MessageProperties.Add(p8);

            Property p9 = new Property();
            p9.Name = "replyto";
            p9.MetaData.DisplayName = "Reply To";
            p9.SoType = SoType.Text;
            //p9.Type = Message.ReplyTo.GetType().ToString();
            MessageProperties.Add(p9);

            Property p10 = new Property();
            p10.Name = "replytosessionid";
            p10.MetaData.DisplayName = "Reply To Session Id";
            p10.SoType = SoType.Text;
            //p10.Type = Message.ReplyToSessionId.GetType().ToString();
            MessageProperties.Add(p10);

            Property p11 = new Property();
            p11.Name = "scheduledenqueuetimeutc";
            p11.MetaData.DisplayName = "Scheduled Enqueue Time Utc";
            p11.SoType = SoType.Decimal;
            //p11.Type = Message.ScheduledEnqueueTimeUtc.GetType().ToString();
            MessageProperties.Add(p11);

            Property p12 = new Property();
            p12.Name = "sequencenumber";
            p12.MetaData.DisplayName = "Sequence Number";
            p12.SoType = SoType.Number;
            //p12.Type = Message.SequenceNumber.GetType().ToString();
            MessageProperties.Add(p12);

            Property p13 = new Property();
            p13.Name = "sessionid";
            p13.MetaData.DisplayName = "Session Id";
            p13.SoType = SoType.Text;
            //p13.Type = Message.SessionId.GetType().ToString();
            MessageProperties.Add(p13);

            Property p14 = new Property();
            p14.Name = "size";
            p14.MetaData.DisplayName = "Size";
            p14.SoType = SoType.Number;
            //p14.Type = Message.Size.GetType().ToString();
            MessageProperties.Add(p14);

            Property p15 = new Property();
            p15.Name = "timetolive";
            p15.MetaData.DisplayName = "Time To Live";
            p15.SoType = SoType.Decimal;
            //p15.Type = Message.TimeToLive.GetType().ToString();
            MessageProperties.Add(p15);

            Property p16 = new Property();
            p16.Name = "to";
            p16.MetaData.DisplayName = "To";
            p16.SoType = SoType.Text;
            //p16.Type = Message.To.GetType().ToString();
            MessageProperties.Add(p16);

            Property p17 = new Property();
            p17.Name = "body";
            p17.MetaData.DisplayName = "Body";
            p17.SoType = SoType.Text;
            //p17.Type = "string";
            MessageProperties.Add(p17);

            Property p18 = new Property();
            p18.Name = "locktoken";
            p18.MetaData.DisplayName = "Lock Token";
            p18.SoType = SoType.Guid;
            //p18.Type = "string";
            MessageProperties.Add(p18);

            MessageProperties.AddRange(GetStandardReturnProperties());

            return MessageProperties;
        }

        */
        /*
        private List<Property> GetMessagePropertiesInput()
        {
            List<Property> MessageProperties = new List<Property>();
//            BrokeredMessage Message = new BrokeredMessage("test");

            Property p0 = new Property();
            p0.Name = "path";
            p0.MetaData.DisplayName = "Path";
            p0.SoType = SoType.Text;
            p0.Type = "string";
            MessageProperties.Add(p0);

            MessageProperties.AddRange(GetStandardInputProperties());

            Property p1 = new Property();
            p1.Name = "contenttype";
            p1.MetaData.DisplayName = "Content Type";
            p1.SoType = SoType.Text;
            //p1.Type = Message.ContentType.GetType().ToString();
            MessageProperties.Add(p1);

            Property p2 = new Property();
            p2.Name = "correlationid";
            p2.MetaData.DisplayName = "Correlation Id";
            p2.SoType = SoType.Text;
            //p2.Type = Message.CorrelationId.GetType().ToString();
            MessageProperties.Add(p2);

            Property p6 = new Property();
            p6.Name = "label";
            p6.MetaData.DisplayName = "Label";
            p6.SoType = SoType.Text;
            //p6.Type = Message.Label.GetType().ToString();
            MessageProperties.Add(p6);

            Property p7 = new Property();
            p7.Name = "messageid";
            p7.MetaData.DisplayName = "Message Id";
            p7.SoType = SoType.Text;
            //p7.Type = Message.MessageId.GetType().ToString();
            MessageProperties.Add(p7);

            Property p8 = new Property();
            p8.Name = "properties";
            p8.MetaData.DisplayName = "Properties";
            p8.SoType = SoType.Text;
            //p8.Type = "IDictionary<string, object>";
            MessageProperties.Add(p8);

            Property p9 = new Property();
            p9.Name = "replyto";
            p9.MetaData.DisplayName = "Reply To";
            p9.SoType = SoType.Text;
            //p9.Type = Message.ReplyTo.GetType().ToString();
            MessageProperties.Add(p9);

            Property p10 = new Property();
            p10.Name = "replytosessionid";
            p10.MetaData.DisplayName = "Reply To Session Id";
            p10.SoType = SoType.Text;
            //p10.Type = Message.ReplyToSessionId.GetType().ToString();
            MessageProperties.Add(p10);

            Property p13 = new Property();
            p13.Name = "sessionid";
            p13.MetaData.DisplayName = "Session Id";
            p13.SoType = SoType.Text;
            //p13.Type = Message.SessionId.GetType().ToString();
            MessageProperties.Add(p13);

            Property p15 = new Property();
            p15.Name = "timetolive";
            p15.MetaData.DisplayName = "Time To Live";
            p15.SoType = SoType.Number;
            //p15.Type = Message.TimeToLive.GetType().ToString();
            MessageProperties.Add(p15);

            Property p16 = new Property();
            p16.Name = "to";
            p16.MetaData.DisplayName = "To";
            p16.SoType = SoType.Text;
            //p16.Type = Message.To.GetType().ToString();
            MessageProperties.Add(p16);

            Property p17 = new Property();
            p17.Name = "body";
            p17.MetaData.DisplayName = "Body";
            p17.SoType = SoType.Text;
            //p17.Type = "string";
            MessageProperties.Add(p17);

            return MessageProperties;
        }
        */
        /*
        private List<Property> GetTopicProperties()
        {
            List<Property> TopicProperties = new List<Property>();

            Property pDMTTL = new Property();
            pDMTTL.Name = "defaultmessagetimetolive";
            pDMTTL.MetaData.DisplayName = "Default Message Time To Live";
            pDMTTL.SoType = SoType.Decimal;
            TopicProperties.Add(pDMTTL);

            Property pDDHTW = new Property();
            pDDHTW.Name = "duplicatedetectionhistorytimewindow";
            pDDHTW.MetaData.DisplayName = "Duplicate Detection History Time Window";
            pDDHTW.SoType = SoType.Decimal;
            TopicProperties.Add(pDDHTW);

            Property pEBO = new Property();
            pEBO.Name = "enablebatchedoperations";
            pEBO.MetaData.DisplayName = "Enable Batched Operations";
            pEBO.SoType = SoType.YesNo;
            TopicProperties.Add(pEBO);

            Property pIRO = new Property();
            pIRO.Name = "isreadonly";
            pIRO.MetaData.DisplayName = "Is Read Only";
            pIRO.SoType = SoType.YesNo;
            TopicProperties.Add(pIRO);

            Property pMM = new Property();
            pMM.Name = "maxsizeinmegabytes";
            pMM.MetaData.DisplayName = "Max Size In Megabytes";
            pMM.SoType = SoType.Number;
            TopicProperties.Add(pMM);

            Property pP = new Property();
            pP.Name = "topic";
            pP.MetaData.DisplayName = "Topic";
            pP.SoType = SoType.Text;
            TopicProperties.Add(pP);

            Property pRDD = new Property();
            pRDD.Name = "requiresduplicatedetection";
            pRDD.MetaData.DisplayName = "Requires Duplicate Detection";
            pRDD.SoType = SoType.YesNo;
            TopicProperties.Add(pRDD);

            Property pSIB = new Property();
            pSIB.Name = "sizeinbytes";
            pSIB.MetaData.DisplayName = "Size In Bytes";
            pSIB.SoType = SoType.Number;
            TopicProperties.Add(pSIB);

            TopicProperties.AddRange(GetStandardReturnProperties());

            return TopicProperties;
        }
        */
        /*
        private List<Property> GetSubscriptionProperties()
        {
            List<Property> SubscriptionProperties = new List<Property>();

            Property pDMTTL = new Property();
            pDMTTL.Name = "defaultmessagetimetolive";
            pDMTTL.MetaData.DisplayName = "Default Message Time To Live";
            pDMTTL.SoType = SoType.Decimal;
            SubscriptionProperties.Add(pDMTTL);

            Property pEBO = new Property();
            pEBO.Name = "enablebatchedoperations";
            pEBO.MetaData.DisplayName = "Enable Batched Operations";
            pEBO.SoType = SoType.YesNo;
            //pEBO.Type = Queue.EnableBatchedOperations.GetType().ToString();
            SubscriptionProperties.Add(pEBO);

            Property pEDLFE = new Property();
            pEDLFE.Name = "enabledeadletteringonfilterevaluationexceptions";
            pEDLFE.MetaData.DisplayName = "Enable Dead Lettering On Filter Evaluation Exceptions";
            pEDLFE.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pEDLFE);

            Property pEDL = new Property();
            pEDL.Name = "enabledeadletteringonmessageexpiration";
            pEDL.MetaData.DisplayName = "Enable Dead Lettering On Message Expiration";
            pEDL.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pEDL);

            Property pIRO = new Property();
            pIRO.Name = "isreadonly";
            pIRO.MetaData.DisplayName = "Is Read Only";
            pIRO.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pIRO);

            Property pLD = new Property();
            pLD.Name = "lockduration";
            pLD.MetaData.DisplayName = "Lock Duration";
            pLD.SoType = SoType.Decimal;
            SubscriptionProperties.Add(pLD);

            Property pMDC = new Property();
            pMDC.Name = "maxdeliverycount";
            pMDC.MetaData.DisplayName = "Max Delivery Count";
            pMDC.SoType = SoType.Number;
            SubscriptionProperties.Add(pMDC);

            Property pMC = new Property();
            pMC.Name = "messagecount";
            pMC.MetaData.DisplayName = "Message Count";
            pMC.SoType = SoType.Number;
            SubscriptionProperties.Add(pMC);

            Property pS = new Property();
            pS.Name = "subscription";
            pS.MetaData.DisplayName = "Subscription";
            pS.SoType = SoType.Text;
            SubscriptionProperties.Add(pS);

            Property pRS = new Property();
            pRS.Name = "requiressession";
            pRS.MetaData.DisplayName = "Requires Session";
            pRS.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pRS);

            Property pP = new Property();
            pP.Name = "topic";
            pP.MetaData.DisplayName = "Topic";
            pP.SoType = SoType.Text;
            SubscriptionProperties.Add(pP);

            SubscriptionProperties.AddRange(GetStandardReturnProperties());

            return SubscriptionProperties;
        }
        */
        /*
        private List<Property> GetQueueProperties()
        {
            List<Property> QueueProperties = new List<Property>();
            QueueDescription Queue = new QueueDescription("template");

            Property pDMTTL = new Property();
            pDMTTL.Name = "defaultmessagetimetolive";
            pDMTTL.MetaData.DisplayName = "Default Message Time To Live";
            pDMTTL.SoType = SoType.Decimal;
            //pDMTTL.Type = Queue.DefaultMessageTimeToLive.GetType().ToString();
            QueueProperties.Add(pDMTTL);

            Property pDDHTW = new Property();
            pDDHTW.Name = "duplicatedetectionhistorytimewindow";
            pDDHTW.MetaData.DisplayName = "Duplicate Detection History Time Window";
            pDDHTW.SoType = SoType.Decimal;
            //pDDHTW.Type = Queue.DuplicateDetectionHistoryTimeWindow.GetType().ToString();
            QueueProperties.Add(pDDHTW);

            Property pEBO = new Property();
            pEBO.Name = "enablebatchedoperations";
            pEBO.MetaData.DisplayName = "Enable Batched Operations";
            pEBO.SoType = SoType.YesNo;
            //pEBO.Type = Queue.EnableBatchedOperations.GetType().ToString();
            QueueProperties.Add(pEBO);

            Property pEDL = new Property();
            pEDL.Name = "enabledeadletteringonmessageexpiration";
            pEDL.MetaData.DisplayName = "Enable Dead Lettering On Message Expiration";
            pEDL.SoType = SoType.YesNo;
            //pEDL.Type = Queue.EnableDeadLetteringOnMessageExpiration.GetType().ToString();
            QueueProperties.Add(pEDL);

            Property pIRO = new Property();
            pIRO.Name = "isreadonly";
            pIRO.MetaData.DisplayName = "Is Read Only";
            pIRO.SoType = SoType.YesNo;
            QueueProperties.Add(pIRO);

            Property pLD = new Property();
            pLD.Name = "lockduration";
            pLD.MetaData.DisplayName = "Lock Duration";
            pLD.SoType = SoType.Decimal;
            //pLD.Type = Queue.LockDuration.GetType().ToString();
            QueueProperties.Add(pLD);

            Property pMDC = new Property();
            pMDC.Name = "maxdeliverycount";
            pMDC.MetaData.DisplayName = "Max Delivery Count";
            pMDC.SoType = SoType.Number;
            //pMDC.Type = Queue.MaxDeliveryCount.GetType().ToString();
            QueueProperties.Add(pMDC);

            Property pMM = new Property();
            pMM.Name = "maxsizeinmegabytes";
            pMM.MetaData.DisplayName = "Max Size In Megabytes";
            pMM.SoType = SoType.Number;
            //pMM.Type = Queue.MaxSizeInMegabytes.GetType().ToString();
            QueueProperties.Add(pMM);

            Property pMC = new Property();
            pMC.Name = "messagecount";
            pMC.MetaData.DisplayName = "Message Count";
            pMC.SoType = SoType.Number;
            //pMC.Type = Queue.MessageCount.GetType().ToString();
            QueueProperties.Add(pMC);

            Property pP = new Property();
            pP.Name = "queue";
            pP.MetaData.DisplayName = "Queue";
            pP.SoType = SoType.Text;
            //pP.Type = Queue.Path.GetType().ToString();
            QueueProperties.Add(pP);

            Property pRDD = new Property();
            pRDD.Name = "requiresduplicatedetection";
            pRDD.MetaData.DisplayName = "Requires Duplicate Detection";
            pRDD.SoType = SoType.YesNo;
            //pRDD.Type = Queue.RequiresDuplicateDetection.GetType().ToString();
            QueueProperties.Add(pRDD);

            Property pRS = new Property();
            pRS.Name = "requiressession";
            pRS.MetaData.DisplayName = "Requires Session";
            pRS.SoType = SoType.YesNo;
            //pRS.Type = Queue.RequiresSession.GetType().ToString();
            QueueProperties.Add(pRS);

            Property pSIB = new Property();
            pSIB.Name = "sizeinbytes";
            pSIB.MetaData.DisplayName = "Size In Bytes";
            pSIB.SoType = SoType.Number;
            //pSIB.Type = Queue.SizeInBytes.GetType().ToString();
            QueueProperties.Add(pSIB);

            QueueProperties.AddRange(GetStandardReturnProperties());

            return QueueProperties;
        }
        */
        #endregion Property Definition

        #region Method Definition
        

        


        
        
        /*
        private Method CreateSendMessage(List<Property> MessageProps)
        {
            Method SendMessage = new Method();
            SendMessage.Name = "sendmessage";
            SendMessage.MetaData.DisplayName = "Send Message";
            SendMessage.Type = MethodType.Read;
            foreach (Property prop in GetMessagePropertiesInput())
            {
                SendMessage.InputProperties.Add(prop);
            }
            SendMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());

            foreach (Property prop in MessageProps)
            {
                SendMessage.ReturnProperties.Add(prop);
            }


            return SendMessage;
        }

        private Method CreateReceiveMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "receivemessage";
            ReceiveMessage.MetaData.DisplayName = "Receive Message";
            ReceiveMessage.Type = MethodType.Read;

            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());

            foreach (Property prop in GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }

            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }            
            
            return ReceiveMessage;
        }

        private Method CreatePeekLockMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "peeklockmessage";
            ReceiveMessage.MetaData.DisplayName = "Peek Lock Message";
            ReceiveMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }
            // remove body from peek lock
            ReceiveMessage.ReturnProperties.Remove(MessageProps.Where(p => p.Name == "body").First());

            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());            
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());

            foreach (Property prop in GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }
            
            return ReceiveMessage;
        }

        private Method CreateReceivePeekLockMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "receivepeeklockmessage";
            ReceiveMessage.MetaData.DisplayName = "Recieve Peek Lock Message";
            ReceiveMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }
            
            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());            

            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());

            foreach (Property prop in GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }

            return ReceiveMessage;
        }

        private Method CreateSessionReceiveMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "receivesessionmessage";
            ReceiveMessage.MetaData.DisplayName = "Receive Session Message";
            ReceiveMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }
         
            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "replytosessionid").First());

            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "replytosessionid").First());

            foreach (Property prop in GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }

            return ReceiveMessage;
        }



        private Method CreateAbandonMessage(List<Property> MessageProps)
        {
            Method AbandonMessage = new Method();
            AbandonMessage.Name = "abandonmessage";
            AbandonMessage.MetaData.DisplayName = "Abandon Message";
            AbandonMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                AbandonMessage.ReturnProperties.Add(prop);
            }
            // remove body from peek lock
            AbandonMessage.ReturnProperties.Remove(MessageProps.Where(p => p.Name == "body").First());

            AbandonMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            AbandonMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            AbandonMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());


            AbandonMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            AbandonMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            AbandonMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());

            foreach (Property prop in GetStandardInputProperties())
            {
                AbandonMessage.InputProperties.Add(prop);
            }

            return AbandonMessage;
        }

        private Method CreateDeadLetterMessage(List<Property> MessageProps)
        {
            Method DeadLetterMessage = new Method();
            DeadLetterMessage.Name = "deadletternmessage";
            DeadLetterMessage.MetaData.DisplayName = "Dead Letter Message";
            DeadLetterMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                DeadLetterMessage.ReturnProperties.Add(prop);
            }
            // remove body from peek lock
            DeadLetterMessage.ReturnProperties.Remove(MessageProps.Where(p => p.Name == "body").First());

            DeadLetterMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            DeadLetterMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            DeadLetterMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());
            

            DeadLetterMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            DeadLetterMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            DeadLetterMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());

            foreach (Property prop in GetStandardInputProperties())
            {
                DeadLetterMessage.InputProperties.Add(prop);
            }

            return DeadLetterMessage;
        }
        */
        #endregion Method Definition

        #endregion Utility Methods

        SoType GetSoType(string colname, Type dataType)
        {
            Trace.WriteLine("COLUMN - " + colname + " - " + dataType.ToString());

            

            switch (dataType.ToString())
            {                    
                case "System.Byte":
                case "System.Boolean":
                    return SoType.YesNo;
                case "System.String":
                case "System.Char":
                    return SoType.Text;
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                    return SoType.Number;
                case "System.Decimal":
                case "System.Double":
                    return SoType.Decimal;
                case "System.DataTime":
                case "System.DateTimeOffset":
                    return SoType.DateTime;                    
                case "System.Guid":
                    return SoType.Guid;
                default:
                    return SoType.Memo;
            }
            
        }

        string _newsmartobjectname = string.Empty;
        string _query = string.Empty;

        private string GetQuery(string q)
        {
            if (q.StartsWith("c:", StringComparison.OrdinalIgnoreCase))
            {
                _query = ReadQuery(q);
            }
            else
            {
                _query = q;
            }

            return _query; 
        }

        private string ReadQuery(string path)
        {
            string textquery = string.Empty;
            using (StreamReader sr = new StreamReader(path))
            {
                textquery = sr.ReadToEnd();
            }            
            return textquery;
        }
        
        #region void DescribeSchema()
        /// <summary>
        /// Describes the schema of the underlying data and services to the K2 platform.
        /// </summary>
        public void DescribeSchema()
        {
            //TypeMappings map = GetTypeMappings();
            ServiceObject obj = null;
            Property property = null;
            string query = GetQuery(serviceBroker.Service.ServiceConfiguration["Query"].ToString());
            string newsmartobjectname = serviceBroker.Service.ServiceConfiguration["NewSmartObjectName"].ToString();
            _newsmartobjectname = newsmartobjectname;

            DataTable dt = QuerySmartObject(query);

            if (dt.Rows.Count == 0)
            {
                throw new Exception("Query must return at least one row");
            }

            ServiceObject QueryObject = new ServiceObject();
            QueryObject.Name = newsmartobjectname.Replace(" ", "_");
            QueryObject.MetaData.DisplayName = newsmartobjectname;
            QueryObject.MetaData.ServiceProperties.Add("query", query);
            QueryObject.Active = true;


            Property smoColQuery = new Property();
            smoColQuery.Name = "queryobjectquery";
            smoColQuery.MetaData.DisplayName = "Query";
            smoColQuery.SoType = SoType.Memo;            
            QueryObject.Properties.Create(smoColQuery);

            Property smoColRowCount = new Property();
            smoColRowCount.Name = "queryobjectrowcount";
            smoColRowCount.MetaData.DisplayName = "Row Count";           
            smoColRowCount.SoType = SoType.Number;
            QueryObject.Properties.Create(smoColRowCount);
            

            foreach (DataColumn col in dt.Columns)
            {
                Trace.WriteLine("ColName: " + col.ColumnName);

                Property smoCol = new Property();
                smoCol.Name = col.ColumnName.ToLower().Replace(" ", "_");
                smoCol.MetaData.DisplayName = col.ColumnName;
                smoCol.SoType = GetSoType(col.ColumnName, col.DataType);
                smoCol.Type = col.DataType.ToString();

                if (!QueryObject.Properties.Contains(col.ColumnName.ToLower().Replace(" ", "_")))
                {
                    QueryObject.Properties.Create(smoCol);
                }                
            }

            // methods - list, load, count, query list, query load, query count

            Method ListMethod = new Method();
            ListMethod.Name = "List";
            ListMethod.MetaData.DisplayName = "List";
            ListMethod.Type = MethodType.List;

            foreach (Property prop in QueryObject.Properties)
            {
                ListMethod.ReturnProperties.Add(prop);
            }
            QueryObject.Methods.Create(ListMethod);

            Method LoadMethod = new Method();
            LoadMethod.Name = "Load";
            LoadMethod.MetaData.DisplayName = "Load";
            LoadMethod.Type = MethodType.Read;

            // need to take input of row number you want

            foreach (Property prop in QueryObject.Properties)
            {
                LoadMethod.ReturnProperties.Add(prop);
            }
            QueryObject.Methods.Create(LoadMethod);

            Method RowCountMethod = new Method();
            RowCountMethod.Name = "RowCount";
            RowCountMethod.MetaData.DisplayName = "Row Count";
            RowCountMethod.Type = MethodType.Read;

            RowCountMethod.ReturnProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjectrowcount", StringComparison.OrdinalIgnoreCase)).First());
            QueryObject.Methods.Create(RowCountMethod);


            Method ListQueryMethod = new Method();
            ListQueryMethod.Name = "QueryList";
            ListQueryMethod.MetaData.DisplayName = "Query List";
            ListQueryMethod.Type = MethodType.List;

            ListQueryMethod.InputProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjectquery", StringComparison.OrdinalIgnoreCase)).First());
            foreach (Property prop in QueryObject.Properties)
            {
                ListQueryMethod.ReturnProperties.Add(prop);
            }
            QueryObject.Methods.Create(ListQueryMethod);

            Method LoadQueryMethod = new Method();
            LoadQueryMethod.Name = "QueryLoad";
            LoadQueryMethod.MetaData.DisplayName = "Query Load";
            LoadQueryMethod.Type = MethodType.Read;

            LoadQueryMethod.InputProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjectquery", StringComparison.OrdinalIgnoreCase)).First());
            foreach (Property prop in QueryObject.Properties)
            {
                LoadQueryMethod.ReturnProperties.Add(prop);
            }
            QueryObject.Methods.Create(LoadQueryMethod);

            Method RowCountQueryMethod = new Method();
            RowCountQueryMethod.Name = "QueryRowCount";
            RowCountQueryMethod.MetaData.DisplayName = "Query Row Count";
            RowCountQueryMethod.Type = MethodType.Read;

            RowCountQueryMethod.InputProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjectquery", StringComparison.OrdinalIgnoreCase)).First());
            RowCountQueryMethod.ReturnProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjectrowcount", StringComparison.OrdinalIgnoreCase)).First());
            QueryObject.Methods.Create(RowCountQueryMethod);


            // to json

            // to xml

            // to excel

            // to file


            serviceBroker.Service.ServiceObjects.Create(QueryObject);





        }
        #endregion

        #region XmlDocument DiscoverSchema()
        /// <summary>
        /// Discovers the schema of the underlying data and services, and then maps the schema into a structure and format which is compliant with the requirements of Service Objects.
        /// </summary>
        /// <returns>An XmlDocument containing the discovered schema in a structure which complies with the requirements of Service Objects.</returns>
        public XmlDocument DiscoverSchema()
        {
            // Add schema discovery and mapping code here.
            throw new NotImplementedException();
        }
        #endregion

        #region TypeMappings GetTypeMappings()
        /// <summary>
        /// Gets the type mappings used to map the underlying data's types to the appropriate K2 SmartObject types.
        /// </summary>
        /// <returns>A TypeMappings object containing the ServiceBroker's type mappings which were previously stored in the service instance configuration.</returns>
        public TypeMappings GetTypeMappings()
        {
            // Lookup and return the type mappings stored in the service instance.
            return (TypeMappings)serviceBroker.Service.ServiceConfiguration[__TypeMappings];
        }
        #endregion

        #region void SetTypeMappings()
        /// <summary>
        /// Sets the type mappings used to map the underlying data's types to the appropriate K2 SmartObject types.
        /// </summary>
        public void SetTypeMappings()
        {
            // Variable declaration.
            TypeMappings map = new TypeMappings();

            // Add type mappings.
            //throw new NotImplementedException();

            // Add the type mappings to the service instance.
            serviceBroker.Service.ServiceConfiguration.Add(__TypeMappings, map);
        }
        #endregion

        #region void Execute(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        /// <summary>
        /// Executes the Service Object method and returns any data.
        /// </summary>
        /// <param name="inputs">A Property[] array containing all the allowed input properties.</param>
        /// <param name="required">A RequiredProperties collection containing the required properties.</param>
        /// <param name="returns">A Property[] array containing all the allowed return properties.</param>
        /// <param name="methodType">A MethoType indicating what type of Service Object method was called.</param>
        /// <param name="serviceObject">A ServiceObject containing populated properties for use with the method call.</param>
        public void Execute(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {

            string query = string.Empty;

            if (inputs.Where(p => p.Name.Equals("queryobjectquery", StringComparison.OrdinalIgnoreCase)).FirstOrDefault() != null)
            {
                query = inputs.Where(p => p.Name.Equals("queryobjectquery", StringComparison.OrdinalIgnoreCase)).FirstOrDefault().Value.ToString();
            }
            else
            {
                query = GetQuery(serviceBroker.Service.ServiceConfiguration["Query"].ToString());
            }

            DataTable dt = QuerySmartObject(query);

            if (serviceObject.Methods[0].Name.Equals("List", StringComparison.OrdinalIgnoreCase) || serviceObject.Methods[0].Name.Equals("QueryList", StringComparison.OrdinalIgnoreCase))
            {
                serviceObject.Properties.InitResultTable();
                System.Data.DataRow dr;

                foreach (DataRow resrow in dt.Rows)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                    foreach (Property prop in returns)
                    {
                        if (dt.Columns.Contains(prop.MetaData.DisplayName))
                        {
                            dr[prop.Name] = resrow[prop.MetaData.DisplayName];
                        }
                    }
                    dr["queryobjectquery"] = query;
                    dr["queryobjectrowcount"] = dt.Rows.Count;
                    serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                }
            }


            if (serviceObject.Methods[0].Name.Equals("Load", StringComparison.OrdinalIgnoreCase) || serviceObject.Methods[0].Name.Equals("QueryLoad", StringComparison.OrdinalIgnoreCase)
                || serviceObject.Methods[0].Name.Equals("RowCount", StringComparison.OrdinalIgnoreCase) || serviceObject.Methods[0].Name.Equals("QueryRowCount", StringComparison.OrdinalIgnoreCase))
            {
                serviceObject.Properties.InitResultTable();
                if (dt.Rows.Count > 0)
                {
                    foreach (Property prop in returns)
                    {
                        if (dt.Columns.Contains(prop.MetaData.DisplayName))
                        {
                            prop.Value = dt.Rows[0][prop.MetaData.DisplayName];
                        }
                    }
                }
                returns.Where(p => p.Name.Equals("queryobjectrowcount", StringComparison.OrdinalIgnoreCase)).FirstOrDefault().Value = query;
                returns.Where(p => p.Name.Equals("queryobjectrowcount", StringComparison.OrdinalIgnoreCase)).FirstOrDefault().Value = dt.Rows.Count;

                serviceObject.Properties.BindPropertiesToResultTable();
            }
            
        }

        #endregion

        #endregion
    }
}