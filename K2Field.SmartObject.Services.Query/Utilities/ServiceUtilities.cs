using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using SourceCode.SmartObjects.Services.ServiceSDK;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using K2Field.SmartObject.Services.Query.Data;

namespace K2Field.SmartObject.Services.Azure.ServiceBus.Utilities
{
    public class ServiceUtilities
    {
        private ServiceAssemblyBase serviceBroker = null;       

        public ServiceUtilities(ServiceAssemblyBase serviceBroker)
        {
            // Set local serviceBroker variable.
            this.serviceBroker = serviceBroker;
        }        
    }
}
