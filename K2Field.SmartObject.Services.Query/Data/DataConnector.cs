using System;
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


            // need to validate SQL statement.
            // perhaps crudely check for SELECT as start?
            // check for ALTER/DROP/delete/truncate/etc


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

        private string ConstructQuery(string query, string condition)
        {
            string q = string.Empty;

            //adapted from - http://stackoverflow.com/questions/8777429/splitting-a-sql-string-per-command

            // Replace all [new line] to [space]
            while (query.Contains(Environment.NewLine))
            {
                query = query.Replace(Environment.NewLine, " ");
            }
            
            // Array of all sql commands using in query
            string[] sqlCommands = { "SELECT", "FROM", "WHERE", "HAVING", "GROUP BY", "ORDER BY" };

            Dictionary<string, int> sqlDictionary = new Dictionary<string, int>();
            sqlDictionary.Add("SELECT", 0);
            sqlDictionary.Add("FROM", 1);
            sqlDictionary.Add("WHERE", 2);
            sqlDictionary.Add("HAVING", 3);
            sqlDictionary.Add("GROUP BY", 4);
            sqlDictionary.Add("ORDER BY ", 5);

            // Insert before each sql expression new line
            foreach (string sqlCommand in sqlDictionary.Keys)
            {
                query = query.Replace(sqlCommand, Environment.NewLine + sqlCommand);
            }

            // Split big sql string to separate commands, and remove empty strings
            string[] sqlArray = query.Split(new string[] { Environment.NewLine },
                        StringSplitOptions.None);
            sqlArray = sqlArray.Where(cmd => !String.IsNullOrEmpty(cmd)).ToArray();
               
            // reconstruct query with new condition - NEEDS WORK
            q = sqlArray[0] + " " + sqlArray[1] + " " + condition;
            string gb = sqlArray.Where(p => p.StartsWith("GROUP BY", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            if(!string.IsNullOrWhiteSpace(gb))
            {
                q += " " + gb;
            }
            string ob = sqlArray.Where(p => p.StartsWith("ORDER BY", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(ob))
            {
                q += " " + ob;
            }

            return q;
        }

        #region Property Definition
        
        
        #endregion Property Definition


        #region Method Definition

              
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


            Property smoColHavingClause = new Property();
            smoColHavingClause.Name = "queryobjecthavingclause";
            smoColHavingClause.MetaData.DisplayName = "Having Clause";
            smoColHavingClause.SoType = SoType.Memo;
            QueryObject.Properties.Create(smoColHavingClause);

            Property smoColWhereClause = new Property();
            smoColWhereClause.Name = "queryobjectwhereclause";
            smoColWhereClause.MetaData.DisplayName = "Where Clause";
            smoColWhereClause.SoType = SoType.Memo;
            QueryObject.Properties.Create(smoColWhereClause);


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


            Method ListHavingMethod = new Method();
            ListHavingMethod.Name = "ListwithHaving";
            ListHavingMethod.MetaData.DisplayName = "List with Having";
            ListHavingMethod.Type = MethodType.List;

            ListHavingMethod.ReturnProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjecthavingclause", StringComparison.OrdinalIgnoreCase)).First());
            foreach (Property prop in QueryObject.Properties)
            {
                ListMethod.ReturnProperties.Add(prop);
            }
            ListHavingMethod.InputProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjecthavingclause", StringComparison.OrdinalIgnoreCase)).First());            
            QueryObject.Methods.Create(ListHavingMethod);


            Method ListWhereMethod = new Method();
            ListWhereMethod.Name = "ListwithWhere";
            ListWhereMethod.MetaData.DisplayName = "List with Where";
            ListWhereMethod.Type = MethodType.List;

            ListWhereMethod.ReturnProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjectwhereclause", StringComparison.OrdinalIgnoreCase)).First());
            foreach (Property prop in QueryObject.Properties)
            {
                ListMethod.ReturnProperties.Add(prop);
            }
            ListWhereMethod.InputProperties.Add(QueryObject.Properties.Where(p => p.Name.Equals("queryobjectwhereclause", StringComparison.OrdinalIgnoreCase)).First());
            QueryObject.Methods.Create(ListWhereMethod);
            

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

            string having = string.Empty;
            if (inputs.Where(p => p.Name.Equals("queryobjecthavingclause", StringComparison.OrdinalIgnoreCase)).FirstOrDefault() != null)
            {
                having = inputs.Where(p => p.Name.Equals("queryobjecthavingclause", StringComparison.OrdinalIgnoreCase)).FirstOrDefault().Value as string;
                if (having.StartsWith("having", StringComparison.OrdinalIgnoreCase))
                {
                    having = having.Substring(6);
                }
                having = "HAVING " + having.Trim();
            }

            string where = string.Empty;
            var whereProp = inputs.Where(p => p.Name.Equals("queryobjectwhereclause", StringComparison.OrdinalIgnoreCase)).FirstOrDefault();
            if (whereProp != null && whereProp.Value != null && !string.IsNullOrWhiteSpace(whereProp.Value.ToString()))
            {
                where = whereProp.Value.ToString();
                if ( where.StartsWith("where", StringComparison.OrdinalIgnoreCase))
                {
                    where = where.Substring(6);
                }
                where = "WHERE " + where.Trim();
            }

            string condition = string.Empty;
            if (!string.IsNullOrWhiteSpace(having))
            {
                condition = having;
            }
            else if (!string.IsNullOrWhiteSpace(where))
            {
                condition = where;
            }

            if (!string.IsNullOrWhiteSpace(condition))
            {
                // add condition to query
                query = ConstructQuery(query, condition);
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