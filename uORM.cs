/*
License: http://www.apache.org/licenses/LICENSE-2.0
Email ID: micro.sudha@gmail.com
 *  
 I ll be glad if you can send email to my ID if you use this library. Soon it will be hosted on GitHub


Simple and tiny object relation mapper for CRUD frameworks. Uses dapper to create on fly serialization methods for mapping results to the objects
 and uses reflection for Add,Update and Delete operations.
 * 
 * Presently it uses Castle dynamic proxy for mapping properties to columns. So all the object properties must be virtual
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Castle.DynamicProxy;
using System.Runtime;
using System.Reflection;
using System.Reflection.Emit;
using System.Data;
using iWorks.Common.Dapper;

namespace iWorks.uMapper
{
    public static class TypeExtensions
    {
        public static PropertyInfo GetPropertyFirstOrDefault(this Type type, string propertyName)
        {
            foreach (PropertyInfo info in type.GetProperties())
            {
                if (info.Name == propertyName)
                    return info;
            }
            return null;
        }
    }
    public interface IObjectPropertyMapper<T> where T : class
    {
        T GetObject();

        string Pop();

        object PopWithValue(out string mappedName);
    }

    class PropertyMapInfo<T>
    {
        internal T mapValue;
        internal Type propertyType;
        internal string propertyName;
    }

    class ObjectPropertiesInterceptor<T> : Castle.DynamicProxy.IInterceptor where T : class
    {

        List<string> getInvokeList;
        T proxyObject;

        public ObjectPropertiesInterceptor()
        {

            ProxyGenerator proxyGen = new ProxyGenerator();
            proxyObject = proxyGen.CreateClassProxy<T>(this);

            getInvokeList = new List<string>();
        }

        public T InterceptObject
        {
            get { return proxyObject; }
        }

        public virtual void Intercept(Castle.DynamicProxy.IInvocation invocation)
        {
            if (invocation.Method.Name.StartsWith("get_", StringComparison.OrdinalIgnoreCase))
                getInvokeList.Add(invocation.Method.Name.Substring(4));

            invocation.Proceed();
        }

        public string GetGetInvocation()
        {
            string val= getInvokeList[0];

            getInvokeList.RemoveAt(0);

            return val;
        }

        public string[] GetGetInvokeList()
        {
            string[] val = getInvokeList.ToArray();

            getInvokeList.Clear();

            return val;
        }
    }

    public class ObjectQueryMapper<T, TMap> : IObjectPropertyMapper<T> where T : class
    {
        #region IObjectPropertyAccessor<T> Members
        ObjectPropertiesInterceptor<T> objectInterceptor;

        Dictionary<string, PropertyMapInfo<TMap>> propertyMap;

        List<string> getInvokeList;

        public ObjectQueryMapper()
        {
            propertyMap = new Dictionary<string, PropertyMapInfo<TMap>>();
            objectInterceptor = new ObjectPropertiesInterceptor<T>();
            SetupPropertyMap();
        }

        public T GetObject()
        {
            return objectInterceptor.InterceptObject;
        }

        private void SetupPropertyMap()
        {
            PropertyInfo[] props = typeof(T).GetProperties();

            foreach (PropertyInfo p in props)
            {
                try
                {
                    PropertyMapInfo<TMap> mapInfo = new PropertyMapInfo<TMap>();
                    mapInfo.propertyType = p.PropertyType;
                    mapInfo.propertyName = p.Name;
                    propertyMap.Add(p.Name, mapInfo);
                }
                catch
                {

                }
            }
        }


        public void SetMapValue(object[] properties, TMap[] mapValues)
        {
            int valueIndex = 0;
            string[] getProperties = objectInterceptor.GetGetInvokeList();
            foreach (string property in getProperties)
            {
                propertyMap[property].mapValue = mapValues[valueIndex];

                valueIndex++;
            }
        }

        public void Select(params object[] properties)
        {

        }

        public object PopWithValue(out string mappedName)
        {
            throw new NotImplementedException();
        }

        public TMap this[object property]
        {
            get { return propertyMap[objectInterceptor.GetGetInvocation()].mapValue; }
            set { propertyMap[objectInterceptor.GetGetInvocation()].mapValue = value; }
        }

        public string Pop()
        {
            return "";
        }

         public virtual void RemoveNullRefMappings()
        {
            List<string> removeList=new List<string>();
            foreach (KeyValuePair<string, PropertyMapInfo<TMap>> property in propertyMap)
            {
                if (property.Value.mapValue== null)
                    removeList.Add(property.Key);
            }

            foreach (string key in removeList)
            {
                propertyMap.Remove(key);
            }
        }

        internal IEnumerable<PropertyMapInfo<TMap>> PropertiesMapList
        {
            get
            {
                PropertyMapInfo<TMap>[] mapArray = new PropertyMapInfo<TMap>[propertyMap.Count];
                propertyMap.Values.CopyTo(mapArray, 0);

                return mapArray;
            }
        }


        #endregion
    }

    class DataSourceObjectsBag
    {
        static Dictionary<Type, Dictionary<string, object>> objectsBag;

        static DataSourceObjectsBag()
        {
            objectsBag = new Dictionary<Type, Dictionary<string, object>>();
        }

        internal static void SetValue<T>(string keyName, object value)
        {
            Dictionary<string, object> objectBag;

            Type type = typeof(T);

            //get the dictory object associated with object name
            if (!objectsBag.TryGetValue(typeof(T), out objectBag))
            {
                objectBag = new Dictionary<string, object>();
                objectsBag[type] = objectBag;
            }

            //set the value to the object
            objectBag[keyName] = value;
        }

        internal static object GetValue<T>(string keyName)
        {
            Dictionary<string, object> objectBag;

            Type type = typeof(T);

            //get the dictory object associated with object name
            if (!objectsBag.TryGetValue(typeof(T), out objectBag))
            {
                return null;
            }

            //set the value to the object
            return objectBag[keyName];
        }

        internal static Dictionary<string, object> CreateBag<T>()
        {
            Dictionary<string, object> objectBag = new Dictionary<string, object>();
            objectsBag[typeof(T)] = objectBag;

            return objectBag;
        }
    }

    public class ColumnDetails
    {
        internal string columnName;
        internal bool autoGenerate;
        internal bool excludeUpdate;
        internal bool isBoundedColumn;
        internal string expression;

        internal ColumnDetails(string columnName)
        {
            this.columnName = columnName;
        }

        internal ColumnDetails(string columnName, bool autoGenerate)
            : this(columnName)
        {
            this.autoGenerate = autoGenerate;
        }

        internal ColumnDetails(string columnName, bool autoGenerate, bool isBounded)
            : this(columnName, autoGenerate)
        {
            this.isBoundedColumn = isBounded;
        }

        internal ColumnDetails(string columnName, string expression)
            : this(columnName)
        {
            this.expression = expression;
        }

        internal ColumnDetails(string columnName, string expression, bool excludeUpdate)
            : this(columnName, expression)
        {
            this.excludeUpdate = excludeUpdate;
        }
    }

    public class ObjectMapProvider<T> where T : class
    {
        static ObjectMapProvider<T> mapProvider;

        private const string DATA_SOURCE_KEY = "tableName";
        private const string COLS_MAP_KEY = "cols";

        Dictionary<string, object> propertyBag;

        List<string> excludeUpdateList;

        ObjectQueryMapper<T, ColumnDetails> objectColumns;

        public ObjectMapProvider()
        {
            objectColumns = new ObjectQueryMapper<T, ColumnDetails>();
            propertyBag = DataSourceObjectsBag.CreateBag<T>();
            excludeUpdateList = new List<string>();
        }

        protected virtual ObjectMapProvider<T> SetSourceName(string sourceObjectName)
        {
            propertyBag[DATA_SOURCE_KEY] = sourceObjectName;
            return this;
        }

        internal virtual string DataSourceObjectName
        {
            get { return (string)propertyBag[DATA_SOURCE_KEY]; }
        }



        protected virtual ObjectMapProvider<T> MapProperty(object param, string columnName)
        {
            objectColumns[param] = new ColumnDetails(columnName);
            return this;
        }

        protected virtual ObjectMapProvider<T> MapProperty(object param, string columnName, bool isPK)
        {
            objectColumns[param] = new ColumnDetails(columnName, isPK, isPK);

            return this;
        }

        protected virtual ObjectMapProvider<T> MapProperty(object param, string columnName, bool isPK,bool isBounded)
        {
            objectColumns[param] = new ColumnDetails(columnName, isPK, isBounded);

            return this;
        }

        protected virtual ObjectMapProvider<T> ExcludeUpdate(params object[] excludeList)
        {
            foreach (object v in excludeList)
            {
                objectColumns[v].excludeUpdate = true;
            }
            return this;
        }

        protected virtual ObjectMapProvider<T> MapProperties(object[] properties, string[] columns)
        {
            int colIndex = 0;
            foreach (object o in properties)
            {
                objectColumns[o] = new ColumnDetails(columns[colIndex++]);
            }

            return this;
        }

        public virtual void BeginMap(T t)
        {
        }

        public virtual void EndMap(T t)
        {

        }

        internal ObjectQueryMapper<T, ColumnDetails> MapObject
        {
            get { return objectColumns; }
        }

        static ObjectMapProvider()
        {
            mapProvider = new ObjectMapProvider<T>();
        }

        public static void RegisterProvider(ObjectMapProvider<T> mapProvider)
        {
            ObjectMapProvider<T>.mapProvider = mapProvider;
        }

        public static ObjectMapProvider<T> GetProvider()
        {
            return ObjectMapProvider<T>.mapProvider;
        }

        internal IEnumerable<PropertyMapInfo<ColumnDetails>> PropertiesMapList
        {
            get { return objectColumns.PropertiesMapList; }
        }

        public virtual string UpdateColumnOnDelete()
        {
            return null;
        }
    }

    public class DatabaseRepository<TType> where TType : class
    {
        internal static Action<IDbCommand, TType> addMethodParamReader;
        internal static Action<IDbCommand, TType> updateMethod;
        internal static Action<IDbCommand, int> deleteMethod;
        internal static Func<IDataReader, object> typeDeserializer;

        IDbConnection dbConnection;

        internal static ObjectQueryMapper<TType, ColumnDetails> objectMapper;

        static string getAllQuery;
        static string getByIDQuery;
        static string getAllFilterQuery;

        static string deleteColumn;

        public virtual IDbConnection DBConnection
        {
            get { return dbConnection; }
            set { dbConnection = value; }
        }

        public DatabaseRepository(IDbConnection connection)
        {
            this.dbConnection = connection;
        }


        internal static void SetDeleteColumn(string deleteColumnName)
        {
            deleteColumn = deleteColumnName;
        }

        internal static void SetGetQuery(string getByIDQuery, string getAllQuery, string getAllFilterQuery)
        {
            DatabaseRepository<TType>.getAllQuery = getAllQuery;
            DatabaseRepository<TType>.getByIDQuery = getByIDQuery;
            DatabaseRepository<TType>.getAllFilterQuery = getAllFilterQuery;
        }

        public virtual void Add(TType addObject)
        {
            IEnumerator<PropertyMapInfo<ColumnDetails>> enumer = objectMapper.PropertiesMapList.GetEnumerator();

            PropertyMapInfo<ColumnDetails> autoGenColumn = null;
            while (enumer.MoveNext())
            {
                if (enumer.Current.mapValue.autoGenerate)
                {
                    autoGenColumn = enumer.Current;
                    break;
                }
            }

            if (autoGenColumn != null)
            {
                int idGenerated = InsertAndFetchNewID(addMethodParamReader, addObject);
                addObject.GetType().GetPropertyFirstOrDefault(autoGenColumn.propertyName).SetValue(addObject, idGenerated, null);
            }
            else
                ExecuteNonQuery(addMethodParamReader, addObject);
        }

        public virtual void Update(TType updateObject)
        {
            if (ExecuteNonQuery(updateMethod, updateObject) == 0)
                throw new ApplicationException("Update failed, 0 rows affected");
        }

        public virtual int Delete(int id)
        {
            return ExecuteNonQuery(deleteMethod, id);
        }




        /// <summary>
        /// Executes the query for the command with command intializer and returns the rows affected
        /// </summary>
        /// <param name="dbCommand"></param>
        /// <param name="param"></param>
        /// <param name="commandInitalizer"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(Action<IDbCommand, TType> commandInitalizer, TType param)
        {
            bool wasClosed = dbConnection.State == ConnectionState.Closed;
            try
            {
                if (wasClosed) dbConnection.Open();
                IDbCommand command = dbConnection.CreateCommand();
                commandInitalizer(command, param);
                return command.ExecuteNonQuery();
            }
            finally
            {
                if (wasClosed) dbConnection.Close();
            }   
        }

        public IEnumerable<TType> Query<TSecond>(string sql, Func<TType, TSecond, TType> map,object dynamicParam=null)
        {
            return dbConnection.Query<TType, TSecond, TType>(sql, map, dynamicParam);
        }


        public int InsertAndFetchNewID(Action<IDbCommand, TType> commandInitalizer, TType param)
        {
            bool wasClosed = dbConnection.State == ConnectionState.Closed;
            IDataReader reader = null;
            IDbCommand command = null;

            try
            {
                if (wasClosed) dbConnection.Open();
                command = dbConnection.CreateCommand();
                commandInitalizer(command, param);
                command.CommandText += System.Environment.NewLine + "Select @@ROWCOUNT,@@IDENTITY";
                reader = command.ExecuteReader();

                if (!reader.Read())
                    throw new ApplicationException("Failed read reader after executing insert query");

                if (reader.GetInt32(0) == 0)
                    throw new ApplicationException("RowCount 0 after executing insert query");

                //return the ID generated
                object retVal = reader.GetValue(1);

                int identity = Convert.ToInt32(retVal);

                reader.Dispose();
                reader = null;

                return identity;
            }
            finally
            {
                if (reader != null)
                {
                    if (!reader.IsClosed) try { command.Cancel(); }
                        catch { /* don't spoil the existing exception */ }
                    reader.Dispose();
                }

                if (wasClosed) dbConnection.Close();
            }
        }

        public int ExecuteNonQuery(Action<IDbCommand, int> commandInitalizer, int param)
        {
            bool wasClosed = dbConnection.State == ConnectionState.Closed;
            try
            {
                if (wasClosed) dbConnection.Open();
                IDbCommand command = dbConnection.CreateCommand();
                commandInitalizer(command, param);
                return command.ExecuteNonQuery();
            }
            finally
            {
                if (wasClosed) dbConnection.Close();
            }
        }

        public void ExecuteNonQueryAndEnsureSingleRowAffected(string query, params object[] paramValues)
        {
            int rowsAffected = ExecuteNonQuery(query, paramValues);

            if (rowsAffected != 1)
                throw new ApplicationException("Row update failed while expecting single row, Rows updated " + rowsAffected);
        }

        public virtual int ExecuteNonQuery(string query, params object[] paramValues)
        {
            bool wasClosed = dbConnection.State == ConnectionState.Closed;
            try
            {
                if (wasClosed) dbConnection.Open();
                IDbCommand command = dbConnection.CreateCommand();
                command.CommandText = query;
                InitCommandParams(command, paramValues);
                return command.ExecuteNonQuery();
            }
            finally
            {
                if (wasClosed) dbConnection.Close();
            }
        }

        public virtual TType ExecuteScalar<TType>(string query, params object[] paramValues)
        {
            bool wasClosed = dbConnection.State == ConnectionState.Closed;
            try
            {
                if (wasClosed) dbConnection.Open();
                IDbCommand command = dbConnection.CreateCommand();
                command.CommandText = query;
                InitCommandParams(command, paramValues);
                object val= command.ExecuteScalar();

                if (val == DBNull.Value)
                    return default(TType);

                return (TType)val;
            }
            finally
            {
                if (wasClosed) dbConnection.Close();
            }
        }


        private static IDbCommand GetCommand(IDbConnection dbConnection, string query, string[] paramNames, object[] queryParams)
        {
            IDbCommand dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandType = CommandType.Text;
            dbCommand.CommandText = query;

            int paramValueIndex = 0;
            foreach (string paramName in paramNames)
            {
                if (query.IndexOf(paramName) > 0)
                {
                    IDataParameter param = dbCommand.CreateParameter();
                    param.DbType = SqlMapper.GetDBType(queryParams[paramValueIndex].GetType());
                    param.Value = queryParams[paramValueIndex];
                    param.ParameterName = paramName;

                    dbCommand.Parameters.Add(param);
                }

                paramValueIndex++;
            }

            return dbCommand;

        }

        

        protected virtual void InitCommandParams(IDbCommand command,  object[] paramValues)
        {
            string commandText = command.CommandText;
            for (int i = 0; i < paramValues.Length; i++)
            {
                IDataParameter param = command.CreateParameter();
                param.ParameterName = "@" + i;
                param.Value = paramValues[i];
                commandText = commandText.Replace("{" + i + "}", "@" + i);
                SqlMapper.GetDBType(param.Value.GetType());

                command.Parameters.Add(param);
            }

            command.CommandText = commandText;

        }

        public void ExecuteAdd(IDbCommand dbCommand)
        {
            dbCommand.CommandText = "Insert into tblValues";
        }

        public TType GetByID(int id)
        {
            IDbCommand dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandText = getByIDQuery;
            dbCommand.CommandType = CommandType.Text;
            IDbDataParameter param = dbCommand.CreateParameter();
            param.Value = id;
            param.ParameterName = "@ID";
            param.DbType = DbType.Int32;

            dbCommand.Parameters.Add(param);

            List<TType> list = ExecuteReader(dbCommand);

            if (list.Count == 0)
                return null;

            return list[0];
        }

        public virtual List<TType> GetAll()
        {
            IDbCommand dbCommand = dbConnection.CreateCommand();

            if (!string.IsNullOrEmpty(deleteColumn))
            {
                getAllQuery += " Where " + deleteColumn + " = 0";
            }
            dbCommand.CommandText = getAllQuery;
            dbCommand.CommandType = CommandType.Text;

            
            return ExecuteReader(dbCommand);
        }

        /// <summary>
        /// Returns the TType list for the specified where condition. 
        /// Where keyword ll be automatically appended to the query
        /// </summary>
        /// <param name="whereCondition"></param>
        /// <param name="paramValues"></param>
        /// <returns></returns>
        public List<TType> GetAll(string whereCondition, params object[] paramValues)
        {
            IDbCommand dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandText = getAllQuery;
            dbCommand.CommandType = CommandType.Text;

            //add delete column condition to the where condition
            //if (!string.IsNullOrEmpty(deleteColumn))
            //{
            //    if (!string.IsNullOrEmpty(whereCondition))
            //        whereCondition = " AND " + deleteColumn + " = 0 ";
            //    else
            //        whereCondition = deleteColumn + " = 0 ";
            //}

            //append where condition if not empty
            if (!string.IsNullOrEmpty(whereCondition))
                dbCommand.CommandText += " Where " + whereCondition;

            InitCommandParams(dbCommand, paramValues);
            return ExecuteReader(dbCommand);
        }

        public TType GetFirstOrDefault(IEnumerable<TType> ttypeEnum)
        {
            List<TType> list = ttypeEnum.ToList();

            if (list.Count == 0)
                return default(TType);

            return list[0];
        }

        public TType FirstOfDefault(string whereCondition, params object[] paramValues)
        {
            List<TType> list = GetAll(whereCondition, paramValues);

            if (list.Count == 0)
                return default(TType);

            return list[0];
        }

        /// <summary>
        /// Returns the TTYpe instance by executing the select query with specfified where condition. 
        /// Do no specify the where key word in the where condition, it does it
        /// </summary>
        /// <param name="whereCondition"></param>
        /// <param name="paramNames"></param>
        /// <param name="paramValues"></param>
        /// <returns></returns>
        public TType QueryWithWhereCondition(string whereCondition, string[] paramNames, object[] paramValues)
        {
            string query = getAllQuery + " Where " + whereCondition;
            IDbCommand dbCommand = GetCommand(dbConnection, query, paramNames, paramValues);

            List<TType> list = ExecuteReader(dbCommand);

            if (list.Count == 0)
                return null;

            return list[0];
        }

        private List<TType> ExecuteReader(IDbCommand dbCommand)
        {
            IDbCommand cmd = null;
            IDataReader reader = null;
            bool wasClosed = dbConnection.State == ConnectionState.Closed;
            try
            {
                if (wasClosed) dbConnection.Open();

                reader = dbCommand.ExecuteReader(wasClosed ? CommandBehavior.CloseConnection : CommandBehavior.Default);
                wasClosed = false;

                List<TType> list = new List<TType>();

                if (typeDeserializer == null)
                   typeDeserializer= GetTypeDeserializer(reader);

                while (reader.Read())
                    list.Add((TType)typeDeserializer(reader));

                reader.Dispose();
                reader = null;

                return list;
            }
            finally
            {
                if (reader != null)
                {
                    if (!reader.IsClosed) try { cmd.Cancel(); }
                        catch { /* don't spoil the existing exception */ }
                    reader.Dispose();
                }
                if (wasClosed) dbConnection.Close();
                if (dbCommand != null) dbCommand.Dispose();
            }
        }

        public Func<IDataReader, object> GetTypeDeserializer(IDataReader reader)
        {
            IEnumerable<PropertyMapInfo<ColumnDetails>> columns = objectMapper.PropertiesMapList;
          

            return SqlMapper.GetTypeDeserializer<TType>(reader);

        }

        internal DatabaseRepository<TType> Clone(IDbConnection dbConnection)
        {
            DatabaseRepository<TType> repo = new DatabaseRepository<TType>(dbConnection);

            /*repo.addMethodParamReader = this.addMethodParamReader;
            repo.deleteMethod = this.deleteMethod;
            repo.getAllFilterQuery = this.getAllFilterQuery;
            repo.getAllQuery = this.getAllQuery;
            repo.getByIDQuery = this.getByIDQuery;
            repo.typeDeserializer = this.typeDeserializer;
            repo.updateMethod = this.updateMethod;
            */
            return repo;
        }
    }

 
    public class DatabaseRepositoryCreater<TType> where TType : class
    {
        ObjectMapProvider<TType> mapProvider;

        TType tTypeInstance;

        IDbConnection dbConnection;
        DatabaseRepository<TType> repo;

        internal DatabaseRepository<TType> Repository
        {
            get { return repo; }
        }

        IDbConnection DBConnection
        {
            get { return dbConnection; }
            set { dbConnection = value; }
        }

        public DatabaseRepositoryCreater(IDbConnection connection,DatabaseRepository<TType> repo)
        {
            this.dbConnection = connection;

            //get the provider
            mapProvider = ObjectMapProvider<TType>.GetProvider();
            DatabaseRepository<TType>.objectMapper = mapProvider.MapObject;
            tTypeInstance = Activator.CreateInstance<TType>();

            this.repo = repo;
        }


        /// <summary>
        /// Invoke provider to map the columns
        /// </summary>
        internal void MapIt()
        {
            mapProvider.BeginMap(mapProvider.MapObject.GetObject());

            mapProvider.EndMap(mapProvider.MapObject.GetObject());

            mapProvider.MapObject.RemoveNullRefMappings();

            InitDapperMapper();

            AddInsertMethod();

            AddUpdateMethod();

            AddDeleteMethod();

            InitSelectMethods();
        }

        protected virtual void InitDapperMapper()
        {
            CustomPropertyTypeMap typeMap = new CustomPropertyTypeMap(typeof(TType), (type, columnName) =>
          {
              foreach (PropertyMapInfo<ColumnDetails> propMap in mapProvider.PropertiesMapList)
              {
                  if (propMap.mapValue.columnName == columnName)
                      return type.GetPropertyFirstOrDefault(propMap.propertyName);
              }

              return type.GetProperty(columnName);
          });

            SqlMapper.SetTypeMap(typeof(TType), typeMap);
        }

        private void RemoveEmptyProperties()
        {
            List<PropertyMapInfo<ColumnDetails>> removeList = new List<PropertyMapInfo<ColumnDetails>>();
        }


        public void InitSelectMethods()
        {
            StringBuilder selectList = new StringBuilder();
            IEnumerable<PropertyMapInfo<ColumnDetails>> properties = mapProvider.PropertiesMapList;

            string appendComma = "";

            ColumnDetails pkColumn = null;

            selectList.Append("Select ");
            foreach (PropertyMapInfo<ColumnDetails> property in properties)
            {
                ColumnDetails colDetails = property.mapValue;
                if (colDetails.isBoundedColumn)
                    pkColumn = colDetails;

                selectList.Append(appendComma + colDetails.columnName);

                appendComma = ",";
            }

            selectList.Append(" From " + mapProvider.DataSourceObjectName);

            StringBuilder getByIDQry = new StringBuilder(selectList.ToString());
            getByIDQry.Append(" Where " + pkColumn.columnName + "=@ID");

            string deleteColumn = mapProvider.UpdateColumnOnDelete();

            if (!string.IsNullOrEmpty(deleteColumn))
                getByIDQry.Append(" AND " + deleteColumn + " = 0");

            DatabaseRepository<TType>.SetDeleteColumn(deleteColumn);
            DatabaseRepository<TType>.SetGetQuery(getByIDQry.ToString(), selectList.ToString(), null);
        }

        private void AddInsertMethod()
        {
            StringBuilder insertQry = new StringBuilder();
            StringBuilder valuesQry = new StringBuilder();

            insertQry.Append("Insert Into " + mapProvider.DataSourceObjectName + "(");
            IEnumerable<PropertyMapInfo<ColumnDetails>> properties = mapProvider.PropertiesMapList;
            string appendComma = "";
            foreach (PropertyMapInfo<ColumnDetails> property in properties)
            {
                ColumnDetails colDetails = property.mapValue;

                if (colDetails.autoGenerate)
                    continue;

                //insert query with columns
                insertQry.Append(appendComma + colDetails.columnName);

                //if there is constant expression is the value, then push without @
                if (!string.IsNullOrEmpty(colDetails.expression))
                    valuesQry.Append(appendComma + colDetails.expression);
                else
                    valuesQry.Append(appendComma + "@" + property.propertyName + "");

                appendComma = ",";
            }

            insertQry.Append(") VALUES (" + valuesQry.ToString() + ")");

            Action<IDbCommand, TType> addMethod = dbConnection.GetExecutNonQueryActionImpl(insertQry.ToString(), tTypeInstance);

            DatabaseRepository<TType>.addMethodParamReader = addMethod;
        }

        private void AddUpdateMethod()
        {
            StringBuilder updateQry = new StringBuilder();

            updateQry.Append("Update " + mapProvider.DataSourceObjectName + " Set ");
            IEnumerable<PropertyMapInfo<ColumnDetails>> properties = mapProvider.PropertiesMapList;
            string appendComma = "";

            PropertyMapInfo<ColumnDetails> pkProperty = null;

            foreach (PropertyMapInfo<ColumnDetails> property in properties)
            {
                ColumnDetails colDetails = property.mapValue;

                if (colDetails.isBoundedColumn)
                {
                    pkProperty = property;
                    continue;
                }

                //ignore if needs to be ignored while updating
                if (colDetails.excludeUpdate)
                    continue;


                //insert query with columns
                updateQry.Append(appendComma + colDetails.columnName + " = @" + property.propertyName);

                appendComma = ",";
            }

            if (pkProperty == null)
                throw new ArgumentNullException("PK Property not found");

            ColumnDetails pkColumn = pkProperty.mapValue;

            updateQry.Append(" Where " + pkColumn.columnName + " = @" + pkProperty.propertyName);

            Action<IDbCommand, TType> updateQryMethod = dbConnection.GetExecutNonQueryActionImpl(updateQry.ToString(), tTypeInstance);

            DatabaseRepository<TType>.updateMethod = updateQryMethod;
        }

        private PropertyMapInfo<ColumnDetails> GetPkColumn()
        {
         PropertyMapInfo<ColumnDetails> pkProperty = null;

            foreach (PropertyMapInfo<ColumnDetails> property in mapProvider.PropertiesMapList)
            {
                ColumnDetails colDetails = property.mapValue;

                if (colDetails.isBoundedColumn)
                {
                    pkProperty = property;
                    return pkProperty;
                }
            }

        return null;
        }


        private void AddDeleteMethod()
        {
            var dm = new DynamicMethod("DBRepo_" + typeof(TType).ToString() + "_CRUD_Delete", null, new[] { typeof(IDbCommand), typeof(int) }, typeof(DatabaseRepository<TType>), true);
            var il = dm.GetILGenerator();

            //create local param
            il.DeclareLocal(typeof(IDbDataParameter));

            //il.Emit(OpCodes.Nop);

            Type dbCommandType = typeof(IDbCommand);

            //create and set param object
            il.Emit(OpCodes.Ldarg_0);
            il.EmitCall(OpCodes.Callvirt, dbCommandType.GetMethod("CreateParameter"), null);
            il.Emit(OpCodes.Stloc_0);

            ////set command text
            il.Emit(OpCodes.Ldarg_0);

            string deleteQry="";
            string updateColumn = mapProvider.UpdateColumnOnDelete();

            if (!string.IsNullOrEmpty(updateColumn))
                deleteQry = "Update " + mapProvider.DataSourceObjectName + " Set " + updateColumn + " = 1 Where " + GetPkColumn().mapValue.columnName + " = @ID";
            else
                deleteQry = "Delete *  From  Where " + GetPkColumn().mapValue.columnName + " = @ID";

            il.Emit(OpCodes.Ldstr, deleteQry);

            il.EmitCall(OpCodes.Callvirt, dbCommandType.GetProperty("CommandText").GetSetMethod(), null);
            il.Emit(OpCodes.Nop);

            //set command type
            il.Emit(OpCodes.Ldarg_0);
            EmitInt32(il, (int)CommandType.Text);
            il.EmitCall(OpCodes.Callvirt, dbCommandType.GetProperty("CommandType").GetSetMethod(), null);
            il.Emit(OpCodes.Nop);

            Type dbParamType = typeof(IDbDataParameter);

            //set db type
            il.Emit(OpCodes.Ldloc_0); //load param
            EmitInt32(il, (int)DbType.Int32);
            il.EmitCall(OpCodes.Callvirt, typeof(IDataParameter).GetProperty("DbType").GetSetMethod(), null);
            il.Emit(OpCodes.Nop);

            //set dbparam value
            il.Emit(OpCodes.Ldloc_0); //load param
            il.Emit(OpCodes.Ldarg_1); //load id
            il.Emit(OpCodes.Box, typeof(System.Int32));
            il.EmitCall(OpCodes.Callvirt, typeof(IDataParameter).GetProperty("Value").GetSetMethod(), null);
            il.Emit(OpCodes.Nop);

            //set dbparam name
            il.Emit(OpCodes.Ldloc_0);  //load param
            il.Emit(OpCodes.Ldstr, "@ID"); //load @ID
            il.EmitCall(OpCodes.Callvirt, typeof(IDataParameter).GetProperty("ParameterName").GetSetMethod(), null);
            il.Emit(OpCodes.Nop);

            //add Param
            il.Emit(OpCodes.Ldarg_0); //load command
            //call parameters
            il.EmitCall(OpCodes.Callvirt, dbCommandType.GetProperty("Parameters").GetGetMethod(), null);
            il.Emit(OpCodes.Ldloc_0); //load the parameters collection 
            //add the param
            il.EmitCall(OpCodes.Callvirt, typeof(System.Collections.IList).GetMethod("Add"), null);
            il.Emit(OpCodes.Pop);
            il.Emit(OpCodes.Nop);

            il.Emit(OpCodes.Ret);


            DatabaseRepository<TType>.deleteMethod = (Action<IDbCommand, int>)dm.CreateDelegate(typeof(Action<IDbCommand, int>));

        }

        public static void EmitInt32(ILGenerator il, int value)
        {
            switch (value)
            {
                case -1: il.Emit(OpCodes.Ldc_I4_M1); break;
                case 0: il.Emit(OpCodes.Ldc_I4_0); break;
                case 1: il.Emit(OpCodes.Ldc_I4_1); break;
                case 2: il.Emit(OpCodes.Ldc_I4_2); break;
                case 3: il.Emit(OpCodes.Ldc_I4_3); break;
                case 4: il.Emit(OpCodes.Ldc_I4_4); break;
                case 5: il.Emit(OpCodes.Ldc_I4_5); break;
                case 6: il.Emit(OpCodes.Ldc_I4_6); break;
                case 7: il.Emit(OpCodes.Ldc_I4_7); break;
                case 8: il.Emit(OpCodes.Ldc_I4_8); break;
                default:
                    if (value >= -128 && value <= 127)
                    {
                        il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldc_I4, value);
                    }
                    break;
            }
        }
    }

    public class DatabaseRepositoryProvider
    {
        static Dictionary<Type, object> repositoriesDictionary;

        static DatabaseRepositoryProvider()
        {
            repositoriesDictionary = new Dictionary<Type, object>();
        }

        public static DatabaseRepository<TType> GetRepository<TType>(IDbConnection connection) where TType : class
        {
            object temp;
            if (!repositoriesDictionary.TryGetValue(typeof(TType), out temp))
            {
                DatabaseRepository<TType> repo = new DatabaseRepository<TType>(connection);
                DatabaseRepositoryCreater<TType> provider = new DatabaseRepositoryCreater<TType>(connection, repo);

                provider.MapIt();

                repositoriesDictionary.Add(typeof(TType), provider.Repository);

                return provider.Repository;
            }

            DatabaseRepository<TType> createdRepo = (DatabaseRepository<TType>)temp;

            return createdRepo.Clone(connection);
        }

        public static void InitRepository<TType>(DatabaseRepository<TType> repo) where TType : class
        {
            object temp;
            if (!repositoriesDictionary.TryGetValue(typeof(TType), out temp))
            {
                DatabaseRepositoryCreater<TType> provider = new DatabaseRepositoryCreater<TType>(repo.DBConnection, repo);

                provider.MapIt();

                repositoriesDictionary.Add(typeof(TType), provider.Repository);
            }
        }
    }

    public class CRUDMethodsBuilder
    {
        /// <summary>
        /// Builds the Add Method to insert a record into the database from the TType
        /// </summary>
        /// <typeparam name="TType"></typeparam>
        /// <returns></returns>
        public static Func<TType, int> BuildAdd<TType>()
        {
            return null;
        }
    }
}
