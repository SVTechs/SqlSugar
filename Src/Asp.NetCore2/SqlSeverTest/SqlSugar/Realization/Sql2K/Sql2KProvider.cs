using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlSugar
{
    public class Sql2KProvider : AdoProvider
    {
        public Sql2KProvider()
        {
            NoNamedParameters = true;
        }
        public override IDbConnection Connection
        {
            get
            {
                if (base._DbConnection == null)
                {
                    try
                    {
                        base._DbConnection = new OdbcConnection(base.Context.CurrentConnectionConfig.ConnectionString);
                    }
                    catch (Exception ex)
                    {
                        Check.Exception(true, ErrorMessage.ConnnectionOpen, ex.Message);
                    }
                }
                return base._DbConnection;
            }
            set
            {
                base._DbConnection = value;
            }
        }
        /// <summary>
        /// Only SqlServer
        /// </summary>
        /// <param name="transactionName"></param>
        public override void BeginTran(string transactionName)
        {
            CheckConnection();
            base.Transaction = ((OdbcConnection)this.Connection).BeginTransaction();
        }
        /// <summary>
        /// Only SqlServer
        /// </summary>
        /// <param name="iso"></param>
        /// <param name="transactionName"></param>
        public override void BeginTran(IsolationLevel iso, string transactionName)
        {
            CheckConnection();
            base.Transaction = ((OdbcConnection)this.Connection).BeginTransaction(iso);
        }
        public override IDataAdapter GetAdapter()
        {
            return new SqlDataAdapter();
        }
        public override DbCommand GetCommand(string sql, SugarParameter[] parameters)
        {
            OdbcCommand sqlCommand = new OdbcCommand(sql, (OdbcConnection)this.Connection);
            sqlCommand.CommandType = this.CommandType;
            sqlCommand.CommandTimeout = this.CommandTimeOut;
            if (this.Transaction != null)
            {
                sqlCommand.Transaction = (OdbcTransaction)this.Transaction;
            }
            if (parameters.HasValue())
            {
                OdbcParameter[] ipars = GetSqlParameter(parameters);
                sqlCommand.Parameters.AddRange(ipars);
            }
            CheckConnection();
            return sqlCommand;
        }
        public override void SetCommandToAdapter(IDataAdapter dataAdapter, DbCommand command)
        {
            ((OdbcDataAdapter)dataAdapter).SelectCommand = (OdbcCommand)command;
        }
        /// <summary>
        /// if mysql return MySqlParameter[] pars
        /// if sqlerver return SqlParameter[] pars ...
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public override IDataParameter[] ToIDbDataParameter(params SugarParameter[] parameters)
        {
            if (parameters == null || parameters.Length == 0) return null;
            OdbcParameter[] result = new OdbcParameter[parameters.Length];
            int index = 0;
            foreach (var parameter in parameters)
            {
                if (parameter.Value == null) parameter.Value = DBNull.Value;
                var sqlParameter = new OdbcParameter();
                sqlParameter.ParameterName = parameter.ParameterName;
                //sqlParameter.UdtTypeName = parameter.UdtTypeName;
                sqlParameter.Size = parameter.Size;
                sqlParameter.Value = parameter.Value;
                sqlParameter.DbType = parameter.DbType;
                sqlParameter.Direction = parameter.Direction;
                result[index] = sqlParameter;
                if (sqlParameter.Direction.IsIn(ParameterDirection.Output, ParameterDirection.InputOutput, ParameterDirection.ReturnValue))
                {
                    if (this.OutputParameters == null) this.OutputParameters = new List<IDataParameter>();
                    this.OutputParameters.RemoveAll(it => it.ParameterName == sqlParameter.ParameterName);
                    this.OutputParameters.Add(sqlParameter);
                }
                ++index;
            }
            return result;
        }
        /// <summary>
        /// if mysql return MySqlParameter[] pars
        /// if sqlerver return SqlParameter[] pars ...
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public OdbcParameter[] GetSqlParameter(params SugarParameter[] parameters)
        {
            var isVarchar = this.Context.IsVarchar();
            if (parameters == null || parameters.Length == 0) return null;
            OdbcParameter[] result = new OdbcParameter[parameters.Length];
            int index = 0;
            foreach (var parameter in parameters)
            {
                if (parameter.Value == null) parameter.Value = DBNull.Value;
                var sqlParameter = new OdbcParameter();
                sqlParameter.ParameterName = parameter.ParameterName;
                //sqlParameter.UdtTypeName = parameter.UdtTypeName;
                sqlParameter.Size = parameter.Size;
                sqlParameter.Value = parameter.Value;
                sqlParameter.DbType = parameter.DbType;
                var isTime = parameter.DbType == System.Data.DbType.Time;
                if (isTime)
                {
                    sqlParameter.DbType = System.Data.DbType.Time;
                    sqlParameter.Value = DateTime.Parse(parameter.Value?.ToString()).TimeOfDay;
                }
                if (sqlParameter.Value != null && sqlParameter.Value != DBNull.Value && sqlParameter.DbType == System.Data.DbType.DateTime)
                {
                    var date = Convert.ToDateTime(sqlParameter.Value);
                    if (date == DateTime.MinValue)
                    {
                        sqlParameter.Value = Convert.ToDateTime("1753/01/01");
                    }
                }
                if (parameter.Direction == 0)
                {
                    parameter.Direction = ParameterDirection.Input;
                }
                sqlParameter.Direction = parameter.Direction;
                result[index] = sqlParameter;
                if (parameter.TypeName.HasValue())
                {
                    //sqlParameter.TypeName = parameter.TypeName;
                    //sqlParameter.SqlDbType = SqlDbType.Structured;
                    sqlParameter.DbType = System.Data.DbType.Object;
                }
                if (sqlParameter.Direction.IsIn(ParameterDirection.Output, ParameterDirection.InputOutput, ParameterDirection.ReturnValue))
                {
                    if (this.OutputParameters == null) this.OutputParameters = new List<IDataParameter>();
                    this.OutputParameters.RemoveAll(it => it.ParameterName == sqlParameter.ParameterName);
                    this.OutputParameters.Add(sqlParameter);
                }

                if (isVarchar && sqlParameter.DbType == System.Data.DbType.String)
                {
                    sqlParameter.DbType = System.Data.DbType.AnsiString;
                }

                ++index;
            }
            return result;
        }
    }
}
