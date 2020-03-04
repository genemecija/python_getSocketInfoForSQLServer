import pandas as pd
import pyodbc
import pymssql
import sys
sys.path.append(r'..\Data')
import crds # external file containing username and password data

# Get Node Servers and Port Num for given serverName

def getSocketForServer(serverName):

    # Creds
    username = crds.usr
    passw = crds.pw

    queryIP = """SELECT CONNECTIONPROPERTY('local_net_address') AS [IPAddress]"""

    queryDynamicPort = """DECLARE       @portNo   NVARCHAR(10)

        EXEC   xp_instance_regread
        @rootkey    = 'HKEY_LOCAL_MACHINE',
        @key        = 'Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll',
        @value_name = 'TcpDynamicPorts',
        @value      = @portNo OUTPUT

        SELECT [PortNumber] = @portNo"""

    queryStaticPort = """DECLARE       @portNo   NVARCHAR(10)
    
        EXEC   xp_instance_regread
        @rootkey    = 'HKEY_LOCAL_MACHINE',
        @key        =
        'Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll',
        @value_name = 'TcpPort',
        @value      = @portNo OUTPUT
        
        SELECT [PortNumber] = @portNo"""

    port = 0
    error = ''

    try:
        # Connect to Database using pymssql
        conn = pymssql.connect(
            user = 'mmc\\'+username, 
            password = passw, 
            host = serverName, 
            database = 'master'
        )

        try:
            serverIP = pd.read_sql_query(queryIP, conn)['IPAddress'][0].decode('UTF-8')
        except Exception as IPerror:
            error = "Error-IP: " + str(IPerror)
            return(error)

        try:
            portNum = pd.read_sql_query(queryDynamicPort, conn)
            if portNum['PortNumber'][0] == None:
                portNum = pd.read_sql_query(queryStaticPort, conn)
                portType = 'Static'
            port = portNum['PortNumber'][0]

        except Exception as e:
            error = "Error-Port: " + str(e)
            return(error)

    except Exception as e:
        error = "Error-DBConn: " + str(e)
        return(error)
    
    IPandPort = str(serverIP) + ":" + str(port)

    return(IPandPort)