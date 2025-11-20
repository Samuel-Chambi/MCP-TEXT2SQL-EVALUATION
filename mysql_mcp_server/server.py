import asyncio
import logging
import os
import sys
from mysql.connector import connect, Error
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from pydantic import AnyUrl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mysql_mcp_server")

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "root"),
        "database": os.getenv("MYSQL_DATABASE", "bird_db"),
        "charset": os.getenv("MYSQL_CHARSET", "utf8mb4"),
        "collation": os.getenv("MYSQL_COLLATION", "utf8mb4_unicode_ci"),
        "autocommit": True,
    }
    
    # Filter out None values
    config = {k: v for k, v in config.items() if v is not None}
    
    # Validate required fields
    required_fields = ["user", "database"]
    for field in required_fields:
        if not config.get(field):
            logger.error(f"Missing required field: {field}")
            raise ValueError(f"Missing required database configuration: {field}")
    
    return config

# Initialize server
app = Server("mysql_mcp_server")

@app.list_resources()
async def list_resources() -> list[Resource]:
    """List MySQL tables as resources."""
    try:
        config = get_db_config()
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                resources = []
                for table in tables:
                    table_name = table[0]
                    resources.append(
                        Resource(
                            uri=AnyUrl(f"mysql://{config['database']}/{table_name}"),
                            name=f"Table: {table_name}",
                            mimeType="application/json",
                            description=f"Data from table: {table_name}"
                        )
                    )
                return resources
    except Error as e:
        logger.error(f"Database error in list_resources: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in list_resources: {str(e)}")
        return []

@app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read table contents."""
    try:
        uri_str = str(uri)
        if not uri_str.startswith("mysql://"):
            raise ValueError(f"Invalid URI scheme: {uri_str}")
        
        # Extract database and table from URI: mysql://database/table
        parts = uri_str[8:].split('/')
        if len(parts) < 2:
            raise ValueError(f"Invalid URI format: {uri_str}")
        
        database = parts[0]
        table = parts[1]
        
        config = get_db_config()
        config["database"] = database  # Use database from URI
        
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                # Safe table name validation
                cursor.execute("SHOW TABLES")
                valid_tables = [t[0] for t in cursor.fetchall()]
                
                if table not in valid_tables:
                    raise ValueError(f"Table not found: {table}")
                
                cursor.execute(f"SELECT * FROM `{table}` LIMIT 100")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                # Format as CSV
                result_lines = [",".join(columns)]
                for row in rows:
                    result_lines.append(",".join(str(cell) for cell in row))
                
                return "\n".join(result_lines)
                
    except Error as e:
        logger.error(f"Database error reading resource {uri}: {str(e)}")
        raise RuntimeError(f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error reading resource {uri}: {str(e)}")
        raise

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available MySQL tools."""
    return [
        Tool(
            name="execute_sql",
            description="Execute an SQL query on the MySQL database",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute SQL commands."""
    if name != "execute_sql":
        raise ValueError(f"Unknown tool: {name}")
    
    query = arguments.get("query", "").strip()
    if not query:
        raise ValueError("Query is required")
    
    try:
        config = get_db_config()
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                
                # For queries that return results
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    if rows:
                        # Format as table
                        result_lines = [" | ".join(columns)]
                        result_lines.append(" | ".join(["---"] * len(columns)))
                        for row in rows:
                            result_lines.append(" | ".join(str(cell) for cell in row))
                        result_text = "\n".join(result_lines)
                    else:
                        result_text = "No results found"
                    
                    return [TextContent(type="text", text=result_text)]
                else:
                    # For queries that don't return results
                    conn.commit()
                    return [TextContent(
                        type="text", 
                        text=f"Query executed successfully. Rows affected: {cursor.rowcount}"
                    )]
                    
    except Error as e:
        error_msg = f"Database error: {str(e)}"
        logger.error(error_msg)
        return [TextContent(type="text", text=error_msg)]
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return [TextContent(type="text", text=error_msg)]

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    try:
        config = get_db_config()
        logger.info(f"Starting MySQL MCP server for database: {config['database']}")
        
        # Test connection
        with connect(**config) as conn:
            logger.info(f"Connected to MySQL server version: {conn.get_server_info()}")
            
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
            
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())