from dagster import ConfigurableResource
import psycopg2


class PostgresResource(ConfigurableResource):
    host: str
    database: str
    user: str
    password: str
    port: int = 5433

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
        )
