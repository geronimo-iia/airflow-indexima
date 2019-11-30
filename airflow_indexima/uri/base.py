"""URI base function and type."""
from typing import Optional, Callable
from airflow_indexima.utils import ConnectionDecorator

__all__ = ['UriDecoratedFactory', 'UriFactory', 'define_load_path_factory']


UriDecoratedFactory = Callable[[str, Optional[ConnectionDecorator]]

UriFactory = Callable[[], str]

def define_load_path_factory(
    conn_id: str,
    decorator: ConnectionDecorator,
    factory: UriDecoratedFactory
) -> UriFactory:
    """Create an uri factory function with UriFactory profile.

    Example:

    def my_decorator(conn:Connection) -> Connection:
        ...
        return conn

    decorated_redshift_uri_factory = define_load_path_factory(
        decorator=my_decorator,
        factory=get_redshift_load_path_uri
        )

    # Parameter
        conn_id (str): connection identifier of data source
        decorator (ConnectionDecorator): Connection decorateur
        factory (UriDecoratedFactory): uri decorated factory

    # Return
        (UriFactory): function used as a macro to get load uri path
    """

    def _load_path_factory() -> str:
        return factory(connection_id=conn_id, decorator=decorator)

    return _load_path_factory
