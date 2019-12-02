"""Define a decorator connection function profile.

Implementation can be used tp customized a connection like
retreive credentials from other backeng like aws ssm.

```ConnectionDecorator = Callable[[Connection], Connection]```

"""
from typing import Callable

from airflow.models import Connection


__all__ = ['ConnectionDecorator']

ConnectionDecorator = Callable[[Connection], Connection]
