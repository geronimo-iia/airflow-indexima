"""airflow-indexima definition."""
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution('airflow_indexima').version
except DistributionNotFound:
    __version__ = '(local)'


def feet_to_meters(feet):
    """Convert feet to meters."""
    try:
        value = float(feet)
    except ValueError:
        print("Unable to convert to float: %s", feet)
    else:
        return (0.3048 * value * 10000.0 + 0.5) / 10000.0
