import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Provides a Spark session for testing.
    
    When running in Databricks notebooks, uses the existing 'spark' session.
    When running locally, creates a new SparkSession.
    """
    # Try to use the global spark session from Databricks
    try:
        import __main__
        if hasattr(__main__, 'spark'):
            return __main__.spark
    except AttributeError:
        pass
    
    # Try to get spark from globals (when running from notebook Python cell)
    import sys
    for frame_info in sys._current_frames().values():
        frame_globals = frame_info.f_globals
        if 'spark' in frame_globals:
            return frame_globals['spark']
    
    # Fallback: create a new session for local testing
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[*]") \
        .getOrCreate()