import requests
import time
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES=3
DEFAULT_BACKOFF_FACTOR=1.0
DEFAULT_TIMEOUT=10
RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]

RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    ConnectionResetError,
    IOError,
)

def build_retry_session(
    max_retries=DEFAULT_MAX_RETRIES,
    backoff_factor=DEFAULT_BACKOFF_FACTOR,
    status_forcelist=None,
    timeout=DEFAULT_TIMEOUT
):
    
    if status_forcelist is None:
        status_forcelist = RETRYABLE_STATUS_CODES

    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.timeout = timeout
    return session

def retry_call(
    func,
    args=(),
    kwargs=None,
    max_retries=DEFAULT_MAX_RETRIES,
    backoff_factor=DEFAULT_BACKOFF_FACTOR,
    timeout=None,
    label="",
):

    if kwargs is None:
        kwargs = {}

    last_exception = None

    for attempt in range(1, max_retries + 2):
        try:
            if timeout is not None:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, *args, **kwargs)
                    return future.result(timeout=timeout)
            else:
                return func(*args, **kwargs)

        except (FuturesTimeoutError, *RETRYABLE_EXCEPTIONS) as exc:
            last_exception = exc
            if attempt <= max_retries:
                wait = backoff_factor * (2 ** (attempt -1))
                logger.warning(
                    "%s attempt %d/%d failed (%s: %s). Retrying in %.1fs...",
                    label, attempt, max_retries+1,
                    type(exc).__name__, exc, wait
                )
                time.sleep(wait)
            else:
                logger.error(
                    "%s FAILED after %d attempts. Last error: %s: %s",
                    label, max_retries + 1, type(exc).__name__, exc,
                )
                raise last_exception