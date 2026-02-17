import json
from india_stocks_api.internal.context import get_httpx_client, get_logger

logger = get_logger(__name__)


def authenticate_broker(api_key, clientcode, broker_pin, totp_code):
    """
    Authenticate with Angel One via loginByPassword endpoint.

    Returns:
        tuple: (jwt_token, feed_token, error)
            On success: (str, str|None, None)
            On failure: (None, None, str)
    """
    try:
        client = get_httpx_client()

        payload = json.dumps({
            "clientcode": clientcode,
            "password": broker_pin,
            "totp": totp_code
        })
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-UserType': 'USER',
            'X-SourceID': 'WEB',
            'X-ClientLocalIP': 'CLIENT_LOCAL_IP',
            'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
            'X-MACAddress': 'MAC_ADDRESS',
            'X-PrivateKey': api_key
        }

        url = "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword"
        response = client.post(url, headers=headers, content=payload)

        data_dict = json.loads(response.text)

        if 'data' in data_dict and data_dict['data'] and 'jwtToken' in data_dict['data']:
            auth_token = data_dict['data']['jwtToken']
            feed_token = data_dict['data'].get('feedToken')
            return auth_token, feed_token, None
        else:
            return None, None, data_dict.get('message', 'Authentication failed. Please try again.')
    except Exception as e:
        logger.error(f"Authentication request failed: {e}")
        return None, None, str(e)
