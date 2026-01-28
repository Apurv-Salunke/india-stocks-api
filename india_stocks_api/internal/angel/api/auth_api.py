import json
from india_stocks_api.internal.context import get_httpx_client


def authenticate_broker(api_key, clientcode, broker_pin, totp_code):
    """
    Authenticate with Angel Broking and obtain a JWT and an optional feed token.
    
    Parameters:
        api_key (str): API private key to include in the request headers.
        clientcode (str): Broker client code/identifier.
        broker_pin (str): Broker PIN or password.
        totp_code (str): Time-based one-time password (TOTP) for two-factor authentication.
    
    Returns:
        tuple: (auth_token, feed_token, error_message)
            auth_token (str or None): JWT on successful authentication, otherwise None.
            feed_token (str or None): Feed token if returned by the broker, otherwise None.
            error_message (str or None): None on success; an error message or exception text on failure.
    """

    try:
        # Get the shared httpx client
        client = get_httpx_client()

        payload = json.dumps(
            {"clientcode": clientcode, "password": broker_pin, "totp": totp_code}
        )
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "CLIENT_LOCAL_IP",  # Ensure these are handled or replaced appropriately
            "X-ClientPublicIP": "CLIENT_PUBLIC_IP",
            "X-MACAddress": "MAC_ADDRESS",
            "X-PrivateKey": api_key,
        }

        print(f"DEBUG: Payload: {payload}")
        print(
            "DEBUG: URL: https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword"
        )

        response = client.post(
            "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword",
            headers=headers,
            content=payload,
        )
        print(f"DEBUG: Response Status: {response.status_code}")
        print(f"DEBUG: Response Text: {response.text}")

        # Add status attribute for compatibility with the existing codebase
        response.status = response.status_code

        data = response.text
        data_dict = json.loads(data)

        if (
            "data" in data_dict
            and data_dict["data"]
            and "jwtToken" in data_dict["data"]
        ):
            # Return both JWT token and feed token if available (None if not)
            auth_token = data_dict["data"]["jwtToken"]
            feed_token = data_dict["data"].get("feedToken", None)
            return auth_token, feed_token, None
        else:
            return (
                None,
                None,
                data_dict.get("message", "Authentication failed. Please try again."),
            )
    except Exception as e:
        return None, None, str(e)