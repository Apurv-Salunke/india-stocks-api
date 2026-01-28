import json
from india_stocks_api.internal.context import get_api_key, get_httpx_client, get_logger

logger = get_logger(__name__)

def get_api_response(endpoint, auth, method="GET", payload=''):
    """
    Perform an HTTP request to the Angel Broking API endpoint and return the parsed JSON response.
    
    Parameters:
        endpoint (str): API path starting with a slash (e.g., "/rest/...") to append to the Angel base URL.
        auth (str): Bearer authentication token to include in the Authorization header.
        method (str): HTTP method to use (default: "GET").
        payload (str | dict, optional): Request body; if a string, an attempt is made to parse it as JSON. For GET requests the payload is ignored.
    
    Returns:
        dict: The response parsed from JSON. Returns an empty dict if the response has no text or if JSON parsing fails.
    """
    AUTH_TOKEN = auth
    api_key = get_api_key("angel")
    client = get_httpx_client()
    
    headers = {
      'Authorization': f'Bearer {AUTH_TOKEN}',
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-UserType': 'USER',
      'X-SourceID': 'WEB',
      'X-ClientLocalIP': 'CLIENT_LOCAL_IP', 
      'X-ClientPublicIP': 'CLIENT_PUBLIC_IP',
      'X-MACAddress': 'MAC_ADDRESS',
      'X-PrivateKey': api_key
    }
    
    url = f"https://apiconnect.angelbroking.com{endpoint}"
    
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except:
            pass

    if method == "GET":
        response = client.get(url, headers=headers)
    elif method == "POST":
        response = client.post(url, headers=headers, json=payload)
    else:
        response = client.request(method, url, headers=headers, json=payload)
    
    response.status = response.status_code
    if not response.text: 
        return {}
    try:
        return json.loads(response.text)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON response from {endpoint}: {response.text}")
        return {}

def create_gtt_rule(payload, auth):
    return get_api_response("/rest/secure/angelbroking/gtt/v1/createRule", auth, "POST", payload)

def modify_gtt_rule(payload, auth):
    return get_api_response("/rest/secure/angelbroking/gtt/v1/modifyRule", auth, "POST", payload)

def cancel_gtt_rule(payload, auth):
    return get_api_response("/rest/secure/angelbroking/gtt/v1/cancelRule", auth, "POST", payload)

def get_gtt_list(payload, auth):
    # Payload usually contains status list like ["FOR_SETTLEMENT", "CANCELLED"]
    return get_api_response("/rest/secure/angelbroking/gtt/v1/listRule", auth, "POST", payload)

def get_gtt_status(id, auth):
    return get_api_response(f"/rest/secure/angelbroking/gtt/v1/ruleDetails/{id}", auth, "GET")