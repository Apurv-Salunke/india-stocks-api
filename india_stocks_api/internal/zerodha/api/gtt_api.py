from india_stocks_api.internal.context import get_httpx_client, get_logger

logger = get_logger(__name__)

def get_api_response(endpoint, auth, method="GET", payload=None):
    """
    Make an API request to Zerodha's GTT API using shared httpx client.
    
    Args:
        endpoint (str): API endpoint (e.g., '/gtt/triggers')
        auth (str): Authentication token in format 'api_key:access_token'
        method (str): HTTP method (GET, POST, PUT, DELETE)
        payload (dict, optional): Request payload for POST/PUT requests
        
    Returns:
        dict: API response data
    """
    AUTH_TOKEN = auth
    base_url = 'https://api.kite.trade'
    
    # Get the shared httpx client with connection pooling
    client = get_httpx_client()
    
    headers = {
        'X-Kite-Version': '3',
        'Authorization': f'token {AUTH_TOKEN}'
    }
    
    url = f"{base_url}{endpoint}"
    
    try:
        # Handle different HTTP methods
        if method.upper() == 'GET':
            response = client.get(
                url,
                headers=headers
            )
        elif method.upper() == 'POST':
            response = client.post(
                url,
                headers=headers,
                data=payload
            )
        elif method.upper() == 'PUT':
            response = client.put(
                url,
                headers=headers,
                data=payload
            )
        elif method.upper() == 'DELETE':
            response = client.delete(
                url,
                headers=headers
            )
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
            
        # Parse and return JSON response
        response.raise_for_status()
        body= response.json()

        if body.get("status") != "success":
            raise RuntimeError(body.get("message", "Unknown API error"))

        return body.get("data")
        
    except Exception as e:
        error_msg = str(e)
        # Try to extract more error details if available
        try:
            if hasattr(e, 'response') and e.response is not None:
                error_detail = e.response.json()
                error_msg = error_detail.get('message', error_msg)
        except Exception:
            pass
            
        logger.exception(f"GTT API request failed: {error_msg}")
        raise

def create_gtt_rule(payload, auth):
    """
    Create a new GTT rule.
    
    Args:
        payload (dict): GTT rule data with type, condition, and orders
        auth (str): Authentication token
    
    Returns:
        dict: API response with trigger_id
    """
    return get_api_response("/gtt/triggers", auth, "POST", payload)

def modify_gtt_rule(trigger_id, payload, auth):
    """
    Modify an existing GTT rule.
    
    Args:
        trigger_id (int): ID of the GTT rule to modify
        payload (dict): Updated GTT rule data
        auth (str): Authentication token
    
    Returns:
        dict: API response with trigger_id
    """
    return get_api_response(f"/gtt/triggers/{trigger_id}", auth, "PUT", payload)

def cancel_gtt_rule(trigger_id, auth):
    """
    Cancel/delete an active GTT rule.
    
    Args:
        trigger_id (int): ID of the GTT rule to cancel
        auth (str): Authentication token
    
    Returns:
        dict: API response with trigger_id
    """
    return get_api_response(f"/gtt/triggers/{trigger_id}", auth, "DELETE")

def get_gtt_list(auth):
    """
    Retrieve list of all GTTs visible in GTT order book.
    Includes active GTTs and GTTs in other states (previous 7 days).
    
    Args:
        auth (str): Authentication token
    
    Returns:
        dict: API response with list of GTTs
    """
    return get_api_response("/gtt/triggers", auth, "GET")

def get_gtt_details(trigger_id, auth):
    """
    Retrieve details of a specific GTT by ID.
    Returns details irrespective of age or status of the GTT.
    
    Args:
        trigger_id (int): ID of the GTT rule
        auth (str): Authentication token
    
    Returns:
        dict: API response with dict of GTT details
    """
    return get_api_response(f"/gtt/triggers/{trigger_id}", auth, "GET")
