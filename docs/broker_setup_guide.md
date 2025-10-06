# 🔧 Broker Setup Guide for India Stocks API

## 📋 **How to Set Up Broker Credentials**

### **Method 1: Environment Variables (Recommended)**

#### **Option A: `.env` File**
1. Copy `india_stocks_api/env.template` to `.env` in your project root
2. Fill in your broker credentials:

```bash
# .env file
# AngelOne Credentials
ANGELONE_CLIENT_CODE=A1357374
ANGELONE_PIN=8989
ANGELONE_TOTP_SECRET=EL7KI5RGSJ5ZXZ625FDETRO44I
BROKER_API_KEY=SP7RmmCu

# Shoonya Credentials
SHOONYA_USER_ID=your_user_id
SHOONYA_PASSWORD=your_password
SHOONYA_TOTP_SECRET=your_totp_secret
BROKER_API_KEY=your_shoonya_api_key
BROKER_API_SECRET=your_shoonya_api_secret

# Dhan Credentials
DHAN_CLIENT_ID=your_client_id
DHAN_PASSWORD=your_password
BROKER_API_KEY=your_dhan_api_key
```

3. Load environment variables in your code:
```python
from dotenv import load_dotenv
load_dotenv()  # Loads .env file
```

#### **Option B: System Environment Variables**
```bash
# Linux/Mac
export ANGELONE_CLIENT_CODE=A1357374
export ANGELONE_PIN=8989
export ANGELONE_TOTP_SECRET=EL7KI5RGSJ5ZXZ625FDETRO44I
export BROKER_API_KEY=SP7RmmCu

# Windows
set ANGELONE_CLIENT_CODE=A1357374
set ANGELONE_PIN=8989
set ANGELONE_TOTP_SECRET=EL7KI5RGSJ5ZXZ625FDETRO44I
set BROKER_API_KEY=SP7RmmCu
```

#### **Option C: Python os.environ**
```python
import os
os.environ['ANGELONE_CLIENT_CODE'] = 'A1357374'
os.environ['ANGELONE_PIN'] = '8989'
os.environ['ANGELONE_TOTP_SECRET'] = 'EL7KI5RGSJ5ZXZ625FDETRO44I'
os.environ['BROKER_API_KEY'] = 'SP7RmmCu'
```

### **Method 2: Direct Credentials (Not Recommended)**

```python
from india_stocks_api.auth import authenticate_broker
import os

# Set credentials directly
os.environ['ANGELONE_CLIENT_CODE'] = 'A1357374'
os.environ['ANGELONE_PIN'] = '8989'
os.environ['ANGELONE_TOTP_SECRET'] = 'EL7KI5RGSJ5ZXZ625FDETRO44I'
os.environ['BROKER_API_KEY'] = 'SP7RmmCu'

# Authenticate
result = authenticate_broker('angelone')
```

## 🚀 **Complete Usage Example**

### **Step 1: Set Up Credentials**
```python
# setup_credentials.py
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Or set directly
os.environ['ANGELONE_CLIENT_CODE'] = 'A1357374'
os.environ['ANGELONE_PIN'] = '8989'
os.environ['ANGELONE_TOTP_SECRET'] = 'EL7KI5RGSJ5ZXZ625FDETRO44I'
os.environ['BROKER_API_KEY'] = 'SP7RmmCu'
```

### **Step 2: Connect to Broker**
```python
# connect_broker.py
from india_stocks_api.auth import (
    authenticate_broker,
    is_authenticated,
    get_auth_headers,
    get_supported_brokers,
    get_missing_credentials
)

def connect_to_broker(broker_name):
    """Connect to a broker with proper error handling"""

    # Check if broker is supported
    if broker_name not in get_supported_brokers():
        print(f"❌ Unsupported broker: {broker_name}")
        return False

    # Check missing credentials
    missing = get_missing_credentials(broker_name)
    if missing:
        print(f"❌ Missing credentials for {broker_name}: {missing}")
        print("💡 Please set these environment variables")
        return False

    # Authenticate
    print(f"🔐 Authenticating with {broker_name}...")
    result = authenticate_broker(broker_name)

    if result['success']:
        print(f"✅ Successfully connected to {broker_name}!")
        print(f"   Auth Token: {result['auth_token'][:20]}...")
        print(f"   Broker User ID: {result['broker_user_id']}")
        return True
    else:
        print(f"❌ Authentication failed: {result['error']}")
        return False

# Usage
if __name__ == "__main__":
    # Connect to AngelOne
    connect_to_broker('angelone')

    # Check if authenticated
    if is_authenticated('angelone'):
        headers = get_auth_headers('angelone')
        print(f"📋 Auth headers: {headers}")
```

## 📋 **Supported Brokers and Required Credentials**

| Broker | Required Environment Variables |
|--------|-------------------------------|
| **AngelOne** | `ANGELONE_CLIENT_CODE`, `ANGELONE_PIN`, `ANGELONE_TOTP_SECRET`, `BROKER_API_KEY` |
| **Shoonya** | `SHOONYA_USER_ID`, `SHOONYA_PASSWORD`, `SHOONYA_TOTP_SECRET`, `BROKER_API_KEY`, `BROKER_API_SECRET` |
| **Dhan** | `DHAN_CLIENT_ID`, `DHAN_PASSWORD`, `BROKER_API_KEY` |
| **Dhan Sandbox** | `DHAN_SANDBOX_CLIENT_ID`, `DHAN_SANDBOX_PASSWORD`, `BROKER_API_KEY` |
| **Fyers** | `FYERS_CLIENT_ID`, `FYERS_PASSWORD`, `FYERS_PIN`, `BROKER_API_KEY`, `BROKER_API_SECRET` |
| **Groww** | `GROWW_USER_ID`, `GROWW_PASSWORD`, `GROWW_TOTP_SECRET`, `BROKER_API_KEY` |
| **5Paisa** | `FIVEPAISA_CLIENT_CODE`, `FIVEPAISA_PASSWORD`, `FIVEPAISA_TOTP_SECRET`, `BROKER_API_KEY` |
| **5PaisaXTS** | `FIVEPAISAXTS_CLIENT_CODE`, `FIVEPAISAXTS_PASSWORD`, `FIVEPAISAXTS_TOTP_SECRET`, `BROKER_API_KEY` |
| **Upstox** | `UPSTOX_CLIENT_ID`, `UPSTOX_CLIENT_SECRET`, `UPSTOX_REDIRECT_URI`, `BROKER_API_KEY` |

## 🔍 **Credential Validation**

```python
from india_stocks_api.auth import get_missing_credentials, get_supported_brokers

# Check supported brokers
print(f"Supported brokers: {get_supported_brokers()}")

# Check missing credentials for AngelOne
missing = get_missing_credentials('angelone')
if missing:
    print(f"Missing: {missing}")
else:
    print("✅ All credentials present")
```

## 🛡️ **Security Best Practices**

1. **Never commit `.env` files** to version control
2. **Use environment variables** instead of hardcoding credentials
3. **Rotate credentials** regularly
4. **Use different credentials** for different environments (dev/prod)
5. **Store sensitive data** in secure credential managers

## 📝 **Example Project Structure**

```
my_trading_app/
├── .env                    # Your credentials (not in git)
├── .env.example           # Template (safe to commit)
├── main.py               # Your trading code
└── requirements.txt     # Dependencies
```

## 🚀 **Quick Start**

1. **Install dependencies:**
   ```bash
   pip install python-dotenv india-stocks-api
   ```

2. **Create `.env` file:**
   ```bash
   cp india_stocks_api/env.template .env
   # Edit .env with your credentials
   ```

3. **Connect to broker:**
   ```python
   from dotenv import load_dotenv
   from india_stocks_api.auth import authenticate_broker

   load_dotenv()
   result = authenticate_broker('angelone')
   print(result)
   ```

That's it! Your broker is now connected and ready to use! 🎉
