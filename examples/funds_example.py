from india_stocks_api.funds import get_margin

# Example for AngelOne
margin = get_margin("angelone")
print(f"Available Cash: {margin.get('availablecash')}")