# Design Philosophy

### Why make custom methods like `_json_parser()` instead of `json.loads()`?
In any Broker class (like angel one), they would have their own `_json_parser()` method to parse the JSON data received from the server. There a custom method allows us to have a more flexible way to parse the data and also allow us to have more control over the data received. We can raise exceptions if the data received is not as expected.
There might be some operations (some trading things) that comes out to be common across multiple Broker classes, so we can have a common method for that, so that Child Broker classes can inherit that method and use it.
