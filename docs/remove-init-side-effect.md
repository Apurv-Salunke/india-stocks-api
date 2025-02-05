When using a `classmethod`, the `__init__` method of the class doesn't run because the class itself is being operated on, rather than an instance of it. If you want some code to run automatically when a `classmethod` is called, you can achieve this by leveraging either:

1. **A custom decorator** to wrap the `classmethod` and perform setup logic before the method runs.
2. **A metaclass** to enforce initialization logic.
3. **Static attributes or separate initialization methods** explicitly called within the `classmethod`.

Here are some approaches to achieve this:

---

### **1. Using a Decorator to Ensure Initialization**
You can define a custom decorator to check or perform the required setup logic before executing the class method.

**Example:**
```python
def ensure_initialized(cls):
    if not hasattr(cls, "_initialized"):
        cls._initialized = False

    def decorator(method):
        def wrapper(cls, *args, **kwargs):
            if not cls._initialized:
                cls._initialize()
                cls._initialized = True
            return method(cls, *args, **kwargs)
        return wrapper

    return decorator

class MyClass:
    _initialized = False

    @classmethod
    def _initialize(cls):
        print("Initializing class-level resources...")
        cls.shared_resource = "Initialized"

    @classmethod
    @ensure_initialized
    def do_something(cls):
        print(f"Using {cls.shared_resource}")

# Example usage
MyClass.do_something()  # Automatically runs initialization logic before executing the classmethod
MyClass.do_something()  # Skips initialization this time
```

---

### **2. Using a Metaclass to Automate Initialization**
A metaclass can enforce that certain setup logic runs when the class is first accessed.

**Example:**
```python
class AutoInitializeMeta(type):
    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_initialized"):
            cls._initialize()
            cls._initialized = True
        return super().__call__(*args, **kwargs)

class MyClass(metaclass=AutoInitializeMeta):
    _initialized = False

    @classmethod
    def _initialize(cls):
        print("Running class initialization logic...")
        cls.shared_resource = "Initialized"

    @classmethod
    def do_something(cls):
        print(f"Using {cls.shared_resource}")

# Example usage
MyClass.do_something()  # Automatically triggers initialization when the class is first accessed
MyClass.do_something()  # Skips initialization
```

---

### **3. Explicit Initialization in Class Methods**
This approach keeps it simple by explicitly checking or calling an initialization method within the `classmethod`.

**Example:**
```python
class MyClass:
    _initialized = False

    @classmethod
    def _initialize(cls):
        print("Initializing...")
        cls.shared_resource = "Some shared resource"
        cls._initialized = True

    @classmethod
    def do_something(cls):
        if not cls._initialized:
            cls._initialize()
        print(f"Using {cls.shared_resource}")

# Example usage
MyClass.do_something()  # Triggers initialization first
MyClass.do_something()  # Uses initialized resources
```

---

### **4. Using the Singleton Pattern for Automatic Initialization**
The Singleton pattern ensures a single point of initialization for a class. You can implement it for class-level resources.

**Example:**
```python
class MyClass:
    _initialized = False

    @classmethod
    def _initialize(cls):
        print("Initializing resources...")
        cls.shared_resource = "Shared Resource"
        cls._initialized = True

    @classmethod
    def get_instance(cls):
        if not cls._initialized:
            cls._initialize()
        return cls

    @classmethod
    def do_something(cls):
        print(f"Using {cls.shared_resource}")

# Example usage
MyClass.get_instance().do_something()  # Automatically ensures the class is initialized
MyClass.do_something()  # Initialization already done
```

---

### **5. Using a `classmethod` to Explicitly Call Initialization**
You can define a separate `initialize` class method and call it before other `classmethod` methods.

**Example:**
```python
class MyClass:
    _initialized = False

    @classmethod
    def initialize(cls):
        if not cls._initialized:
            print("Initializing resources...")
            cls.shared_resource = "Shared Data"
            cls._initialized = True

    @classmethod
    def do_something(cls):
        cls.initialize()  # Ensure initialization
        print(f"Using {cls.shared_resource}")

# Example usage
MyClass.do_something()  # Automatically initializes and performs the operation
MyClass.do_something()  # Uses the already-initialized resources
```

---

### **Comparison of Approaches**
| **Approach**             | **When to Use**                                                                                         |
|---------------------------|-------------------------------------------------------------------------------------------------------|
| **Decorator**             | When you want to abstract initialization logic and apply it to multiple classmethods easily.          |
| **Metaclass**             | When initialization logic needs to be enforced globally or for many classes.                         |
| **Explicit Check**        | When you want fine-grained control over initialization and prefer simpler, more explicit logic.       |
| **Singleton Pattern**     | When the class has shared state or resources and should be initialized only once for all consumers.   |

Each approach ensures that initialization logic is triggered without explicitly instantiating the class. Choose the one that best fits your design and complexity requirements.
