Using a Metaclass to Control Inheritance
A more advanced approach involves using a metaclass to control which methods or attributes are inherited by the child class.

Example:

```python
class BlockInheritedMethodMeta(type):
    def __new__(cls, name, bases, dct):
        if 'inherited_method' in dct:
            del dct['inherited_method']  # Remove the inherited method
        return super().__new__(cls, name, bases, dct)

class Parent:
    def inherited_method(self):
        print("This is the parent method")

class Child(Parent, metaclass=BlockInheritedMethodMeta):
    pass

child = Child()
# This will raise an AttributeError because the inherited method is blocked
child.inherited_method()  # Raises AttributeError
```
In this example, the `BlockInheritedMethodMeta` metaclass is used to block the `inherited_method` from being inherited by the `Child` class.

With a metaclass, you can dynamically alter the class definition at the time of class creation, including preventing certain methods from being inherited.
