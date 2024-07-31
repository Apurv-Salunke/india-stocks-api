from setuptools import setup, find_packages

setup(
    name="india_stock_api",
    version="0.1.0",
    description="A unified API for Indian stock market brokers",
    author="Apurv Salunke",
    author_email="salunke.apurv7@gmail.com",
    packages=find_packages(where="core"),
    install_requires=[
        "requests",
        "pandas",
        "numpy",
        # Add other dependencies here
    ],
    extras_require={
        "dev": ["pytest", "flake8"],
        "docs": ["sphinx"],
    },
    python_requires=">=3.6",
)
