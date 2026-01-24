from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()
setup(
    name="india_stocks_api",
    version="2.0.0",
    description="A unified API for Indian stock market brokers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="Apurv Salunke",
    author_email="salunke.apurv7@gmail.com",
    python_requires=">=3.8",
    packages=find_packages(),
    install_requires=[
        "httpx>=0.28.1",
        "pandas>=2.3.3",
        "pyotp>=2.9.0",
        "sqlalchemy>=2.0.0",
        "python-dateutil>=2.9.0",
        "pyzmq>=27.1.0",
        "logzero>=1.7.0",
        "websocket-client>=1.8.0",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: Information Technology",
        "Topic :: Software Development :: Build Tools",
        "Topic :: Office/Business :: Financial :: Investment",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Environment :: Console",
    ],
    extras_require={
        "dev": ["pytest", "flake8"],
        "docs": ["sphinx"],
    },
    project_urls={
        "Documentation": "https://github.com/Apurv-Salunke/india-stocks-api/wiki",
        "Source Code": "https://github.com/Apurv-Salunke/india-stocks-api",
    },
)
