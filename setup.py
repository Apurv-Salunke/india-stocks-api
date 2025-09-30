from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()
setup(
    name="india_stocks_api",
    version="1.0.0",
    description="A unified API for Indian stock market brokers",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="Apurv Salunke",
    author_email="salunke.apurv7@gmail.com",
    python_requires=">=3.8",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
        "numpy",
        "pyotp",
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
