# Docs for Building the PyPI Package

## Pre-requisite
Run the following command to install or upgrade the necessary tools:

```bash
python3 -m pip install --upgrade build twine
```

## Step 1
Build the package using:

```bash
python3 -m build
```

## Step 2
Upload the package to PyPI with:

```bash
twine upload dist/*
```

### Explanation of Updates:
- Added proper Markdown headings (##) for sections.
- Formatted the commands as code blocks using triple backticks and specified the language as `bash` for better readability.
