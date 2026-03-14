from setuptools import setup, find_packages

setup(
    name="mcp-google-contacts-server",
    version="0.1.0",  # This will be replaced by the workflow based on the tag
    description="MCP server for Google Contacts integration",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="Rayan Zaki",
    author_email="rayan.hassici@ensia.edu.dz",
    url="https://github.com/rayanzaki/mcp-google-contacts-server",
    packages=find_packages(),
    install_requires=[
        "fastmcp",
        "google-api-python-client",
        "google-auth",
        "google-auth-oauthlib",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "mcp-google-contacts=main:main",
        ],
    },
)
