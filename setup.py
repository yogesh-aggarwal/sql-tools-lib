from setuptools import setup, find_packages

with open("README.md", "r") as readMe:
    longDesc = readMe.read()

setup(
    name="sql_tools",
    packages=find_packages(),
    version="3.1.0",
    license="AGPL 3.0",
    description="An integrative library that contains tools for performing various tasks related to MySQL & sqlite3 databases.",
    long_description=longDesc,
    long_description_content_type="text/markdown",
    include_package_data=True,
    author="Yogesh Aggarwal",
    author_email="developeryogeshgit@gmail.com",
    url="https://github.com/yogesh-aggarwal/sql-tools-lib",
    download_url="https://raw.githubusercontent.com/yogesh-aggarwal/sql-tools-lib/master/dist/sql_tools-3.1.0.tar.gz",
    keywords=["SQL", "DATABASES", "TABLES", "RECORDS"],
    install_requires=["numpy", "pandas", "cryptography", "colorama"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",  # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
