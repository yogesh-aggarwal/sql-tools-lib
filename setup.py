from setuptools import setup

with open("README.md", "r") as readMe:
    longDesc = readMe.read()

setup(
  name = 'sql_tools',
  packages = ['sql_tools'],
  version = '0.1.7',
  license='GNU',
  description = 'An integrative library that contains tools for performing various tasks related to the relations (table records).',
  long_description = longDesc,
  author = 'Yogesh Aggarwal',
  author_email = 'developeryogeshgit@gmail.com',
  url = 'https://github.com/yogesh-developer/sql-tools-lib',
  download_url = 'https://github.com/yogesh-developer/sql-tools-lib/dist/sql_operation-0.1.6.tar.gz',
  keywords = ['SQL', 'DATABASES', 'RECORDS'],
  install_requires=[
          'mysql-connector',
          'db-sqlite3',
          "numpy",
          "pandas",
          "pathlib",
      ],
  classifiers=[
    'Development Status :: 5 - Production/Stable',      # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
  ],
)
