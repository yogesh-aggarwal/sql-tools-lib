from setuptools import setup

with open("README.md", "r") as readMe:
    longDesc = readMe.read()

setup(
  name = 'sql_tools',         # How you named your package folder (MyLib)
  packages = ['sql_tools'],   # Chose the same as "name"
  version = '0.1.5',      # Start with a small number and increase it with every change you make
  license='GNU',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'An integrative library that contains tools for performing various tasks related to the relations (table records).',   # Give a short description about your library
  long_description = longDesc,
  author = 'Yogesh Aggarwal',                   # Type in your name
  author_email = 'developeryogeshgit@gmail.com',      # Type in your E-Mail
  url = 'https://github.com/yogesh-developer/sql-tools-lib',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/yogesh-developer/sql-tools-lib/dist/sql_operation-0.1.3.tar.gz',    # I explain this later on
  keywords = ['SQL', 'DATABASES', 'RECORDS'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'mysql-connector',
          'db-sqlite3',
          "numpy",
          "pathlib",
      ],
  classifiers=[
    'Development Status :: 5 - Production/Stable',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)
