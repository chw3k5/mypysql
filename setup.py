from setuptools import setup, find_packages


setup(name='mypysql',
      version='0.1.0',
      description='Load data to a MySQL database',
      author='Caleb Wheeler',
      author_email='chw3k5@gmail.com',
      packages=find_packages(),
      url="https://github.com/chw3k5/mypysql",
      python_requires='>3.7',
      install_requires=['mysql-connector-python',
                        'pymysql',
                        'pandas',
                        'sqlalchemy',
                        'Unidecode'])
