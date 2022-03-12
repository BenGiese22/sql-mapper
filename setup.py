from setuptools import setup, find_packages


setup(
    name='sql-mapper',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click',
        'treelib',
    ],
    entry_points={
        'console_scripts': [
            'sql-mapper=cli:cli'
        ],
    },
)