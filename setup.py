import pathlib

from setuptools import setup, find_packages


# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


setup(
    description='AWS API tools for SparkFlow',
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/adaros92/sparkflow-awstools",
    version='1.4.2',
    install_requires=['boto3', 'tenacity'],
    tests_require=['pytest', 'pytest-cov', 'tox', 'Random-Word', 'tenacity'],
    license="MIT",
    classifiers=[
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.7",
        ],
    packages=find_packages(exclude=("test",)),
    name='sparkflowtools',
    python_requires='>=3.6',
    package_data={
            'sparkflowtools': ['configs/*']
        },
    entry_points={
        'console_scripts': [
                'sparkflow-tools = sparkflowtools.__main__:main'
            ]
    }
)
