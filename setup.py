from setuptools import setup, find_packages

setup(
    name="databricks-acl-app",
    version="0.0.1",
    author="Nivethan Venkatachalam",
    author_email="nivethanvenkat28@gmail.com",
    description="A Databricks ACL provisioning and schema tagging tool based on YAML configurations.",
    url="https://github.com/Nivethan2893/databricks-uc.git",
    packages=find_packages(),
    install_requires=[
        "PyYAML",
        "pyspark"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
