from setuptools import find_packages, setup

setup(
    name="indostastic",
    packages=find_packages(exclude=["indostastic_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
