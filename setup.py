from pathlib import Path

from setuptools import find_packages, setup


def find_version() -> str:
    version = "1.0.0"
    extension_yaml_path = Path(__file__).parent / "extension" / "extension.yaml"
    try:
        with open(extension_yaml_path, encoding="utf-8") as f:
            for line in f:
                if line.startswith("version:"):
                    version = line.split(":", 1)[1].strip().strip('"').strip("'")
                    break
    except Exception:
        pass
    return version


setup(
    name="airflow_extension",
    version=find_version(),
    description="Enterprise Apache Airflow 2.10+/3.x Dynatrace Extension 2.0",
    author="Enterprise Observability Engineering",
    packages=find_packages(include=["airflow_extension", "airflow_extension.*"]),
    python_requires=">=3.10",
    include_package_data=True,
    install_requires=[
        "dt-extensions-sdk>=1.11.0",
        "requests>=2.31.0",
    ],
    extras_require={"dev": ["dt-extensions-sdk[cli]>=1.11.0"]},
)
