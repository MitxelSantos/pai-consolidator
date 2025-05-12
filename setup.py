from setuptools import setup, find_packages

setup(
    name="pai_consolidator",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "openpyxl",
        "numpy",
    ],
    entry_points={
        "console_scripts": [
            "pai-consolidator=pai_consolidator.cli:main",
        ],
    },
    author="Miguel Santos",
    author_email="mitxelsk811@gmail.com",
    description="Herramienta para consolidar datos PAI de vacunación",
    keywords="salud, vacunación, PAI, datos",
    python_requires=">=3.6",
)