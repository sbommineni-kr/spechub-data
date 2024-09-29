from setuptools import setup, find_packages

setup(
    name="spechub-data",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # Add your dependencies here
    ],
    author="Sudheer Bommienni",
    author_email="sudheer.bommineni@kroger.com",
    description="Data services for SpecHub application",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/sbommineni-kr/spechub-data",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
