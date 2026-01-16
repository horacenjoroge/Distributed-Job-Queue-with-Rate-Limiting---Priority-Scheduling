"""
Setup script for the job queue package.
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="jobqueue",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A distributed job queue with rate limiting and priority scheduling",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/jobqueue",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.4",
            "pytest-asyncio>=0.23.3",
            "pytest-cov>=4.1.0",
            "black>=24.1.1",
            "flake8>=7.0.0",
            "isort>=5.13.2",
            "mypy>=1.8.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "jobqueue-api=jobqueue.api.main:main",
            "jobqueue-worker=jobqueue.worker.main:main",
        ],
    },
)
